/**
 * dedupEmails.js — Remove duplicate emails in TARGET mailbox
 *
 * Scans target folders for messages with the same SourceMessageId (primary)
 * or same subject+receivedDateTime (fallback). When duplicates are found,
 * keeps the one with the most attachments (or the first one) and deletes the rest.
 *
 * Same patterns as fixAttachments.js: batch ops, quota retry, 60s between users.
 */

'use strict';

const fs          = require('fs');
const path        = require('path');
const axios       = require('axios');
const chalk       = require('chalk');
const Logger      = require('./logger');
const TenantAuth  = require('./auth');
const GraphClient = require('./graphClient');
const UserLoader  = require('./userLoader');

// ── Constants ─────────────────────────────────────────────────────────────────
const GRAPH_BASE           = 'https://graph.microsoft.com/v1.0';
const GRAPH_BATCH_URL      = `${GRAPH_BASE}/$batch`;
const MIGRATION_PROPERTY_ID = 'String {8ECCC264-6880-4EBE-992F-8888D2EEAA1D} Name SourceMessageId';

const BATCH_SIZE          = 20;
const PAGE_SIZE           = 100;
const BATCH_DELAY_MS      = 1500;
const BATCH_PAUSE_EVERY   = 5;
const FOLDER_DELAY_MS     = 5000;
const USER_DELAY_MS       = 60000;

const WELL_KNOWN = {
  'Caixa de Entrada': 'inbox',       'Inbox': 'inbox',
  'Itens Enviados':   'sentitems',   'Sent Items': 'sentitems',
  'Itens Excluídos':  'deleteditems','Deleted Items': 'deleteditems',
  'Rascunhos':        'drafts',      'Drafts': 'drafts',
  'Lixo Eletrônico':  'junkemail',   'Junk Email': 'junkemail',
  'Arquivo Morto':    'archive',     'Archive': 'archive',
};

const DRY_RUN = process.argv.includes('--dry-run');
const ONLY_USER = (() => {
  const idx = process.argv.indexOf('--user');
  if (idx !== -1 && process.argv[idx + 1]) return process.argv[idx + 1];
  const eq = process.argv.find(a => a.startsWith('--user='));
  return eq ? eq.split('=')[1] : null;
})();

let config;
try {
  config = JSON.parse(fs.readFileSync(path.resolve(process.cwd(), 'config.json'), 'utf8'));
} catch (e) {
  console.error(chalk.red('❌ config.json not found'));
  process.exit(1);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function chunk(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) chunks.push(arr.slice(i, i + size));
  return chunks;
}

// ── JSON Batch ────────────────────────────────────────────────────────────────
async function sendBatch(authInstance, requests, attempt = 1) {
  const MAX_ATTEMPTS = 5;
  const token = await authInstance.getToken();
  const response = await axios.post(
    GRAPH_BATCH_URL,
    { requests },
    {
      headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
      validateStatus: null
    }
  );
  if (response.status === 429) {
    const wait = parseInt(response.headers['retry-after'] || '10') * 1000;
    await sleep(wait);
    return sendBatch(authInstance, requests, attempt + 1);
  }
  if ((response.status === 503 || response.status === 504) && attempt <= MAX_ATTEMPTS) {
    await sleep(2000 * Math.pow(2, attempt - 1));
    return sendBatch(authInstance, requests, attempt + 1);
  }
  const responses = response.data?.responses || [];
  if (responses.length === 0 && requests.length > 0 && attempt <= MAX_ATTEMPTS) {
    await sleep(3000 * attempt);
    return sendBatch(authInstance, requests, attempt + 1);
  }
  const results = {};
  for (const r of responses) results[r.id] = { status: r.status, body: r.body };
  return results;
}

// ── Fetch all messages from a folder ─────────────────────────────────────────
async function fetchAllMessages(client, userEmail, folderId) {
  const messages = [];
  let skip = 0;
  const expand = `singleValueExtendedProperties($filter=id eq '${MIGRATION_PROPERTY_ID}')`;
  while (true) {
    const result = await client.get(
      `/users/${userEmail}/mailFolders/${folderId}/messages`,
      {
        '$top':    PAGE_SIZE,
        '$skip':   skip,
        '$select': 'id,subject,receivedDateTime,hasAttachments,singleValueExtendedProperties',
        '$expand': expand
      }
    );
    const msgs = result.value || [];
    messages.push(...msgs);
    if (msgs.length < PAGE_SIZE) break;
    skip += PAGE_SIZE;
  }
  return messages;
}

// ── Find duplicates in a list of messages ─────────────────────────────────────
function findDuplicates(messages) {
  const bySourceId = new Map();   // sourceMessageId → [msg, msg, ...]
  const byFallback = new Map();   // subject|receivedDateTime → [msg, msg, ...]
  const hasSourceId = new Set();

  for (const msg of messages) {
    const prop = msg.singleValueExtendedProperties?.find(p => p.id === MIGRATION_PROPERTY_ID);
    if (prop?.value) {
      hasSourceId.add(msg.id);
      if (!bySourceId.has(prop.value)) bySourceId.set(prop.value, []);
      bySourceId.get(prop.value).push(msg);
    }
  }

  // Fallback: only for messages WITHOUT SourceMessageId
  for (const msg of messages) {
    if (hasSourceId.has(msg.id)) continue;
    if (!msg.subject || !msg.receivedDateTime) continue;
    const key = `${msg.subject}|${msg.receivedDateTime}`;
    if (!byFallback.has(key)) byFallback.set(key, []);
    byFallback.get(key).push(msg);
  }

  const toDelete = [];

  // Process SourceMessageId groups
  for (const [srcId, msgs] of bySourceId) {
    if (msgs.length <= 1) continue;
    // Keep the one with hasAttachments=true, or the first
    msgs.sort((a, b) => (b.hasAttachments ? 1 : 0) - (a.hasAttachments ? 1 : 0));
    for (let i = 1; i < msgs.length; i++) {
      toDelete.push({ msg: msgs[i], reason: `dup SourceMessageId`, kept: msgs[0].id });
    }
  }

  // Process fallback groups
  for (const [key, msgs] of byFallback) {
    if (msgs.length <= 1) continue;
    msgs.sort((a, b) => (b.hasAttachments ? 1 : 0) - (a.hasAttachments ? 1 : 0));
    for (let i = 1; i < msgs.length; i++) {
      toDelete.push({ msg: msgs[i], reason: `dup subject+date`, kept: msgs[0].id });
    }
  }

  return toDelete;
}

// ── Process a single folder ───────────────────────────────────────────────────
async function deduplicateFolder(tgtClient, tgtAuth, userEmail, folderId, folderName, logger) {
  const stats = { deleted: 0, failed: 0 };

  logger.info(`\n   📁 ${folderName}: scanning for duplicates...`);

  const messages = await fetchAllMessages(tgtClient, userEmail, folderId);
  if (messages.length === 0) {
    logger.info(`   ✅ Empty folder`);
    return stats;
  }

  const toDelete = findDuplicates(messages);

  if (toDelete.length === 0) {
    logger.info(`   ✅ No duplicates (${messages.length} msgs checked)`);
    return stats;
  }

  logger.info(`   Found ${toDelete.length} duplicates in ${messages.length} messages`);

  if (DRY_RUN) {
    for (const d of toDelete.slice(0, 5)) {
      logger.info(`   [DRY RUN] Would delete: "${d.msg.subject}" (${d.reason})`);
    }
    if (toDelete.length > 5) logger.info(`   [DRY RUN] ... and ${toDelete.length - 5} more`);
    stats.deleted = toDelete.length;
    return stats;
  }

  // Batch DELETE
  const batches = chunk(toDelete, BATCH_SIZE);
  for (let bi = 0; bi < batches.length; bi++) {
    const batch = batches[bi];
    const requests = batch.map((d, idx) => ({
      id: String(idx),
      method: 'DELETE',
      url: `/users/${userEmail}/messages/${d.msg.id}`
    }));

    const results = await sendBatch(tgtAuth, requests);

    for (let idx = 0; idx < batch.length; idx++) {
      const r = results[String(idx)];
      if (r && (r.status === 204 || r.status === 404)) {
        stats.deleted++;
      } else {
        const errMsg = r?.body?.error?.message || (r ? `HTTP ${r.status}` : 'no response');
        logger.warn(`   ⚠️  Could not delete "${batch[idx].msg.subject}": ${errMsg}`);
        stats.failed++;
      }
    }

    const progress = Math.min((bi + 1) * BATCH_SIZE, toDelete.length);
    logger.info(`   🗑️  [${progress}/${toDelete.length}] ${Math.round(progress / toDelete.length * 100)}%`);

    if (bi < batches.length - 1) {
      if ((bi + 1) % BATCH_PAUSE_EVERY === 0) await sleep(5000);
      else await sleep(BATCH_DELAY_MS);
    }
  }

  logger.info(`   ✓ ${folderName}: ${stats.deleted} deleted, ${stats.failed} failed`);
  return stats;
}

// ── Process a single user ─────────────────────────────────────────────────────
async function processUser(tgtClient, tgtAuth, user, logger) {
  const stats = { deleted: 0, failed: 0 };
  logger.info(`\n👤 ${user.sourceEmail} → ${user.targetEmail}`);
  logger.info('   📂 Loading target folders...');

  // Load target folders
  const tgtFolderMap = new Map();
  for await (const f of tgtClient.paginate(
    `/users/${user.targetEmail}/mailFolders`,
    { '$top': 100, '$expand': 'childFolders' }, 'folders'
  )) {
    tgtFolderMap.set(f.id, f);
    for (const c of (f.childFolders || [])) tgtFolderMap.set(c.id, c);
  }

  // Resolve well-known folders
  const resolved = {};
  for (const [name, wkId] of Object.entries(WELL_KNOWN)) {
    if (resolved[wkId]) continue;
    try {
      const f = await tgtClient.get(`/users/${user.targetEmail}/mailFolders/${wkId}`);
      resolved[wkId] = f.id;
      tgtFolderMap.set(f.id, f);
    } catch (e) { /* skip */ }
  }

  const folders = [...tgtFolderMap.values()];
  const checkedIds = new Set();
  logger.info(`   ✓ ${folders.length} folders`);

  for (const folder of folders) {
    if (checkedIds.has(folder.id)) continue;
    checkedIds.add(folder.id);

    const s = await deduplicateFolder(tgtClient, tgtAuth, user.targetEmail, folder.id, folder.displayName, logger);
    stats.deleted += s.deleted;
    stats.failed  += s.failed;

    if (s.deleted > 0) await sleep(FOLDER_DELAY_MS);
  }

  logger.info(`\nUser done: ${stats.deleted} fixed, ${stats.failed} failed`);
  return stats;
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  const mainLogger = new Logger(config.logs_dir || './logs', 'dedup-emails');

  mainLogger.info(chalk.cyan('\n🔄 Email Dedup') + (DRY_RUN ? chalk.yellow(' [DRY RUN]') : ''));
  mainLogger.info(`   Target: ${config.target_tenant.domain}`);

  const userLoader = new UserLoader(config.users_csv);
  let users = userLoader.load();
  if (ONLY_USER) {
    users = users.filter(u => u.sourceEmail.toLowerCase() === ONLY_USER.toLowerCase());
    if (!users.length) { mainLogger.error(`User not found: ${ONLY_USER}`); process.exit(1); }
  }
  mainLogger.info(`   Users: ${users.length}${ONLY_USER ? ` (filtered: ${ONLY_USER})` : ''}`);

  const tgtAuth = new TenantAuth(config.target_tenant, 'target');
  await tgtAuth.getToken();
  const tgtClient = new GraphClient(tgtAuth, config.migration, mainLogger);

  mainLogger.success('Target tenant authenticated ✓');

  const globalStats = { deleted: 0, failed: 0 };

  for (let i = 0; i < users.length; i++) {
    const user = users[i];
    const logger = new Logger(
      config.logs_dir || './logs',
      user.sourceEmail.replace('@', '_').replace(/\./g, '_')
    );

    try {
      const s = await processUser(tgtClient, tgtAuth, user, logger);
      globalStats.deleted += s.deleted;
      globalStats.failed  += s.failed;
    } catch (err) {
      logger.error(`Fatal error for ${user.sourceEmail}: ${err.message}`);
    }

    if (i < users.length - 1) {
      mainLogger.info(`\n   ⏳ Waiting 60s before next user (quota recovery)...`);
      await sleep(USER_DELAY_MS);
    }
  }

  mainLogger.info(chalk.green(`\n✅ All done: ${globalStats.deleted} deleted, ${globalStats.failed} failed`));
}

main().catch(err => {
  console.error(chalk.red('Fatal:'), err.message);
  process.exit(1);
});
