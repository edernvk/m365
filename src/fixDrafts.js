#!/usr/bin/env node
/**
 * fixDrafts.js — Corrige mensagens migradas incorretamente como [Rascunho]
 *
 * MELHORIAS v4 (baseadas na documentação oficial Graph API):
 *
 * 1. JSON BATCHING: agrupa até 20 DELETEs/POSTs em 1 chamada HTTP
 *    → Graph API envia 4 em paralelo internamente, respeitando os limites
 *    → Reduz ~95% do número de chamadas HTTP e praticamente elimina rate limiting
 *    → Fonte: https://learn.microsoft.com/en-us/graph/json-batching
 *
 * 2. DEDUP de pastas por ID: evita reprocessar a mesma pasta 2x
 *    quando $expand retorna filhos tanto dentro do pai quanto como top-level
 *
 * 3. Folder ID reutilizado: não chama resolveFolderFresh() por mensagem,
 *    usa o tgtFolderMap carregado no início (já tem IDs válidos)
 *
 * 4. MAPI flags corretos: Integer 0x0E07=5 (Read+Submit), String 0x001A=IPM.Note,
 *    Integer 0x0E17=1 (PR_MESSAGE_STATE) — remove draft flag na criação
 *
 * 5. Sem throttle artificial: o batching já controla a taxa naturalmente
 *
 * Usage:
 *   node src/fixDrafts.js                      # todos os usuários
 *   node src/fixDrafts.js --user email@domain  # só um usuário
 *   node src/fixDrafts.js --dry-run            # simulação
 *   node src/fixDrafts.js --batch-size 10      # tamanho do batch (default: 20)
 */

const fs       = require('fs');
const path     = require('path');
const minimist = require('minimist');
const chalk    = require('chalk');
const axios    = require('axios');
const TenantAuth  = require('./auth');
const GraphClient = require('./graphClient');
const UserLoader  = require('./userLoader');
const pLimit   = require('p-limit');
const Logger      = require('./logger');

const MIGRATION_PROPERTY_ID = 'String {8ECCC264-6880-4EBE-992F-8888D2EEAA1D} Name SourceMessageId';
const GRAPH_BATCH_URL = 'https://graph.microsoft.com/v1.0/$batch';

const argv       = minimist(process.argv.slice(2));
const ONLY_USER  = argv.user || argv.u || null;
const DRY_RUN    = argv['dry-run'] || argv.d || false;
const BATCH_SIZE = Math.min(parseInt(argv['batch-size'] || '20', 10), 20); // Graph max is 20
const PAGE_SIZE  = 100;
const PHASE_DELAY_MS = 2000; // wait after bulk delete before recreating

const WELL_KNOWN = {
  'Caixa de Entrada': 'inbox',       'Inbox': 'inbox',
  'Itens Enviados':   'sentitems',   'Sent Items': 'sentitems',
  'Itens Excluídos':  'deleteditems','Deleted Items': 'deleteditems',
  'Rascunhos':        'drafts',      'Drafts': 'drafts',
  'Lixo Eletrônico':  'junkemail',   'Junk Email': 'junkemail',
  'Arquivo Morto':    'archive',     'Archive': 'archive',
};

let config;
try {
  config = JSON.parse(fs.readFileSync(path.resolve(process.cwd(), 'config.json'), 'utf8'));
} catch (e) {
  console.error(chalk.red('❌ config.json not found'));
  process.exit(1);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function sanitizeHeaders(headers) {
  const seen = new Set();
  const out  = [];
  for (const h of (headers || [])) {
    const name = h?.name?.toLowerCase();
    if (name && name.startsWith('x-') && !seen.has(name)) {
      seen.add(name);
      out.push(h);
    }
  }
  return out.slice(0, 5);
}

function buildPayload(msg, sourceId, includeHeaders) {
  const originalDate = msg.receivedDateTime || msg.sentDateTime;
  return {
    subject:       msg.subject || '(sem assunto)',
    body:          msg.body || { contentType: 'text', content: '' },
    from:          msg.from,
    toRecipients:  msg.toRecipients  || [],
    ccRecipients:  msg.ccRecipients  || [],
    bccRecipients: msg.bccRecipients || [],
    replyTo:       msg.replyTo       || [],
    receivedDateTime: msg.receivedDateTime,
    sentDateTime:     msg.sentDateTime,
    isRead:    msg.isRead,
    flag:      msg.flag,
    importance: msg.importance || 'normal',
    ...(includeHeaders && msg.internetMessageHeaders?.length
      ? { internetMessageHeaders: sanitizeHeaders(msg.internetMessageHeaders) }
      : {}),
    singleValueExtendedProperties: [
      originalDate     && { id: 'SystemTime 0x0E06', value: originalDate },
      msg.sentDateTime && { id: 'SystemTime 0x0039', value: msg.sentDateTime },
      { id: 'String 0x001A',  value: 'IPM.Note' },  // message class
      { id: 'Integer 0x0E07', value: '5' },          // Read(1)+Submit(4) = non-draft
      { id: 'Integer 0x0E17', value: '1' },          // PR_MESSAGE_STATE: not draft
      sourceId && { id: MIGRATION_PROPERTY_ID, value: sourceId }
    ].filter(Boolean)
  };
}

// ── JSON Batch helper ─────────────────────────────────────────────────────────
// Sends up to 20 requests in a single HTTP call to /$batch
// Returns map of { id → { status, body } }
async function sendBatch(authInstance, requests) {
  const token = await authInstance.getToken();
  const response = await axios.post(
    GRAPH_BATCH_URL,
    { requests },
    {
      headers: {
        'Authorization': `Bearer ${token}`,
        'Content-Type': 'application/json'
      },
      validateStatus: null
    }
  );

  if (response.status === 429) {
    const retryAfter = parseInt(response.headers['retry-after'] || '10') * 1000;
    await sleep(retryAfter);
    return sendBatch(authInstance, requests); // retry
  }

  const results = {};
  for (const r of (response.data?.responses || [])) {
    results[r.id] = { status: r.status, body: r.body };
  }
  return results;
}

// ── Chunk array into groups of N ─────────────────────────────────────────────
function chunk(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) chunks.push(arr.slice(i, i + size));
  return chunks;
}

// ── Get all drafts in a folder ────────────────────────────────────────────────
async function getDrafts(client, userEmail, folderId) {
  const drafts = [];
  let skip = 0;
  while (true) {
    const result = await client.get(
      `/users/${userEmail}/mailFolders/${folderId}/messages`,
      {
        '$top': PAGE_SIZE, '$skip': skip,
        '$filter': 'isDraft eq true',
        '$select': 'id,subject,receivedDateTime,sentDateTime,body,from,toRecipients,ccRecipients,bccRecipients,replyTo,flag,importance,isRead',
        '$expand': `singleValueExtendedProperties($filter=id eq '${MIGRATION_PROPERTY_ID}')`
      }
    );
    const msgs = result.value || [];
    drafts.push(...msgs);
    if (msgs.length < PAGE_SIZE) break;
    skip += PAGE_SIZE;
  }
  return drafts;
}

// ── Fetch original from source by SourceMessageId or subject+date ─────────────
async function fetchOriginal(srcClient, srcEmail, srcFolderId, draft) {
  const sourceProp = draft.singleValueExtendedProperties?.find(p => p.id === MIGRATION_PROPERTY_ID);
  const sourceId   = sourceProp?.value || null;

  if (sourceId) {
    try {
      return await srcClient.get(
        `/users/${srcEmail}/messages/${sourceId}`,
        { '$select': 'id,subject,receivedDateTime,sentDateTime,body,from,toRecipients,ccRecipients,bccRecipients,replyTo,flag,importance,isRead,internetMessageHeaders' }
      );
    } catch (e) { /* fall through */ }
  }

  if (draft.subject && draft.receivedDateTime) {
    try {
      const safe   = draft.subject.replace(/'/g, "''");
      const result = await srcClient.get(
        `/users/${srcEmail}/mailFolders/${srcFolderId}/messages`,
        {
          '$top': 1,
          '$filter': `subject eq '${safe}' and receivedDateTime eq ${draft.receivedDateTime}`,
          '$select': 'id,subject,receivedDateTime,sentDateTime,body,from,toRecipients,ccRecipients,bccRecipients,replyTo,flag,importance,isRead,internetMessageHeaders'
        }
      );
      if (result.value?.[0]) return result.value[0];
    } catch (e) { /* fall through */ }
  }

  return null;
}

// ── Fix all drafts in a folder ────────────────────────────────────────────────
async function fixFolder(srcClient, tgtClient, srcAuth, tgtAuth, srcEmail, tgtEmail, srcFolder, tgtFolderId, logger, stats) {
  const drafts = await getDrafts(tgtClient, tgtEmail, tgtFolderId);
  if (drafts.length === 0) return;

  logger.info(`\n   📁 ${srcFolder.displayName}: ${chalk.yellow(drafts.length + ' drafts')}`);

  if (DRY_RUN) {
    logger.info(`   [DRY RUN] Would fix ${drafts.length} drafts`);
    stats.fixed += drafts.length;
    return;
  }

  // ── PHASE 1: Fetch originals from source (concurrency 3) ───────────────
  // 3 concurrent reads against source tenant — safe within 4 concurrent/app/mailbox limit
  logger.info(`   ⬇️  Phase 1/3: Fetching ${drafts.length} originals from source (concurrency 3)...`);
  const fetchLimit = pLimit(4); // 4 = documented max concurrent requests per app per mailbox (Graph API)
  let fetchCount = 0;
  const enriched = await Promise.all(
    drafts.map(draft => fetchLimit(async () => {
      const original = await fetchOriginal(srcClient, srcEmail, srcFolder.id, draft);
      fetchCount++;
      if (fetchCount % 500 === 0 || fetchCount === drafts.length) {
        logger.info(`   ⬇️  [${fetchCount}/${drafts.length}] ${Math.round(fetchCount/drafts.length*100)}%`);
      }
      return { draft, original };
    }))
  );
  const foundCount = enriched.filter(e => e.original).length;
  logger.info(`   ✓ Phase 1: ${foundCount} from source, ${enriched.length - foundCount} from draft copy`);

  // ── PHASE 2: Batch DELETE all drafts ─────────────────────────────────────
  // Each batch of 20 = 1 HTTP call instead of 20
  logger.info(`   🗑️  Phase 2/3: Batch deleting ${drafts.length} drafts (${BATCH_SIZE}/batch)...`);
  const deleted = new Set();
  const deleteChunks = chunk(enriched, BATCH_SIZE);

  for (let i = 0; i < deleteChunks.length; i++) {
    const batch = deleteChunks[i];
    // Use message ID as batch request ID — guarantees unique mapping across chunks
    const requests = batch.map(e => ({
      id: e.draft.id,
      method: 'DELETE',
      url: `/users/${tgtEmail}/messages/${e.draft.id}`
    }));

    const results = await sendBatch(tgtAuth, requests);

    for (const e of batch) {
      const r = results[e.draft.id];
      if (r && r.status === 204) {
        deleted.add(e.draft.id);
      }
      // 404 = already gone from previous run — skip silently (no warning, not an error)
    }

    const progress = Math.min((i + 1) * BATCH_SIZE, drafts.length);
    logger.info(`   🗑️  [${progress}/${drafts.length}] ${Math.round(progress/drafts.length*100)}% — batch ${i+1}/${deleteChunks.length}`);
  }
  logger.info(`   ✓ Phase 2: ${deleted.size}/${drafts.length} deleted`);

  // Wait for Exchange to settle after bulk delete
  logger.info(`   ⏳ Waiting ${PHASE_DELAY_MS/1000}s for Exchange to settle...`);
  await sleep(PHASE_DELAY_MS);

  // ── PHASE 3: Batch CREATE corrected messages ──────────────────────────────
  const toCreate = enriched.filter(e => deleted.has(e.draft.id));
  if (toCreate.length === 0) {
    logger.info(`   ✉️  Phase 3/3: Nothing to create (all deletes failed or already gone)`);
    return;
  }

  logger.info(`   ✉️  Phase 3/3: Batch creating ${toCreate.length} messages (${BATCH_SIZE}/batch)...`);
  let created = 0, failed = 0;
  const createChunks = chunk(toCreate, BATCH_SIZE);

  for (let i = 0; i < createChunks.length; i++) {
    const batch = createChunks[i];
    // Use source draft ID as batch request ID — unique per message
    const requests = batch.map(e => {
      const payload = e.original
        ? buildPayload(e.original, e.original.id, true)
        : buildPayload(e.draft, null, false);
      return {
        id: e.draft.id,
        method: 'POST',
        url: `/users/${tgtEmail}/mailFolders/${tgtFolderId}/messages`,
        headers: { 'Content-Type': 'application/json' },
        body: payload
      };
    });

    const results = await sendBatch(tgtAuth, requests);

    for (const e of batch) {
      const r = results[e.draft.id];
      if (r && r.status === 201) {
        created++;
      } else {
        const errMsg = r?.body?.error?.message || `status ${r?.status}`;
        logger.error(`   ✗ Create failed "${e.draft.subject}": ${errMsg}`);
        failed++;
      }
    }

    const progress = Math.min((i + 1) * BATCH_SIZE, toCreate.length);
    logger.info(`   ✉️  [${progress}/${toCreate.length}] ${Math.round(progress/toCreate.length*100)}% — batch ${i+1}/${createChunks.length}`);
  }

  logger.info(`   ✓ Phase 3: ${created} created, ${failed} failed`);
  stats.fixed  += created;
  stats.failed += failed;
}

// ── Process a single user ─────────────────────────────────────────────────────
async function processUser(srcClient, tgtClient, srcAuth, tgtAuth, user, logger) {
  const stats = { fixed: 0, failed: 0 };
  logger.info(`\n👤 ${user.sourceEmail} → ${user.targetEmail}`);

  logger.info('   📂 Loading folders...');

  // Dedup by ID — $expand can return children both inside parent AND as top-level
  const srcFolderMap = new Map();
  for await (const f of srcClient.paginate(`/users/${user.sourceEmail}/mailFolders`, { '$expand': 'childFolders', '$top': 100 })) {
    srcFolderMap.set(f.id, f);
    if (f.childFolders?.length) {
      for (const c of f.childFolders) srcFolderMap.set(c.id, c);
    }
  }
  const srcFolders = [...srcFolderMap.values()];

  // Target folder map: name → id (also deduped)
  const tgtFolderMap = {};
  for await (const f of tgtClient.paginate(`/users/${user.targetEmail}/mailFolders`, { '$expand': 'childFolders', '$top': 100 })) {
    tgtFolderMap[f.displayName] = f.id;
    if (f.childFolders?.length) {
      for (const c of f.childFolders) tgtFolderMap[c.displayName] = c.id;
    }
  }
  logger.info(`   ✓ ${srcFolders.length} source folders, ${Object.keys(tgtFolderMap).length} target folders`);

  let totalDrafts = 0;

  for (const srcFolder of srcFolders) {
    // Find matching target folder — use map first, then well-known fallback
    let tgtFolderId = tgtFolderMap[srcFolder.displayName];
    if (!tgtFolderId && WELL_KNOWN[srcFolder.displayName]) {
      try {
        const f = await tgtClient.get(`/users/${user.targetEmail}/mailFolders/${WELL_KNOWN[srcFolder.displayName]}`);
        tgtFolderId = f.id;
        tgtFolderMap[srcFolder.displayName] = tgtFolderId; // cache it
      } catch (e) { continue; }
    }
    if (!tgtFolderId) continue;

    await fixFolder(
      srcClient, tgtClient, srcAuth, tgtAuth,
      user.sourceEmail, user.targetEmail,
      srcFolder, tgtFolderId,
      logger, stats
    );
    totalDrafts += stats.fixed + stats.failed;
  }

  if (totalDrafts === 0) logger.info('   ✅ No drafts found — nothing to fix!');
  else logger.success(`   User done: ${stats.fixed} fixed, ${stats.failed} failed`);

  return stats;
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  const mainLogger = new Logger(config.logs_dir || './logs', 'fixdrafts');

  console.log(chalk.bold.cyan('\n🔧 M365 Draft Fixer v4'));
  console.log(chalk.gray(`   Source:     ${config.source_tenant.domain}`));
  console.log(chalk.gray(`   Target:     ${config.target_tenant.domain}`));
  console.log(chalk.gray(`   Batch size: ${BATCH_SIZE} requests/call (JSON batching)`));
  if (DRY_RUN) console.log(chalk.yellow('   ⚠️  DRY RUN\n'));
  else console.log('');

  const userLoader = new UserLoader(config.users_csv || './users.csv');
  let users = userLoader.load();
  if (ONLY_USER) {
    users = users.filter(u => u.sourceEmail.toLowerCase() === ONLY_USER.toLowerCase());
    if (!users.length) { mainLogger.error(`User not found: ${ONLY_USER}`); process.exit(1); }
  }

  mainLogger.info('Authenticating...');
  const srcAuth = new TenantAuth(config.source_tenant, 'SOURCE');
  const tgtAuth = new TenantAuth(config.target_tenant, 'TARGET');
  await srcAuth.getToken();
  await tgtAuth.getToken();

  // Conservative throttle for listing — batching handles the bulk operations
  const fixConfig = { ...config.migration, throttle_delay_ms: 300 };
  const srcClient = new GraphClient(srcAuth, fixConfig, mainLogger);
  const tgtClient = new GraphClient(tgtAuth, fixConfig, mainLogger);
  mainLogger.success('Both tenants authenticated ✓\n');

  const globalStats = { fixed: 0, failed: 0 };

  for (const user of users) {
    const logger = new Logger(config.logs_dir || './logs', user.sourceEmail);
    try {
      const s = await processUser(srcClient, tgtClient, srcAuth, tgtAuth, user, logger);
      globalStats.fixed  += s.fixed;
      globalStats.failed += s.failed;
    } catch (err) {
      logger.error(`Fatal error for ${user.sourceEmail}: ${err.message}`);
    }
  }

  console.log('\n' + chalk.bold('─'.repeat(50)));
  console.log(chalk.bold.green('✅ Fix complete!'));
  console.log(chalk.green(`   Fixed:  ${globalStats.fixed}`));
  if (globalStats.failed > 0)
    console.log(chalk.red(`   Failed: ${globalStats.failed}`));
  console.log('');
}

main().catch(err => {
  console.error(chalk.red('Fatal:'), err.message);
  process.exit(1);
});