#!/usr/bin/env node
/**
 * fixDrafts.js — Corrige mensagens migradas incorretamente como [Rascunho]
 *
 * COMO FUNCIONA:
 *   - Worker A: busca rascunhos no destino + coleta dados do original na origem → fila
 *   - Worker B: consome a fila em paralelo → DELETE rascunho + POST correto
 *   - A mensagem é criada com Integer 0x0E07 = 1 que remove o flag de rascunho
 *     diretamente no POST (PATCH isDraft:false é ignorado silenciosamente pela API)
 *
 * Usage:
 *   node src/fixDrafts.js                        # todos os usuários
 *   node src/fixDrafts.js --user email@domain    # só um usuário
 *   node src/fixDrafts.js --dry-run              # simulação
 *   node src/fixDrafts.js --concurrency 5        # paralelismo (default: 3)
 */

const fs      = require('fs');
const path    = require('path');
const minimist = require('minimist');
const chalk   = require('chalk');
const pLimit  = require('p-limit');
const TenantAuth  = require('./auth');
const GraphClient = require('./graphClient');
const UserLoader  = require('./userLoader');
const Logger      = require('./logger');

const MIGRATION_PROPERTY_ID = 'String {8ECCC264-6880-4EBE-992F-8888D2EEAA1D} Name SourceMessageId';

const argv        = minimist(process.argv.slice(2));
const ONLY_USER   = argv.user || argv.u || null;
const DRY_RUN     = argv['dry-run'] || argv.d || false;
const CONCURRENCY = parseInt(argv.concurrency || argv.c || '3', 10);
const PAGE_SIZE   = 100;

const WELL_KNOWN = {
  'Caixa de Entrada': 'inbox',     'Inbox': 'inbox',
  'Itens Enviados':   'sentitems', 'Sent Items': 'sentitems',
  'Itens Excluídos':  'deleteditems', 'Deleted Items': 'deleteditems',
  'Rascunhos':        'drafts',    'Drafts': 'drafts',
  'Lixo Eletrônico':  'junkemail', 'Junk Email': 'junkemail',
  'Arquivo Morto':    'archive',   'Archive': 'archive',
};

let config;
try {
  config = JSON.parse(fs.readFileSync(path.resolve(process.cwd(), 'config.json'), 'utf8'));
} catch (e) {
  console.error(chalk.red('❌ config.json not found'));
  process.exit(1);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Sanitize headers: only x-, no duplicates, max 5
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

// Build the corrected message payload
// KEY: Integer 0x0E07 = 1 removes the draft flag at creation time.
// PATCH isDraft:false is silently ignored by Graph API.
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
      // Preserve original dates
      originalDate     && { id: 'SystemTime 0x0E06', value: originalDate },
      msg.sentDateTime && { id: 'SystemTime 0x0039', value: msg.sentDateTime },
      // Remove draft flag — the ONLY way that works in Graph API
      { id: 'Integer 0x0E07', value: '1' },
      // Track source message ID for future dedup
      sourceId && { id: MIGRATION_PROPERTY_ID, value: sourceId }
    ].filter(Boolean)
  };
}

// Resolve folder ID fresh from API each time to avoid stale IDs
async function resolveFolderFresh(client, userEmail, folderName) {
  if (WELL_KNOWN[folderName]) {
    try {
      const f = await client.get(`/users/${userEmail}/mailFolders/${WELL_KNOWN[folderName]}`);
      return f.id;
    } catch (e) { /* fall through */ }
  }
  for await (const f of client.paginate(`/users/${userEmail}/mailFolders`)) {
    if (f.displayName === folderName) return f.id;
    for await (const c of client.paginate(`/users/${userEmail}/mailFolders/${f.id}/childFolders`)) {
      if (c.displayName === folderName) return c.id;
    }
  }
  return null;
}

// Get all drafts in a folder
async function getDrafts(client, userEmail, folderId) {
  const drafts = [];
  let skip = 0;
  while (true) {
    const result = await client.get(
      `/users/${userEmail}/mailFolders/${folderId}/messages`,
      {
        '$top': PAGE_SIZE, '$skip': skip,
        '$filter': 'isDraft eq true',
        '$select': 'id,subject,receivedDateTime,sentDateTime,body,from,toRecipients,ccRecipients,bccRecipients,replyTo,flag,importance,isRead,isDraft',
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

// Try to fetch original from source
async function fetchOriginal(srcClient, srcEmail, srcFolderId, draft) {
  const sourceProp = draft.singleValueExtendedProperties?.find(p => p.id === MIGRATION_PROPERTY_ID);
  const sourceId   = sourceProp?.value || null;

  // Method 1: fetch directly by source ID
  if (sourceId) {
    try {
      return await srcClient.get(
        `/users/${srcEmail}/messages/${sourceId}`,
        { '$select': 'id,subject,receivedDateTime,sentDateTime,isDraft,body,from,toRecipients,ccRecipients,bccRecipients,replyTo,flag,importance,isRead,internetMessageHeaders' }
      );
    } catch (e) { /* fall through */ }
  }

  // Method 2: search by subject + receivedDateTime
  if (draft.subject && draft.receivedDateTime) {
    try {
      const safe = draft.subject.replace(/'/g, "''");
      const result = await srcClient.get(
        `/users/${srcEmail}/mailFolders/${srcFolderId}/messages`,
        {
          '$top': 1,
          '$filter': `subject eq '${safe}' and receivedDateTime eq ${draft.receivedDateTime}`,
          '$select': 'id,subject,receivedDateTime,sentDateTime,isDraft,body,from,toRecipients,ccRecipients,bccRecipients,replyTo,flag,importance,isRead,internetMessageHeaders'
        }
      );
      if (result.value?.[0]) return result.value[0];
    } catch (e) { /* fall through */ }
  }

  return null;
}

// Process one user
async function processUser(srcClient, tgtClient, user, logger) {
  const stats = { fixed: 0, failed: 0, total: 0 };

  logger.info(`\n👤 ${user.sourceEmail} → ${user.targetEmail}`);

  // Load folders from both tenants
  const srcFolders = [];
  for await (const f of srcClient.paginate(`/users/${user.sourceEmail}/mailFolders`)) {
    srcFolders.push(f);
    for await (const c of srcClient.paginate(`/users/${user.sourceEmail}/mailFolders/${f.id}/childFolders`)) {
      srcFolders.push(c);
    }
  }

  const tgtFolderMap = {};
  for await (const f of tgtClient.paginate(`/users/${user.targetEmail}/mailFolders`)) {
    tgtFolderMap[f.displayName] = f.id;
    for await (const c of tgtClient.paginate(`/users/${user.targetEmail}/mailFolders/${f.id}/childFolders`)) {
      tgtFolderMap[c.displayName] = c.id;
    }
  }

  const limit = pLimit(CONCURRENCY);

  for (const srcFolder of srcFolders) {
    // Resolve target folder ID
    let tgtFolderId = tgtFolderMap[srcFolder.displayName];
    if (!tgtFolderId && WELL_KNOWN[srcFolder.displayName]) {
      try {
        const f = await tgtClient.get(`/users/${user.targetEmail}/mailFolders/${WELL_KNOWN[srcFolder.displayName]}`);
        tgtFolderId = f.id;
      } catch (e) { continue; }
    }
    if (!tgtFolderId) continue;

    const drafts = await getDrafts(tgtClient, user.targetEmail, tgtFolderId);
    if (drafts.length === 0) continue;

    stats.total += drafts.length;
    logger.info(`   📁 ${srcFolder.displayName}: ${chalk.yellow(drafts.length + ' drafts')} — fixing with concurrency ${CONCURRENCY}...`);

    // ── PARALLEL: Worker A (fetch original) + Worker B (delete+create) ──────
    // We build tasks that each: fetch original → delete draft → create correct
    // This is naturally parallel via p-limit — up to CONCURRENCY tasks at once.
    // Worker A and B are merged per-message to avoid race conditions.

    const tasks = drafts.map(draft => limit(async () => {
      if (DRY_RUN) {
        logger.info(`   [DRY RUN] Would fix: "${draft.subject}"`);
        stats.fixed++;
        return;
      }

      // ── Worker A: fetch original from source ──────────────────────────────
      const original = await fetchOriginal(srcClient, user.sourceEmail, srcFolder.id, draft);

      // ── Resolve folder ID fresh before creating ───────────────────────────
      const freshFolderId = await resolveFolderFresh(tgtClient, user.targetEmail, srcFolder.displayName);
      if (!freshFolderId) {
        logger.error(`   ✗ Folder "${srcFolder.displayName}" not found — skipping "${draft.subject}"`);
        stats.failed++;
        return;
      }

      try {
        // ── Worker B: delete old draft ────────────────────────────────────
        await tgtClient.request('DELETE', `/users/${user.targetEmail}/messages/${draft.id}`);

        // ── Worker B: create corrected message ────────────────────────────
        const payload = original
          ? buildPayload(original, original.id, true)   // from source — with clean headers
          : buildPayload(draft,    null,         false); // from draft  — no headers (safe)

        await tgtClient.post(
          `/users/${user.targetEmail}/mailFolders/${freshFolderId}/messages`,
          payload
        );

        // NOTE: NO PATCH isDraft:false — it is silently ignored by Graph API.
        // The Integer 0x0E07 = 1 in the payload above is the correct mechanism.

        logger.info(`   ✓ ${original ? 'Fixed' : 'Recreated'}: "${draft.subject}"`);
        stats.fixed++;

      } catch (err) {
        logger.error(`   ✗ Failed "${draft.subject}": ${err.message}`);
        stats.failed++;
      }
    }));

    await Promise.all(tasks);

    logger.info(`   ✅ ${srcFolder.displayName} done: ${stats.fixed} fixed, ${stats.failed} failed`);
  }

  if (stats.total === 0) {
    logger.info('   ✅ No drafts found — nothing to fix!');
  } else {
    logger.success(`   User complete: ${stats.fixed}/${stats.total} fixed, ${stats.failed} failed`);
  }

  return stats;
}

// Main
async function main() {
  const mainLogger = new Logger(config.logs_dir || './logs', 'fixdrafts');

  console.log(chalk.bold.cyan('\n🔧 M365 Draft Fixer'));
  console.log(chalk.gray(`   Source:      ${config.source_tenant.domain}`));
  console.log(chalk.gray(`   Target:      ${config.target_tenant.domain}`));
  console.log(chalk.gray(`   Concurrency: ${CONCURRENCY} parallel fixes`));
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
  const srcClient = new GraphClient(srcAuth, config.migration, mainLogger);
  const tgtClient = new GraphClient(tgtAuth, config.migration, mainLogger);
  mainLogger.success('Both tenants authenticated ✓\n');

  const globalStats = { fixed: 0, failed: 0 };

  for (const user of users) {
    const logger = new Logger(config.logs_dir || './logs', user.sourceEmail);
    try {
      const s = await processUser(srcClient, tgtClient, user, logger);
      globalStats.fixed  += s.fixed;
      globalStats.failed += s.failed;
    } catch (err) {
      logger.error(`Fatal: ${err.message}`);
    }
  }

  console.log('\n' + chalk.bold('─'.repeat(50)));
  console.log(chalk.bold.green('✅ Fix complete!'));
  console.log(chalk.green(`   Fixed:    ${globalStats.fixed}`));
  if (globalStats.failed > 0)
    console.log(chalk.red(`   Failed:   ${globalStats.failed}`));
  console.log('');
}

main().catch(err => {
  console.error(chalk.red('Fatal:'), err.message);
  process.exit(1);
});