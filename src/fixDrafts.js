#!/usr/bin/env node
/**
 * fixDrafts.js — Corrige mensagens migradas incorretamente como [Rascunho]
 *
 * Execução 100% sequencial — sem concurrency para evitar rate limiting.
 * Por pasta: Fase 1 (fetch) → Fase 2 (delete) → delay → Fase 3 (create)
 *
 * Usage:
 *   node src/fixDrafts.js                      # todos os usuários
 *   node src/fixDrafts.js --user email@domain  # só um usuário
 *   node src/fixDrafts.js --dry-run            # simulação
 */

const fs       = require('fs');
const path     = require('path');
const minimist = require('minimist');
const chalk    = require('chalk');
const TenantAuth  = require('./auth');
const GraphClient = require('./graphClient');
const UserLoader  = require('./userLoader');
const pLimit   = require('p-limit');
const Logger      = require('./logger');

const MIGRATION_PROPERTY_ID = 'String {8ECCC264-6880-4EBE-992F-8888D2EEAA1D} Name SourceMessageId';

const argv      = minimist(process.argv.slice(2));
const ONLY_USER = argv.user || argv.u || null;
const DRY_RUN   = argv['dry-run'] || argv.d || false;
const PAGE_SIZE = 100;

// Conservative delays to avoid rate limiting
const THROTTLE_MS    = 150;  // between individual API calls
const PHASE_DELAY_MS = 2000; // between delete and create phases
const CONCURRENCY    = 3;    // parallel per phase

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
      { id: 'String 0x001A', value: 'IPM.Note' },   // message class = email
      { id: 'Integer 0x0E07', value: '5' },          // PR_MESSAGE_FLAGS: Read(1)+Submit(4) = non-draft
      { id: 'Integer 0x0E17', value: '1' },          // PR_MESSAGE_STATE: not draft
      sourceId && { id: MIGRATION_PROPERTY_ID, value: sourceId }
    ].filter(Boolean)
  };
}

async function resolveFolderFresh(client, userEmail, folderName) {
  if (WELL_KNOWN[folderName]) {
    try {
      const f = await client.get(`/users/${userEmail}/mailFolders/${WELL_KNOWN[folderName]}`);
      return f.id;
    } catch (e) { /* fall through */ }
  }
  for await (const f of client.paginate(`/users/${userEmail}/mailFolders`, { '$top': 50 })) {
    if (f.displayName === folderName) return f.id;
  }
  return null;
}

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

async function fixFolder(srcClient, tgtClient, srcEmail, tgtEmail, srcFolder, tgtFolderId, logger, stats) {
  const drafts = await getDrafts(tgtClient, tgtEmail, tgtFolderId);
  if (drafts.length === 0) return;

  logger.info(`\n   📁 ${srcFolder.displayName}: ${chalk.yellow(drafts.length + ' drafts')}`);

  if (DRY_RUN) {
    for (const d of drafts) logger.info(`   [DRY RUN] Would fix: "${d.subject}"`);
    stats.fixed += drafts.length;
    return;
  }

  // ── PHASE 1: Fetch originals (concurrency 3) ────────────────────────────
  logger.info(`   ⬇️  Phase 1/3: Fetching ${drafts.length} originals...`);
  const limit1 = pLimit(CONCURRENCY);
  let fetchCount = 0;
  const enriched = await Promise.all(
    drafts.map(draft => limit1(async () => {
      const original = await fetchOriginal(srcClient, srcEmail, srcFolder.id, draft);
      await sleep(THROTTLE_MS);
      fetchCount++;
      if (fetchCount % 50 === 0 || fetchCount === drafts.length) {
        const pct = Math.round((fetchCount / drafts.length) * 100);
        const subj = (draft.subject || '').substring(0, 50);
        logger.info(`   ⬇️  [${fetchCount}/${drafts.length}] ${pct}% — "${subj}"`);
      }
      return { draft, original };
    }))
  );
  const foundCount = enriched.filter(e => e.original).length;
  logger.info(`   ✓ Phase 1 done: ${foundCount} from source, ${enriched.length - foundCount} from draft copy`);

  // ── PHASE 2: Delete all drafts (concurrency 3) ──────────────────────────
  logger.info(`   🗑️  Phase 2/3: Deleting ${drafts.length} drafts...`);
  const limit2 = pLimit(CONCURRENCY);
  let delCount = 0;
  const deleteResults = await Promise.all(
    enriched.map(({ draft }) => limit2(async () => {
      try {
        await tgtClient.request('DELETE', `/users/${tgtEmail}/messages/${draft.id}`);
        await sleep(THROTTLE_MS);
        delCount++;
        if (delCount % 50 === 0 || delCount === drafts.length) {
          const pct = Math.round((delCount / drafts.length) * 100);
          const subj = (draft.subject || '').substring(0, 50);
          logger.info(`   🗑️  [${delCount}/${drafts.length}] ${pct}% — "${subj}"`);
        }
        return { id: draft.id, ok: true };
      } catch (e) {
        logger.warn(`   ⚠️  Delete failed "${draft.subject}": ${e.message}`);
        return { id: draft.id, ok: false };
      }
    }))
  );
  const deleted = deleteResults.filter(r => r.ok).map(r => r.id);
  logger.info(`   ✓ Phase 2 done: ${deleted.length}/${drafts.length} deleted`);

  // Wait for Exchange to settle
  logger.info(`   ⏳ Waiting ${PHASE_DELAY_MS/1000}s for Exchange to settle...`);
  await sleep(PHASE_DELAY_MS);

  // Resolve folder fresh once
  const freshFolderId = await resolveFolderFresh(tgtClient, tgtEmail, srcFolder.displayName);
  if (!freshFolderId) {
    logger.error(`   ✗ Could not resolve folder "${srcFolder.displayName}"`);
    stats.failed += drafts.length;
    return;
  }

  // ── PHASE 3: Recreate (concurrency 2) ───────────────────────────────────
  logger.info(`   ✉️  Phase 3/3: Recreating ${deleted.length} messages...`);
  const deletedSet = new Set(deleted);
  const toCreate   = enriched.filter(e => deletedSet.has(e.draft.id));
  const limit3     = pLimit(CONCURRENCY);

  let createCount = 0;
  const createResults = await Promise.all(
    toCreate.map(({ draft, original }) => limit3(async () => {
      try {
        const payload = original
          ? buildPayload(original, original.id, true)
          : buildPayload(draft, null, false);
        await tgtClient.post(
          `/users/${tgtEmail}/mailFolders/${freshFolderId}/messages`,
          payload
        );
        await sleep(THROTTLE_MS);
        createCount++;
        if (createCount % 50 === 0 || createCount === toCreate.length) {
          const pct  = Math.round((createCount / toCreate.length) * 100);
          const subj = (draft.subject || '').substring(0, 50);
          const src  = original ? '✓src' : '~draft';
          logger.info(`   ✉️  [${createCount}/${toCreate.length}] ${pct}% ${src} — "${subj}"`);
        }
        return true;
      } catch (e) {
        logger.error(`   ✗ Create failed "${draft.subject}": ${e.message}`);
        return false;
      }
    }))
  );
  const created = createResults.filter(Boolean).length;
  const failed  = createResults.filter(r => !r).length;

  logger.info(`   ✓ Phase 3 done: ${created} created, ${failed} failed`);
  stats.fixed  += created;
  stats.failed += failed;
}

async function processUser(srcClient, tgtClient, user, logger) {
  const stats = { fixed: 0, failed: 0 };
  logger.info(`\n👤 ${user.sourceEmail} → ${user.targetEmail}`);

  // Load folders with $expand — fewer API calls
  logger.info('   📂 Loading folders...');
  const srcFolders = [];
  for await (const f of srcClient.paginate(`/users/${user.sourceEmail}/mailFolders`, { '$expand': 'childFolders', '$top': 100 })) {
    srcFolders.push(f);
    if (f.childFolders?.length) srcFolders.push(...f.childFolders);
  }

  const tgtFolderMap = {};
  for await (const f of tgtClient.paginate(`/users/${user.targetEmail}/mailFolders`, { '$expand': 'childFolders', '$top': 100 })) {
    tgtFolderMap[f.displayName] = f.id;
    if (f.childFolders?.length) {
      for (const c of f.childFolders) tgtFolderMap[c.displayName] = c.id;
    }
  }
  logger.info(`   ✓ ${srcFolders.length} source, ${Object.keys(tgtFolderMap).length} target folders`);

  let totalDrafts = 0;

  for (const srcFolder of srcFolders) {
    let tgtFolderId = tgtFolderMap[srcFolder.displayName];
    if (!tgtFolderId && WELL_KNOWN[srcFolder.displayName]) {
      try {
        const f = await tgtClient.get(`/users/${user.targetEmail}/mailFolders/${WELL_KNOWN[srcFolder.displayName]}`);
        tgtFolderId = f.id;
      } catch (e) { continue; }
    }
    if (!tgtFolderId) continue;

    await fixFolder(srcClient, tgtClient, user.sourceEmail, user.targetEmail, srcFolder, tgtFolderId, logger, stats);
    totalDrafts += stats.fixed + stats.failed;
  }

  if (totalDrafts === 0) logger.info('   ✅ No drafts found!');
  else logger.success(`   Done: ${stats.fixed} fixed, ${stats.failed} failed`);

  return stats;
}

async function main() {
  const mainLogger = new Logger(config.logs_dir || './logs', 'fixdrafts');

  console.log(chalk.bold.cyan('\n🔧 M365 Draft Fixer'));
  console.log(chalk.gray(`   Source: ${config.source_tenant.domain}`));
  console.log(chalk.gray(`   Target: ${config.target_tenant.domain}`));
  console.log(chalk.gray(`   Mode:   Sequential (no concurrency)`));
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

  const fixConfig = { ...config.migration, throttle_delay_ms: 200 }; // GraphClient internal throttle
  const srcClient = new GraphClient(srcAuth, fixConfig, mainLogger);
  const tgtClient = new GraphClient(tgtAuth, fixConfig, mainLogger);
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
  console.log(chalk.green(`   Fixed:  ${globalStats.fixed}`));
  if (globalStats.failed > 0)
    console.log(chalk.red(`   Failed: ${globalStats.failed}`));
  console.log('');
}

main().catch(err => {
  console.error(chalk.red('Fatal:'), err.message);
  process.exit(1);
});