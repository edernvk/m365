/**
 * verifyMigration.js — Compare source vs target mailboxes
 *
 * For each user, compares message counts per folder between source and target.
 * Reports missing messages and overall match percentage.
 *
 * Same patterns as fixAttachments.js: folder loading, well-known resolution, etc.
 */

'use strict';

const fs          = require('fs');
const path        = require('path');
const chalk       = require('chalk');
const Logger      = require('./logger');
const TenantAuth  = require('./auth');
const GraphClient = require('./graphClient');
const UserLoader  = require('./userLoader');

// ── Constants ─────────────────────────────────────────────────────────────────
const PAGE_SIZE = 100;
const USER_DELAY_MS = 60000;

const WELL_KNOWN = {
  'Caixa de Entrada': 'inbox',       'Inbox': 'inbox',
  'Itens Enviados':   'sentitems',   'Sent Items': 'sentitems',
  'Itens Excluídos':  'deleteditems','Deleted Items': 'deleteditems',
  'Rascunhos':        'drafts',      'Drafts': 'drafts',
  'Lixo Eletrônico':  'junkemail',   'Junk Email': 'junkemail',
  'Arquivo Morto':    'archive',     'Archive': 'archive',
};

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

// ── Get message count for a folder ──────────────────────────────────────────
async function getMessageCount(client, userEmail, folderId) {
  try {
    const detail = await client.get(
      `/users/${userEmail}/mailFolders/${folderId}`,
      { '$select': 'id,displayName,totalItemCount' }
    );
    return detail.totalItemCount || 0;
  } catch (e) {
    return -1;
  }
}

// ── Load folders with well-known resolution ─────────────────────────────────
async function loadFolders(client, userEmail, isTarget = false) {
  const map = new Map(); // displayName.toLowerCase() → { id, displayName }

  for await (const f of client.paginate(
    `/users/${userEmail}/mailFolders`,
    { '$top': 100, '$expand': 'childFolders' }, 'folders'
  )) {
    map.set(f.displayName.toLowerCase(), { id: f.id, displayName: f.displayName });
    for (const c of (f.childFolders || [])) {
      map.set(c.displayName.toLowerCase(), { id: c.id, displayName: c.displayName });
    }
  }

  // Resolve well-known folders
  if (isTarget) {
    for (const [name, wkId] of Object.entries(WELL_KNOWN)) {
      try {
        const f = await client.get(`/users/${userEmail}/mailFolders/${wkId}`);
        map.set(name.toLowerCase(), { id: f.id, displayName: f.displayName });
        map.set(f.displayName.toLowerCase(), { id: f.id, displayName: f.displayName });
      } catch (e) { /* skip */ }
    }
  }

  return map;
}

// ── Process a single user ─────────────────────────────────────────────────────
async function processUser(srcClient, tgtClient, user, logger) {
  const stats = { totalSource: 0, totalTarget: 0, totalMissing: 0, foldersChecked: 0 };
  logger.info(`\n👤 ${user.sourceEmail} → ${user.targetEmail}`);
  logger.info('   📂 Loading folders...');

  const srcFolders = await loadFolders(srcClient, user.sourceEmail, false);
  const tgtFolders = await loadFolders(tgtClient, user.targetEmail, true);

  logger.info(`   ✓ ${srcFolders.size} source folders, ${tgtFolders.size} target folders`);

  const results = [];
  const checkedSrc = new Set();

  for (const [nameKey, srcFolder] of srcFolders) {
    if (checkedSrc.has(srcFolder.id)) continue;
    checkedSrc.add(srcFolder.id);

    const tgtFolder = tgtFolders.get(nameKey);
    if (!tgtFolder) {
      // Try well-known mapping
      const wkName = Object.keys(WELL_KNOWN).find(k => k.toLowerCase() === nameKey);
      if (wkName) {
        const altKey = Object.entries(WELL_KNOWN)
          .filter(([, v]) => v === WELL_KNOWN[wkName])
          .map(([k]) => k.toLowerCase())
          .find(k => tgtFolders.has(k));
        if (altKey) {
          const tgtF = tgtFolders.get(altKey);
          const srcCount = await getMessageCount(srcClient, user.sourceEmail, srcFolder.id);
          const tgtCount = await getMessageCount(tgtClient, user.targetEmail, tgtF.id);
          if (srcCount >= 0 && tgtCount >= 0) {
            const missing = Math.max(0, srcCount - tgtCount);
            const pct = srcCount > 0 ? ((tgtCount / srcCount) * 100).toFixed(1) : '100.0';
            results.push({ folder: srcFolder.displayName, srcCount, tgtCount, missing, pct });
            stats.totalSource += srcCount;
            stats.totalTarget += tgtCount;
            stats.totalMissing += missing;
            stats.foldersChecked++;
          }
          continue;
        }
      }
      continue;
    }

    const srcCount = await getMessageCount(srcClient, user.sourceEmail, srcFolder.id);
    const tgtCount = await getMessageCount(tgtClient, user.targetEmail, tgtFolder.id);

    if (srcCount < 0 || tgtCount < 0) continue;

    const missing = Math.max(0, srcCount - tgtCount);
    const pct = srcCount > 0 ? ((tgtCount / srcCount) * 100).toFixed(1) : '100.0';

    results.push({ folder: srcFolder.displayName, srcCount, tgtCount, missing, pct });
    stats.totalSource += srcCount;
    stats.totalTarget += tgtCount;
    stats.totalMissing += missing;
    stats.foldersChecked++;
  }

  // Sort: folders with missing messages first
  results.sort((a, b) => b.missing - a.missing);

  // Print results
  logger.info(`\n   📊 Verification Results:`);
  for (const r of results) {
    if (r.srcCount === 0) continue; // skip empty folders
    const status = r.missing === 0 ? '✅' : r.missing <= 3 ? '⚠️' : '❌';
    logger.info(
      `   ${status} ${r.folder.padEnd(30)} ${String(r.srcCount).padStart(6)} source / ${String(r.tgtCount).padStart(6)} target — ${r.missing > 0 ? `${r.missing} missing` : 'OK'} (${r.pct}%)`
    );
  }

  const globalPct = stats.totalSource > 0
    ? ((stats.totalTarget / stats.totalSource) * 100).toFixed(1)
    : '100.0';

  logger.info(`\n   ════════════════════════════════════════════════`);
  logger.info(`   📊 TOTAL: ${stats.totalSource} source / ${stats.totalTarget} target — ${stats.totalMissing} missing (${globalPct}% match)`);
  logger.info(`   📁 ${stats.foldersChecked} folders checked`);

  // Use "fixed" = verified count, "failed" = missing count for dashboard compat
  logger.info(`\nUser done: ${stats.totalTarget} fixed, ${stats.totalMissing} failed`);

  return stats;
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  const mainLogger = new Logger(config.logs_dir || './logs', 'verify-migration');

  mainLogger.info(chalk.cyan('\n✅ Migration Verifier'));
  mainLogger.info(`   Source: ${config.source_tenant.domain}`);
  mainLogger.info(`   Target: ${config.target_tenant.domain}`);

  const userLoader = new UserLoader(config.users_csv);
  let users = userLoader.load();
  if (ONLY_USER) {
    users = users.filter(u => u.sourceEmail.toLowerCase() === ONLY_USER.toLowerCase());
    if (!users.length) { mainLogger.error(`User not found: ${ONLY_USER}`); process.exit(1); }
  }
  mainLogger.info(`   Users: ${users.length}${ONLY_USER ? ` (filtered: ${ONLY_USER})` : ''}`);

  const srcAuth = new TenantAuth(config.source_tenant, 'source');
  await srcAuth.getToken();
  const srcClient = new GraphClient(srcAuth, config.migration, mainLogger);

  const tgtAuth = new TenantAuth(config.target_tenant, 'target');
  await tgtAuth.getToken();
  const tgtClient = new GraphClient(tgtAuth, config.migration, mainLogger);

  mainLogger.success('Both tenants authenticated ✓');

  const globalStats = { totalSource: 0, totalTarget: 0, totalMissing: 0 };

  for (let i = 0; i < users.length; i++) {
    const user = users[i];
    const logger = new Logger(
      config.logs_dir || './logs',
      user.sourceEmail.replace('@', '_').replace(/\./g, '_')
    );

    try {
      const s = await processUser(srcClient, tgtClient, user, logger);
      globalStats.totalSource  += s.totalSource;
      globalStats.totalTarget  += s.totalTarget;
      globalStats.totalMissing += s.totalMissing;
    } catch (err) {
      logger.error(`Fatal error for ${user.sourceEmail}: ${err.message}`);
    }

    if (i < users.length - 1) {
      mainLogger.info(`\n   ⏳ Waiting 60s before next user (quota recovery)...`);
      await sleep(USER_DELAY_MS);
    }
  }

  const globalPct = globalStats.totalSource > 0
    ? ((globalStats.totalTarget / globalStats.totalSource) * 100).toFixed(1)
    : '100.0';

  mainLogger.info(chalk.green(`\n✅ All done: ${globalStats.totalTarget}/${globalStats.totalSource} emails verified (${globalPct}% match), ${globalStats.totalMissing} missing`));
}

main().catch(err => {
  console.error(chalk.red('Fatal:'), err.message);
  process.exit(1);
});
