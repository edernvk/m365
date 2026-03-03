#!/usr/bin/env node
/**
 * M365 Tenant-to-Tenant Migrator
 * Usage: node src/migrator.js [--workload all|email|onedrive|calendar|contacts] [--user email@domain.com] [--dry-run]
 *
 * Required: config.json + users.csv in project root
 */

const path = require('path');
const minimist = require('minimist');
const pLimit = require('p-limit');
const cliProgress = require('cli-progress');
const chalk = require('chalk');

const TenantAuth = require('./auth');
const GraphClient = require('./graphClient');
const EmailMigrator = require('./emailMigrator');
const OneDriveMigrator = require('./onedriveMigrator');
const CalendarMigrator = require('./calendarMigrator');
const CheckpointManager = require('./checkpoint');
const Logger = require('./logger');
const { loadUsers } = require('./userLoader');

// ─── Parse CLI args ───────────────────────────────────────────────────────────
const argv = minimist(process.argv.slice(2));
const WORKLOAD = argv.workload || argv.w || 'all';
const ONLY_USER = argv.user || argv.u || null;
const DRY_RUN = argv['dry-run'] || argv.d || false;
const RESET = argv.reset || false;

// ─── Load config ──────────────────────────────────────────────────────────────
const CONFIG_PATH = path.resolve(process.cwd(), 'config.json');
let config;
try {
  config = require(CONFIG_PATH);
} catch (e) {
  console.error(chalk.red('❌ config.json not found or invalid. Copy config.json and fill in your tenant credentials.'));
  process.exit(1);
}

if (DRY_RUN) config.migration.dry_run = true;

// ─── Main ─────────────────────────────────────────────────────────────────────
async function main() {
  const mainLogger = new Logger(config.logs_dir || './logs', 'main');

  console.log(chalk.bold.cyan('\n🚀 M365 Tenant-to-Tenant Migrator'));
  console.log(chalk.gray(`   Source: ${config.source_tenant.domain}`));
  console.log(chalk.gray(`   Target: ${config.target_tenant.domain}`));
  console.log(chalk.gray(`   Workload: ${WORKLOAD}`));
  if (DRY_RUN) console.log(chalk.yellow('   ⚠️  DRY RUN mode — no data will be written\n'));

  // Load users
  let users;
  try {
    users = loadUsers(config.users_csv || './users.csv');
    if (ONLY_USER) {
      users = users.filter(u => u.source === ONLY_USER.toLowerCase());
      if (users.length === 0) throw new Error(`User not found in CSV: ${ONLY_USER}`);
    }
    mainLogger.info(`Loaded ${users.length} user(s) for migration`);
  } catch (err) {
    mainLogger.error(err.message);
    process.exit(1);
  }

  // Initialize auth for both tenants
  mainLogger.info('Authenticating to source tenant...');
  const srcAuth = new TenantAuth(config.source_tenant, 'SOURCE');
  const tgtAuth = new TenantAuth(config.target_tenant, 'TARGET');

  // Validate auth before starting
  try {
    await srcAuth.getToken();
    mainLogger.success('Source tenant authenticated ✓');
    await tgtAuth.getToken();
    mainLogger.success('Target tenant authenticated ✓');
  } catch (err) {
    mainLogger.error(`Authentication failed: ${err.message}`);
    mainLogger.error('Check your client_id, client_secret, and tenant_id in config.json');
    process.exit(1);
  }

  // Graph clients
  const srcClient = new GraphClient(srcAuth, config.migration);
  const tgtClient = new GraphClient(tgtAuth, config.migration);

  // Checkpoint manager with absolute path
  const checkpointPath = path.resolve(process.cwd(), config.checkpoint_file || 'resume.json');
  const checkpoint = new CheckpointManager(checkpointPath);
  if (RESET) {
    checkpoint.reset(ONLY_USER || null);
    mainLogger.info('Checkpoint reset');
  }

  // Progress bar
  const progressBar = new cliProgress.SingleBar({
    format: chalk.cyan('{bar}') + ' {percentage}% | {value}/{total} users | ETA: {eta}s',
    barCompleteChar: '█',
    barIncompleteChar: '░',
    hideCursor: true
  });

  progressBar.start(users.length, 0);

  // Run migrations with concurrency limit
  const limit = pLimit(config.migration.concurrent_users || 3);
  const results = { success: 0, failed: 0, skipped: 0 };

  await Promise.all(users.map(user => limit(async () => {
    const userLogger = new Logger(config.logs_dir || './logs', user.source);

    try {
      const workloads = WORKLOAD === 'all'
        ? (config.migration.workloads || ['email', 'calendar', 'contacts', 'onedrive'])
        : [WORKLOAD];

      for (const wl of workloads) {
        if (config.migration.resume_on_restart && checkpoint.isUserDone(user.source, wl)) {
          userLogger.info(`Skipping ${wl} (already completed)`);
          results.skipped++;
          continue;
        }

        userLogger.info(`Starting ${wl} for ${user.source}`);
        const cp = checkpoint.getUserCheckpoint(user.source, wl);
        let result;

        if (wl === 'email') {
          const migrator = new EmailMigrator(srcClient, tgtClient, config.migration, userLogger);
          // CRITICAL FIX: Pass checkpoint manager as 4th parameter
          result = await migrator.migrate(user.source, user.target, cp, checkpoint);
        } else if (wl === 'onedrive') {
          const migrator = new OneDriveMigrator(srcClient, tgtClient, config.migration, userLogger);
          result = await migrator.migrate(user.source, user.target, cp);
        } else if (wl === 'calendar') {
          const migrator = new CalendarMigrator(srcClient, tgtClient, config.migration, userLogger);
          result = await migrator.migrateCalendar(user.source, user.target, cp);
        } else if (wl === 'contacts') {
          const migrator = new CalendarMigrator(srcClient, tgtClient, config.migration, userLogger);
          result = await migrator.migrateContacts(user.source, user.target, cp);
        }

        if (result?.success) {
          checkpoint.markUserDone(user.source, wl);
          userLogger.updateSummary(user.source, wl, result.stats);
          results.success++;
        } else {
          results.failed++;
        }

        // Auto-save checkpoint after each workload
        checkpoint.save();
      }

    } catch (err) {
      userLogger.error(`Unexpected error for ${user.source}: ${err.message}`);
      results.failed++;
    }

    progressBar.increment();
  })));

  progressBar.stop();

  // Final report
  console.log('\n' + chalk.bold('─'.repeat(50)));
  console.log(chalk.bold.green(`✅ Migration complete!`));
  console.log(chalk.green(`   Success: ${results.success}`));
  console.log(chalk.yellow(`   Skipped: ${results.skipped}`));
  console.log(chalk.red(`   Failed:  ${results.failed}`));
  console.log(chalk.gray(`   Logs: ${path.resolve(config.logs_dir || './logs')}`));
  console.log(chalk.gray(`   Summary: ${path.resolve(config.logs_dir || './logs', 'summary.json')}`));
  console.log('');

  if (results.failed > 0) {
    console.log(chalk.yellow('⚠️  Some migrations failed. Check the logs and re-run to resume from where it stopped.'));
    console.log(chalk.yellow('   Re-run with: npm start (resume is automatic)\n'));
  }

  process.exit(results.failed > 0 ? 1 : 0);
}

main().catch(err => {
  console.error(chalk.red('Fatal error:'), err.message);
  process.exit(1);
});