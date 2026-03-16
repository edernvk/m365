'use strict';

const fs                = require('fs');
const UserLoader        = require('./userLoader');
const EmailMigrator     = require('./emailMigrator');
const OneDriveMigrator  = require('./onedriveMigrator');
const CalendarMigrator  = require('./calendarMigrator');
const CheckpointManager = require('./checkpoint');
const Logger            = require('./logger');
const TenantAuth        = require('./auth');
const GraphClient       = require('./graphClient');

const USER_DELAY_MS = 60000; // 60s between users — quota recovery

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function main() {
  const logger = new Logger('main');

  // CLI flags: --workload email|calendar|contacts|onedrive|all  --user email@domain
  const args         = process.argv.slice(2);
  const workloadArg  = args.find(a => a.startsWith('--workload'))?.split('=')[1]
                    || args[args.indexOf('--workload') + 1]
                    || null;
  const userFilter   = args.find(a => a.startsWith('--user'))?.split('=')[1]
                    || (args.indexOf('--user') !== -1 ? args[args.indexOf('--user') + 1] : null);

  try {
    const config = JSON.parse(fs.readFileSync('./config.json', 'utf8'));

    // Determine active workloads
    let activeWorkloads = config.migration.workloads || ['email'];
    if (workloadArg) {
      activeWorkloads = workloadArg === 'all'
        ? ['email', 'calendar', 'contacts', 'onedrive']
        : workloadArg.split(',');
    }

    const userLoader = new UserLoader(config.users_csv);
    let users = userLoader.load();
    if (userFilter) {
      users = users.filter(u => u.sourceEmail === userFilter || u.targetEmail === userFilter);
      if (users.length === 0) { logger.error(`No user found matching: ${userFilter}`); process.exit(1); }
    }

    logger.info(`\n🚀 M365 Tenant-to-Tenant Migrator`);
    logger.info(`   Source:    ${config.source_tenant.domain}`);
    logger.info(`   Target:    ${config.target_tenant.domain}`);
    logger.info(`   Workloads: ${activeWorkloads.join(', ')}`);
    logger.info(`   Users:     ${users.length}`);

    const checkpointManager = new CheckpointManager(config.checkpoint_file);
    const checkpoint        = checkpointManager.load();

    logger.info('Authenticating...');
    const sourceAuth = new TenantAuth(config.source_tenant, 'source');
    const targetAuth = new TenantAuth(config.target_tenant, 'target');
    await sourceAuth.getToken();
    await targetAuth.getToken();
    const sourceClient = new GraphClient(sourceAuth, config.migration, logger);
    const targetClient = new GraphClient(targetAuth, config.migration, logger);
    logger.success('Both tenants authenticated ✓');

    for (let i = 0; i < users.length; i++) {
      const user       = users[i];
      const userLogger = new Logger(config.logs_dir || './logs',
                           user.sourceEmail.replace('@', '_').replace(/\./g, '_'));

      logger.info(`\n${'─'.repeat(60)}`);
      logger.info(`👤 [${i+1}/${users.length}] ${user.sourceEmail} → ${user.targetEmail}`);

      // ── EMAIL ──────────────────────────────────────────────────────────────
      if (activeWorkloads.includes('email')) {
        try {
          userLogger.info('📧 Starting email migration...');
          const emailMigrator = new EmailMigrator(
            sourceClient, targetClient, config.migration, userLogger, checkpointManager
          );
          const result = await emailMigrator.migrate(user.sourceEmail, user.targetEmail, checkpoint);
          if (!result.success) userLogger.error(`Email failed: ${result.error}`);
          else userLogger.success(`Email done: ${result.stats.messages_migrated} migrated`);
        } catch (err) {
          userLogger.error(`Email fatal: ${err.message}`);
        }
      }

      // ── CALENDAR ───────────────────────────────────────────────────────────
      if (activeWorkloads.includes('calendar')) {
        try {
          userLogger.info('📅 Starting calendar migration...');
          const calMigrator = new CalendarMigrator(
            sourceClient, targetClient, config.migration, userLogger
          );
          const result = await calMigrator.migrateCalendar(user.sourceEmail, user.targetEmail, checkpoint);
          if (!result.success) userLogger.error(`Calendar failed: ${result.error}`);
          else userLogger.success(`Calendar done: ${result.stats.migrated} events migrated`);
        } catch (err) {
          userLogger.error(`Calendar fatal: ${err.message}`);
        }
      }

      // ── CONTACTS ───────────────────────────────────────────────────────────
      if (activeWorkloads.includes('contacts')) {
        try {
          userLogger.info('👥 Starting contacts migration...');
          const calMigrator = new CalendarMigrator(
            sourceClient, targetClient, config.migration, userLogger
          );
          const result = await calMigrator.migrateContacts(user.sourceEmail, user.targetEmail, checkpoint);
          if (!result.success) userLogger.error(`Contacts failed: ${result.error}`);
          else userLogger.success(`Contacts done: ${result.stats.migrated} contacts migrated`);
        } catch (err) {
          userLogger.error(`Contacts fatal: ${err.message}`);
        }
      }

      // ── ONEDRIVE ───────────────────────────────────────────────────────────
      if (activeWorkloads.includes('onedrive')) {
        try {
          userLogger.info('☁️  Starting OneDrive migration...');
          const odMigrator = new OneDriveMigrator(
            sourceClient, targetClient, config.migration, userLogger, checkpointManager
          );
          const result = await odMigrator.migrate(user.sourceEmail, user.targetEmail, checkpoint);
          if (!result.success) userLogger.error(`OneDrive failed: ${result.error}`);
          else userLogger.success(`OneDrive done`);
        } catch (err) {
          userLogger.error(`OneDrive fatal: ${err.message}`);
        }
      }

      // Save checkpoint after each user
      checkpointManager.save();
      userLogger.info('💾 Checkpoint saved');

      // Wait between users (quota recovery)
      if (i < users.length - 1) {
        logger.info(`\n⏳ Waiting 60s before next user (quota recovery)...`);
        await sleep(USER_DELAY_MS);
      }
    }

    logger.success('\n✅ All migrations completed');

  } catch (err) {
    logger.error(`Fatal: ${err.message}`);
    process.exit(1);
  }
}

main();
