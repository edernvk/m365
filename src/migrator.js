const fs = require('fs');
const UserLoader = require('./userLoader');
const EmailMigrator = require('./emailMigrator');
const OneDriveMigrator = require('./onedriveMigrator');
const CalendarMigrator = require('./calendarMigrator');
const CheckpointManager = require('./checkpoint');
const Logger = require('./logger');
const TenantAuth = require('./auth');
const GraphClient = require('./graphClient');

async function main() {
  const logger = new Logger('main');
  
  // Parse command line arguments
  const args = process.argv.slice(2);
  const workloadArg = args.find(arg => arg.startsWith('--workload'));
  const syncMode = args.includes('--sync');
  
  const workload = workloadArg ? workloadArg.split('=')[1] : 'all';
  
  if (syncMode) {
    logger.info('🔄 SYNC MODE ENABLED - Will check for new messages in all folders');
  }
  
  try {
    // Load config
    const config = JSON.parse(fs.readFileSync('./config.json', 'utf8'));
    
    // Load users
    const userLoader = new UserLoader(config.users_csv);
    const users = userLoader.load();
    
    logger.info(`📧 M365 Tenant-to-Tenant Migrator`);
    logger.info(`   Source: ${config.source_tenant.domain}`);
    logger.info(`   Target: ${config.target_tenant.domain}`);
    logger.info(`   Workload: ${workload}`);
    if (syncMode) {
      logger.info(`   Mode: SYNC (incremental - only new messages)`);
    }
    
    // Load checkpoint
    const checkpointManager = new CheckpointManager(config.checkpoint_file);
    const checkpoint = checkpointManager.load();
    
    logger.info(`Loaded ${users.length} user(s) for migration`);
    
    // Authenticate to source and target (CORRIGIDO!)
    logger.info('Authenticating to source tenant...');
    const sourceAuth = new TenantAuth(config.source_tenant, 'source');
    await sourceAuth.getToken();
    const sourceClient = new GraphClient(sourceAuth, config.migration, logger);
    logger.success('Source tenant authenticated ✓');
    
    logger.info('Authenticating to target tenant...');
    const targetAuth = new TenantAuth(config.target_tenant, 'target');
    await targetAuth.getToken();
    const targetClient = new GraphClient(targetAuth, config.migration, logger);
    logger.success('Target tenant authenticated ✓');
    
    // Migrate users
    for (const user of users) {
      const userLogger = new Logger(user.sourceEmail.replace('@', '_').replace(/\./g, '_'));
      
      try {
        // Email migration
        if (workload === 'all' || workload === 'email') {
          const emailKey = `${user.sourceEmail}_email`;
          
          // Sync mode: reprocess even if marked as done
          if (checkpoint[emailKey] === 'done' && !syncMode) {
            userLogger.info(`Skipping email (already completed)`);
          } else {
            if (checkpoint[emailKey] === 'done' && syncMode) {
              userLogger.info(`🔄 SYNC MODE: Re-processing ${user.sourceEmail} for new messages`);
            } else {
              userLogger.info(`Starting email for ${user.sourceEmail}`);
            }
            
            userLogger.info(`Starting email migration: ${user.sourceEmail} → ${user.targetEmail}`);
            
            const emailMigrator = new EmailMigrator(
              sourceClient,
              targetClient,
              { ...config.migration, sync: syncMode },
              userLogger,
              checkpointManager
            );
            
            const result = await emailMigrator.migrate(
              user.sourceEmail,
              user.targetEmail,
              checkpoint
            );
            
            if (!result.success) {
              userLogger.error(`Email migration failed: ${result.error}`);
            } else {
              userLogger.success(`Email migration completed successfully`);
            }
          }
        }
        
        // OneDrive migration
        if (workload === 'all' || workload === 'onedrive') {
          userLogger.info(`Starting OneDrive for ${user.sourceEmail}`);
          
          const onedriveMigrator = new OneDriveMigrator(
            sourceClient,
            targetClient,
            config.migration,
            userLogger,
            checkpointManager
          );
          
          const result = await onedriveMigrator.migrate(
            user.sourceEmail,
            user.targetEmail,
            checkpoint
          );
          
          if (!result.success) {
            userLogger.error(`OneDrive migration failed: ${result.error}`);
          } else {
            userLogger.success(`OneDrive migration completed successfully`);
          }
        }
        
        // Calendar migration
        if (workload === 'all' || workload === 'calendar') {
          userLogger.info(`Starting calendar for ${user.sourceEmail}`);
          
          const calendarMigrator = new CalendarMigrator(
            sourceClient,
            targetClient,
            config.migration,
            userLogger,
            checkpointManager
          );
          
          const result = await calendarMigrator.migrate(
            user.sourceEmail,
            user.targetEmail,
            checkpoint
          );
          
          if (!result.success) {
            userLogger.error(`Calendar migration failed: ${result.error}`);
          } else {
            userLogger.success(`Calendar migration completed successfully`);
          }
        }
        
      } catch (err) {
        userLogger.error(`Migration failed for ${user.sourceEmail}: ${err.message}`);
      }
    }
    
    logger.success('Migration process completed');
    
  } catch (err) {
    logger.error(`Migration failed: ${err.message}`);
    process.exit(1);
  }
}

main();