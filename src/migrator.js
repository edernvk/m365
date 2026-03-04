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
  
  try {
    // Load config
    const config = JSON.parse(fs.readFileSync('./config.json', 'utf8'));
    
    // Load users
    const userLoader = new UserLoader(config.users_csv);
    const users = userLoader.load();
    
    logger.info(`📧 M365 Tenant-to-Tenant Migrator`);
    logger.info(`   Source: ${config.source_tenant.domain}`);
    logger.info(`   Target: ${config.target_tenant.domain}`);
    logger.info(`   Workload: email`);
    
    // Load checkpoint
    const checkpointManager = new CheckpointManager(config.checkpoint_file);
    const checkpoint = checkpointManager.load();
    
    logger.info(`Loaded ${users.length} user(s) for migration`);
    
    // Authenticate to source and target
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
        userLogger.info(`Starting email migration: ${user.sourceEmail} → ${user.targetEmail}`);
        
        const emailMigrator = new EmailMigrator(
          sourceClient,
          targetClient,
          config.migration,
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