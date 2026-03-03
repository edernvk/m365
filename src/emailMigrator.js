/**
 * Email Migration Module
 * - Pre-scan: logs total messages and MB before migrating
 * - Deduplication: skips messages already in target (by internetMessageId)
 * - Checkpoint: skips already migrated messages on resume
 * - Date preservation: uses singleValueExtendedProperties to set original date
 * - Headers: not preserved (Microsoft Graph API limit causes issues)
 */

// Extended property ID used by Outlook to store the sent/received date (PR_MESSAGE_DELIVERY_TIME)
const DATE_PROP_ID = 'SystemTime 0x0E06';

class EmailMigrator {
  constructor(sourceClient, targetClient, config, logger) {
    this.src = sourceClient;
    this.tgt = targetClient;
    this.config = config;
    this.logger = logger;
    this.pageSize = config.email_page_size || 100;
  }

  async migrate(sourceEmail, targetEmail, checkpoint = {}) {
    this.logger.info(`Starting email migration: ${sourceEmail} → ${targetEmail}`);

    const stats = {
      folders_total: 0,
      folders_done: 0,
      messages_total: 0,
      messages_migrated: 0,
      messages_skipped: 0,
      messages_failed: 0,
      bytes_total: 0
    };

    try {
      // 1. Get all folders
      const folders = await this._getAllFolders(sourceEmail);
      stats.folders_total = folders.length;
      this.logger.info(`Found ${folders.length} folders`);

      // 2. Pre-scan: count messages and size per folder
      this.logger.info('Scanning mailbox size (this may take a moment)...');
      let totalMessages = 0;
      let totalBytes = 0;
      const folderSizes = {};

      for (const folder of folders) {
        try {
          const detail = await this.src.get(
            `/users/${sourceEmail}/mailFolders/${folder.id}`,
            { '$select': 'id,displayName,totalItemCount,sizeInBytes' }
          );
          const count = detail.totalItemCount || 0;
          const bytes = detail.sizeInBytes || 0;
          folderSizes[folder.id] = { count, bytes };
          totalMessages += count;
          totalBytes += bytes;
        } catch (e) {
          folderSizes[folder.id] = { count: 0, bytes: 0 };
        }
      }

      stats.messages_total = totalMessages;
      stats.bytes_total = totalBytes;

      this.logger.info(
        `📊 Mailbox scan complete: ${totalMessages.toLocaleString()} messages | ${this._formatBytes(totalBytes)} total`
      );

      for (const folder of folders) {
        const sz = folderSizes[folder.id];
        if (sz && sz.count > 0) {
          this.logger.info(
            `   📁 ${folder.displayName.padEnd(30)} ${String(sz.count).padStart(6)} msgs | ${this._formatBytes(sz.bytes)}`
          );
        }
      }

      // 3. Migrate folder by folder
      for (const folder of folders) {
        const folderKey = `email_folder_${folder.id}`;

        if (checkpoint[folderKey] === 'done') {
          this.logger.info(`⏭  Skipping (already migrated): ${folder.displayName}`);
          stats.folders_done++;
          continue;
        }

        const sz = folderSizes[folder.id] || { count: 0, bytes: 0 };
        this.logger.info(
          `📂 Migrating [${stats.folders_done + 1}/${folders.length}]: ${folder.displayName} (${sz.count} msgs / ${this._formatBytes(sz.bytes)})`
        );

        const targetFolderId = await this._ensureFolder(targetEmail, folder.displayName);

        // Build dedup index from target folder
        const targetIndex = await this._buildTargetIndex(targetEmail, targetFolderId);
        this.logger.info(`   Target folder has ${targetIndex.size} existing message(s) — will skip duplicates`);

        const folderStats = await this._migrateFolder(
          sourceEmail, folder.id,
          targetEmail, targetFolderId,
          checkpoint, targetIndex,
          sz.count // Pass expected count for progress
        );

        stats.messages_migrated += folderStats.migrated;
        stats.messages_skipped  += folderStats.skipped;
        stats.messages_failed   += folderStats.failed;
        stats.folders_done++;

        checkpoint[folderKey] = 'done';

        this.logger.info(
          `   ✓ ${folder.displayName}: ${folderStats.migrated} migrated, ${folderStats.skipped} skipped, ${folderStats.failed} failed`
        );
      }

      this.logger.success(
        `Email migration complete: ${stats.messages_migrated} migrated, ${stats.messages_skipped} skipped, ${stats.messages_failed} failed`
      );
      return { success: true, stats };

    } catch (err) {
      this.logger.error(`Email migration failed: ${err.message}`);
      return { success: false, error: err.message, stats };
    }
  }

  async _buildTargetIndex(userEmail, folderId) {
    const ids = new Set();
    try {
      for await (const msg of this.tgt.paginate(
        `/users/${userEmail}/mailFolders/${folderId}/messages`,
        { '$select': 'internetMessageId', '$top': 500 }
      )) {
        if (msg.internetMessageId) ids.add(msg.internetMessageId);
      }
    } catch (e) {
      this.logger.warn(`Could not build target index for dedup: ${e.message}`);
    }
    return ids;
  }

  async _getAllFolders(userEmail) {
    const folders = [];
    const topFolders = [];
    for await (const f of this.src.paginate(`/users/${userEmail}/mailFolders`)) {
      topFolders.push(f);
    }
    for (const folder of topFolders) {
      folders.push(folder);
      const children = await this._getChildFolders(userEmail, folder.id);
      folders.push(...children);
    }
    return folders;
  }

  async _getChildFolders(userEmail, parentId) {
    const children = [];
    for await (const f of this.src.paginate(`/users/${userEmail}/mailFolders/${parentId}/childFolders`)) {
      children.push(f);
      const nested = await this._getChildFolders(userEmail, f.id);
      children.push(...nested);
    }
    return children;
  }

  async _ensureFolder(userEmail, folderName) {
    const wellKnownMap = {
      'Inbox': 'inbox', 'Caixa de Entrada': 'inbox',
      'Sent Items': 'sentitems', 'Itens Enviados': 'sentitems',
      'Deleted Items': 'deleteditems', 'Itens Excluídos': 'deleteditems',
      'Drafts': 'drafts', 'Rascunhos': 'drafts',
      'Junk Email': 'junkemail', 'Lixo Eletrônico': 'junkemail',
      'Archive': 'archive', 'Arquivo Morto': 'archive',
      'Outbox': 'outbox'
    };

    if (wellKnownMap[folderName]) {
      try {
        const f = await this.tgt.get(`/users/${userEmail}/mailFolders/${wellKnownMap[folderName]}`);
        return f.id;
      } catch (e) { /* fall through */ }
    }

    try {
      for await (const f of this.tgt.paginate(`/users/${userEmail}/mailFolders`)) {
        if (f.displayName === folderName) return f.id;
      }
    } catch (e) { /* ignore */ }

    try {
      const newFolder = await this.tgt.post(`/users/${userEmail}/mailFolders`, { displayName: folderName });
      return newFolder.id;
    } catch (err) {
      this.logger.warn(`Could not create folder "${folderName}", using inbox: ${err.message}`);
      const inbox = await this.tgt.get(`/users/${userEmail}/mailFolders/inbox`);
      return inbox.id;
    }
  }

  async _migrateFolder(srcEmail, srcFolderId, tgtEmail, tgtFolderId, checkpoint, targetIndex, expectedCount = 0) {
    const stats = { total: 0, migrated: 0, skipped: 0, failed: 0 };
    let skip = 0;
    let processedCount = 0;

    while (true) {
      const result = await this.src.get(
        `/users/${srcEmail}/mailFolders/${srcFolderId}/messages`,
        {
          '$top': this.pageSize,
          '$skip': skip,
          '$select': 'id,internetMessageId,subject,receivedDateTime,sentDateTime,isRead,isDraft,flag,importance,body,from,toRecipients,ccRecipients,bccRecipients,replyTo,internetMessageHeaders'
        }
      );

      const messages = result.value || [];
      if (messages.length === 0) break;
      stats.total += messages.length;

      for (const msg of messages) {
        const msgKey = `email_msg_${msg.id}`;

        // Skip 1: checkpoint
        if (checkpoint[msgKey]) {
          stats.skipped++;
          processedCount++;
          continue;
        }

        // Skip 2: already in target (dedup)
        if (msg.internetMessageId && targetIndex.has(msg.internetMessageId)) {
          this.logger.info(`⏭  Duplicate: "${msg.subject}"`);
          checkpoint[msgKey] = 'done';
          stats.skipped++;
          processedCount++;
          continue;
        }

        if (this.config.dry_run) {
          this.logger.info(`[DRY RUN] Would migrate: ${msg.subject}`);
          stats.migrated++;
          processedCount++;
          continue;
        }

        try {
          await this._createMessage(tgtEmail, tgtFolderId, msg);
          if (msg.internetMessageId) targetIndex.add(msg.internetMessageId);
          checkpoint[msgKey] = 'done';
          stats.migrated++;
          processedCount++;
          
          // Progress indicator every 10 messages or at specific milestones
          if (processedCount % 10 === 0 && expectedCount > 0) {
            const percentage = Math.min(100, Math.round((processedCount / expectedCount) * 100));
            this.logger.info(`   ⏳ Progress: ${processedCount}/${expectedCount} (${percentage}%) | ✓ ${stats.migrated} migrated, ⏭ ${stats.skipped} skipped, ✗ ${stats.failed} failed`);
          }
        } catch (err) {
          this.logger.error(`Failed to migrate message "${msg.subject}": ${err.message}`);
          stats.failed++;
          processedCount++;
        }
      }

      if (messages.length < this.pageSize) break;
      skip += this.pageSize;
    }

    return stats;
  }

  async _createMessage(userEmail, folderId, msg) {
    // Use the original received date, fall back to sent date
    const originalDate = msg.receivedDateTime || msg.sentDateTime;

    const payload = {
      subject: msg.subject || '(sem assunto)',
      body: msg.body || { contentType: 'text', content: '' },
      from: msg.from,
      toRecipients:  msg.toRecipients  || [],
      ccRecipients:  msg.ccRecipients  || [],
      bccRecipients: msg.bccRecipients || [],
      replyTo:       msg.replyTo       || [],
      receivedDateTime: msg.receivedDateTime,
      sentDateTime: msg.sentDateTime,
      isRead:    msg.isRead,
      flag:      msg.flag,
      importance: msg.importance || 'normal',
      // singleValueExtendedProperties pins the displayed date in Outlook
      // PR_MESSAGE_DELIVERY_TIME (0x0E06) = the timestamp shown in the inbox list
      singleValueExtendedProperties: originalDate ? [
        { id: `SystemTime 0x0E06`, value: originalDate },
        { id: `SystemTime 0x0039`, value: msg.sentDateTime || originalDate }
      ] : []
    };

    const created = await this.tgt.post(
      `/users/${userEmail}/mailFolders/${folderId}/messages`,
      payload
    );

    if (!msg.isDraft) {
      await this.tgt.patch(
        `/users/${userEmail}/messages/${created.id}`,
        { isDraft: false }
      );
    }

    return created;
  }

  _formatBytes(bytes) {
    if (!bytes || bytes === 0) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
  }
}

module.exports = EmailMigrator;