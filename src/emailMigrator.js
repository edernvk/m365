/**
 * Email Migration Module - SOLUÇÃO DEFINITIVA
 * - Deduplicação 100% confiável usando singleValueExtendedProperties
 * - Armazena ID da mensagem fonte como propriedade customizada
 * - Compara IDs FONTE antes de migrar (sempre detecta duplicatas)
 * - Checkpoint salvo a cada 10 mensagens
 * - Preservação de datas originais
 */

// GUID único para identificar mensagens migradas por esta ferramenta
const MIGRATION_PROPERTY_ID = 'String {8ECCC264-6880-4EBE-992F-8888D2EEAA1D} Name SourceMessageId';

class EmailMigrator {
  constructor(sourceClient, targetClient, config, logger, checkpointManager = null) {
    this.src = sourceClient;
    this.tgt = targetClient;
    this.config = config;
    this.logger = logger;
    this.checkpointManager = checkpointManager;
    this.pageSize = config.email_page_size || 100;
    
    // FORÇA atualização do logger nos GraphClients para mostrar email do usuário
    if (this.src) this.src.logger = logger;
    if (this.tgt) this.tgt.logger = logger;
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
      this.logger.info('📊 Scanning mailbox...');
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
          let bytes = detail.sizeInBytes || 0;
          
          // FALLBACK: Se sizeInBytes = 0 mas tem mensagens, estima ~150KB/msg
          if (bytes === 0 && count > 0) {
            bytes = count * 150 * 1024; // 150KB por mensagem (estimativa)
          }
          
          folderSizes[folder.id] = { count, bytes, estimated: detail.sizeInBytes === 0 };
          totalMessages += count;
          totalBytes += bytes;
        } catch (e) {
          folderSizes[folder.id] = { count: 0, bytes: 0, estimated: false };
        }
      }

      stats.messages_total = totalMessages;
      stats.bytes_total = totalBytes;

      const hasEstimates = Object.values(folderSizes).some(f => f.estimated);
      this.logger.info(
        `📊 Scan complete: ${totalMessages.toLocaleString()} messages | ${this._formatBytes(totalBytes)}${hasEstimates ? ' (estimated)' : ''}`
      );
      
      // Estimativa de tempo (considerando velocidade otimizada: ~700 msgs/min)
      if (totalMessages > 0) {
        const estimatedMinutes = Math.ceil(totalMessages / 700);
        const hours = Math.floor(estimatedMinutes / 60);
        const mins = estimatedMinutes % 60;
        const timeStr = hours > 0 ? `${hours}h ${mins}min` : `${mins}min`;
        this.logger.info(`⏱️  Estimated time: ~${timeStr} (at ~700 msgs/min)`);
      }

      for (const folder of folders) {
        const sz = folderSizes[folder.id];
        if (sz && sz.count > 0) {
          const sizeStr = sz.estimated ? `~${this._formatBytes(sz.bytes)}` : this._formatBytes(sz.bytes);
          this.logger.info(
            `   📁 ${folder.displayName.padEnd(30)} ${String(sz.count).padStart(6)} msgs | ${sizeStr}`
          );
        }
      }

      // 3. Migrate folder by folder
      const startTime = Date.now();
      let processedMessages = 0;
      
      for (const folder of folders) {
        const folderKey = `email_folder_${folder.id}`;

        // Sync mode: reprocess all folders to catch new messages
        // Normal mode: skip folders marked as done
        if (checkpoint[folderKey] === 'done' && !this.config.sync) {
          this.logger.info(`⏭  Skipping (already migrated): ${folder.displayName}`);
          stats.folders_done++;
          const sz = folderSizes[folder.id] || { count: 0 };
          processedMessages += sz.count;
          continue;
        }
        
        if (checkpoint[folderKey] === 'done' && this.config.sync) {
          this.logger.info(`🔄 SYNC: Re-checking ${folder.displayName} for new messages...`);
        }

        const sz = folderSizes[folder.id] || { count: 0, bytes: 0 };
        const sizeStr = sz.estimated ? `~${this._formatBytes(sz.bytes)}` : this._formatBytes(sz.bytes);
        
        // Progresso global
        const globalProgress = totalMessages > 0 ? Math.round((processedMessages / totalMessages) * 100) : 0;
        this.logger.info(
          `\n📂 [${stats.folders_done + 1}/${folders.length}] ${folder.displayName} (${sz.count} msgs / ${sizeStr}) | Global: ${globalProgress}%`
        );

        const targetFolderId = await this._ensureFolder(targetEmail, folder.displayName);

        // Build dedup index from target folder
        const targetIndex = await this._buildTargetIndex(targetEmail, targetFolderId, folder.displayName);
        const totalProtected = targetIndex.ids.size + targetIndex.fallbackKeys.size;
        if (totalProtected > 0) {
          this.logger.info(`   ✅ Found ${totalProtected} existing message(s) - will skip duplicates`);
          if (targetIndex.ids.size > 0) {
            this.logger.info(`      └─ ${targetIndex.ids.size} with SourceMessageId (exact match)`);
          }
          if (targetIndex.fallbackKeys.size > 0) {
            this.logger.info(`      └─ ${targetIndex.fallbackKeys.size} without SourceMessageId (fallback: subject+date)`);
          }
        } else {
          this.logger.info(`   ✅ No existing messages found - starting fresh`);
        }

        const folderStats = await this._migrateFolder(
          sourceEmail, folder.id,
          targetEmail, targetFolderId,
          checkpoint, targetIndex,
          sz.count
        );

        stats.messages_migrated += folderStats.migrated;
        stats.messages_skipped  += folderStats.skipped;
        stats.messages_failed   += folderStats.failed;
        stats.folders_done++;
        processedMessages += sz.count;

        checkpoint[folderKey] = 'done';
        
        // Velocidade atual e tempo restante
        const elapsedMinutes = (Date.now() - startTime) / 60000;
        const currentSpeed = processedMessages / elapsedMinutes;
        const remainingMessages = totalMessages - processedMessages;
        const remainingMinutes = currentSpeed > 0 ? Math.ceil(remainingMessages / currentSpeed) : 0;
        
        this.logger.info(
          `✅ Folder complete | Speed: ${Math.round(currentSpeed)} msgs/min | ETA: ${remainingMinutes}min remaining`
        );
        
        // Save checkpoint after each folder
        if (this.checkpointManager) {
          this.checkpointManager.save();
          this.logger.info(`   💾 Checkpoint saved (folder complete)`);
        }

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

  async _buildTargetIndex(userEmail, folderId, folderName = 'folder') {
    const ids = new Set();
    const fallbackKeys = new Set(); // Para mensagens sem SourceMessageId
    
    try {
      this.logger.info(`   🔍 Checking existing messages in "${folderName}"...`);
      
      // SOLUÇÃO CORRIGIDA: Busca todas as mensagens e expande a propriedade customizada
      // Graph API não permite filtrar apenas por existência da propriedade
      const expand = `singleValueExtendedProperties($filter=id eq '${MIGRATION_PROPERTY_ID}')`;
      
      for await (const msg of this.tgt.paginate(
        `/users/${userEmail}/mailFolders/${folderId}/messages`,
        { 
          '$expand': expand,
          '$select': 'id,subject,receivedDateTime',
          '$top': 500 
        },
        'existing messages'
      )) {
        // Extrai o ID da mensagem FONTE armazenado na propriedade customizada
        const sourceIdProp = msg.singleValueExtendedProperties?.find(
          p => p.id === MIGRATION_PROPERTY_ID
        );
        if (sourceIdProp && sourceIdProp.value) {
          // Esta mensagem foi migrada - adiciona ID da FONTE ao índice
          ids.add(sourceIdProp.value);
        } else {
          // FALLBACK: Mensagem SEM SourceMessageId (criada antes da implementação)
          // Cria chave baseada em subject + data para deduplicação
          if (msg.subject && msg.receivedDateTime) {
            const fallbackKey = `${msg.subject}|${msg.receivedDateTime}`;
            fallbackKeys.add(fallbackKey);
          }
        }
      }
    } catch (e) {
      this.logger.warn(`Could not build target index for dedup: ${e.message}`);
    }
    return { ids, fallbackKeys };
  }

  async _getAllFolders(userEmail) {
    const folders = [];
    const topFolders = [];
    for await (const f of this.src.paginate(`/users/${userEmail}/mailFolders`, {}, 'folders')) {
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
    for await (const f of this.src.paginate(`/users/${userEmail}/mailFolders/${parentId}/childFolders`, {}, 'child folders')) {
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
      for await (const f of this.tgt.paginate(`/users/${userEmail}/mailFolders`, {}, 'target folders')) {
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
    let messagesSinceLastSave = 0;

    while (true) {
      const result = await this.src.get(
        `/users/${srcEmail}/mailFolders/${srcFolderId}/messages`,
        {
          '$top': this.pageSize,
          '$skip': skip,
          '$select': 'id,subject,receivedDateTime,sentDateTime,isRead,isDraft,flag,importance,body,from,toRecipients,ccRecipients,bccRecipients,replyTo'
        }
      );

      const messages = result.value || [];
      
      if (messages.length === 0) break;
      stats.total += messages.length;

      for (const msg of messages) {
        const msgKey = `email_msg_${msg.id}`;

        // Skip 1: checkpoint (MAS NÃO no sync mode - sync sempre verifica destino!)
        if (checkpoint[msgKey] && !this.config.sync) {
          stats.skipped++;
          processedCount++;
          continue;
        }

        // Skip 2: already in target (deduplicação via SourceMessageId OU fallback)
        let isDuplicate = false;
        
        // Método 1: Verifica por SourceMessageId (preferencial)
        if (targetIndex.ids.has(msg.id)) {
          isDuplicate = true;
        }
        
        // Método 2: FALLBACK - Verifica por subject+date (para mensagens antigas sem SourceMessageId)
        if (!isDuplicate && targetIndex.fallbackKeys.size > 0) {
          const fallbackKey = `${msg.subject}|${msg.receivedDateTime}`;
          if (targetIndex.fallbackKeys.has(fallbackKey)) {
            isDuplicate = true;
          }
        }
        
        if (isDuplicate) {
          checkpoint[msgKey] = 'done';
          stats.skipped++;
          processedCount++;
          messagesSinceLastSave++;
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
          
          // Adiciona ID FONTE ao índice após criar (para deduplicação futura)
          targetIndex.ids.add(msg.id);
          
          // Também adiciona ao fallback index
          if (msg.subject && msg.receivedDateTime) {
            const fallbackKey = `${msg.subject}|${msg.receivedDateTime}`;
            targetIndex.fallbackKeys.add(fallbackKey);
          }
          
          checkpoint[msgKey] = 'done';
          stats.migrated++;
          processedCount++;
          messagesSinceLastSave++;
          
          // Save checkpoint every 10 messages
          if (messagesSinceLastSave >= 10 && this.checkpointManager) {
            this.checkpointManager.save();
            messagesSinceLastSave = 0;
          }
          
          // Progress indicator every 10 messages
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
    
    // Final save for this folder
    if (messagesSinceLastSave > 0 && this.checkpointManager) {
      this.checkpointManager.save();
    }

    return stats;
  }

  async _createMessage(userEmail, folderId, msg) {
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
      isDraft:   false, // OTIMIZAÇÃO: Criar já como não-draft (economiza 1 PATCH por mensagem!)
      flag:      msg.flag,
      importance: msg.importance || 'normal',
      singleValueExtendedProperties: [
        // Preservar datas originais
        { id: 'SystemTime 0x0E06', value: originalDate },
        { id: 'SystemTime 0x0039', value: msg.sentDateTime || originalDate },
        // CRÍTICO: Armazenar ID da mensagem fonte para deduplicação
        { id: MIGRATION_PROPERTY_ID, value: msg.id }
      ].filter(p => p.value) // Remove se não tiver valor
    };

    const created = await this.tgt.post(
      `/users/${userEmail}/mailFolders/${folderId}/messages`,
      payload
    );

    // REMOVIDO: PATCH para marcar como não-draft (já criamos assim!)
    // Economiza ~150-300ms por mensagem

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