/**
 * Email Migration Module v3
 *
 * v3 changes vs v2:
 * - Attachments fully supported (was missing in v2 fetch $select)
 * - Attachments fetched SEPARATELY per message (not inline in list) — avoids
 *   huge payloads on folder scans; Graph $expand+contentBytes on 100 msgs = crash
 * - Small attachments (≤ 3MB): included inline in POST payload
 * - Large attachments (> 3MB): uploaded via createUploadSession (4MB chunks)
 * - Quota retry (same pattern as fixDrafts): 60s pause + 3 retries per batch
 * - Attachment fetch concurrency: 3 parallel (same as fixDrafts phase 1)
 * - Delays between batches: 1500ms + 5s every 5 batches (same as fixDrafts)
 */

const axios = require('axios');
let pLimit;
try { pLimit = require('p-limit'); } catch(e) { pLimit = null; }

const GRAPH_BASE          = 'https://graph.microsoft.com/v1.0';
const GRAPH_BATCH_URL     = `${GRAPH_BASE}/$batch`;
const MIGRATION_PROPERTY_ID = 'String {8ECCC264-6880-4EBE-992F-8888D2EEAA1D} Name SourceMessageId';
const BATCH_SIZE          = 20;    // Graph API max per batch call
const INLINE_LIMIT        = 3 * 1024 * 1024; // 3MB — above this = upload session
const BATCH_DELAY_MS      = 1500;  // delay between create batches (quota protection)
const BATCH_PAUSE_EVERY   = 5;     // pause longer every N batches
const BATCH_PAUSE_MS      = 5000;  // longer pause duration
const QUOTA_RETRY_WAIT_MS = 60000; // 60s pause when quota hit
const MAX_BATCH_RETRIES   = 3;     // max retries per batch on quota errors
const ATT_CONCURRENCY     = 3;     // parallel attachment fetches per message

class EmailMigrator {
  constructor(sourceClient, targetClient, config, logger, checkpointManager = null) {
    this.src = sourceClient;
    this.tgt = targetClient;
    this.config = config;
    this.logger = logger;
    this.checkpointManager = checkpointManager;
    this.pageSize = config.email_page_size || 100;
    if (this.src) this.src.logger = logger;
    if (this.tgt) this.tgt.logger = logger;
    this._tgtFolderCache = null;
  }

  _sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

  // ── JSON Batch: envia até 20 requests em 1 chamada HTTP ───────────────────
  async _sendBatch(authInstance, requests) {
    const token = await authInstance.getToken();
    const response = await axios.post(
      GRAPH_BATCH_URL,
      { requests },
      {
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        validateStatus: null
      }
    );
    if (response.status === 429) {
      const wait = parseInt(response.headers['retry-after'] || '10') * 1000;
      this.logger.warn(`   ⏸️  Batch rate limited, waiting ${wait/1000}s...`);
      await this._sleep(wait);
      return this._sendBatch(authInstance, requests);
    }
    const results = {};
    for (const r of (response.data?.responses || [])) results[r.id] = r;
    return results;
  }

  _chunk(arr, size) {
    const chunks = [];
    for (let i = 0; i < arr.length; i += size) chunks.push(arr.slice(i, i + size));
    return chunks;
  }

  // ── Fetch attachments for a single message ────────────────────────────────
  // Done separately (not inline in list) to avoid huge list payloads
  async _fetchAttachments(userEmail, messageId) {
    try {
      // Step 1: List without contentBytes (not valid in $select on collection)
      const list = await this.src.get(
        `/users/${userEmail}/messages/${messageId}/attachments`,
        { '$select': 'id,name,contentType,size,isInline,lastModifiedDateTime' }
      );
      const meta = list.value || [];
      if (meta.length === 0) return [];

      // Step 2: Fetch each attachment individually to get contentBytes
      const attachments = await Promise.all(
        meta.map(async att => {
          try {
            return await this.src.get(`/users/${userEmail}/messages/${messageId}/attachments/${att.id}`);
          } catch (e) {
            this.logger.warn(`   ⚠️  Could not fetch content for "${att.name}": ${e.message}`);
            return att;
          }
        })
      );
      return attachments;
    } catch (e) {
      this.logger.warn(`   ⚠️  Could not fetch attachments for ${messageId}: ${e.message}`);
      return [];
    }
  }

  // ── Enrich messages with attachments (concurrency 3) ─────────────────────
  async _enrichWithAttachments(messages, userEmail) {
    const needsAttachments = messages.filter(m => m.hasAttachments);
    if (needsAttachments.length === 0) return messages;

    const limit = pLimit ? pLimit(ATT_CONCURRENCY) : null;
    let fetched = 0;

    const fetchFn = async (msg) => {
      msg.attachments = await this._fetchAttachments(userEmail, msg.id);
      fetched++;
      if (fetched % 20 === 0 || fetched === needsAttachments.length) {
        this.logger.info(`   📎 Attachments fetched: ${fetched}/${needsAttachments.length}`);
      }
      return msg;
    };

    if (limit) {
      await Promise.all(needsAttachments.map(msg => limit(() => fetchFn(msg))));
    } else {
      // fallback: sequential if p-limit not available
      for (const msg of needsAttachments) await fetchFn(msg);
    }

    return messages;
  }

  // ── Upload large attachment (> 3MB) via upload session ────────────────────
  async _uploadLargeAttachment(tgtEmail, messageId, attachment) {
    const token = await this.tgt.auth.getToken();

    // Create upload session
    const sessionResp = await axios.post(
      `${GRAPH_BASE}/users/${tgtEmail}/messages/${messageId}/attachments/createUploadSession`,
      {
        AttachmentItem: {
          attachmentType: 'file',
          name:     attachment.name,
          size:     attachment.size,
          isInline: attachment.isInline || false
        }
      },
      {
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        validateStatus: null
      }
    );

    if (sessionResp.status !== 201) {
      throw new Error(`Upload session failed: HTTP ${sessionResp.status}`);
    }

    const uploadUrl  = sessionResp.data.uploadUrl;
    const fileBytes  = Buffer.from(attachment.contentBytes, 'base64');
    const chunkSize  = 4 * 1024 * 1024; // 4MB chunks
    let offset = 0;

    while (offset < fileBytes.length) {
      const end        = Math.min(offset + chunkSize, fileBytes.length);
      const chunkBytes = fileBytes.slice(offset, end);

      const resp = await axios.put(uploadUrl, chunkBytes, {
        headers: {
          'Content-Type':   'application/octet-stream',
          'Content-Range':  `bytes ${offset}-${end - 1}/${fileBytes.length}`,
          'Content-Length': chunkBytes.length
        },
        validateStatus: null,
        maxBodyLength: Infinity
      });

      if (resp.status !== 200 && resp.status !== 201 && resp.status !== 202) {
        throw new Error(`Chunk upload failed at offset ${offset}: HTTP ${resp.status}`);
      }

      offset = end;
    }
  }

  // ── Build message payload — small attachments inline, large flagged ───────
  _buildPayload(msg) {
    const originalDate = msg.receivedDateTime || msg.sentDateTime;

    const payload = {
      subject:       msg.subject || '(sem assunto)',
      body:          msg.body || { contentType: 'text', content: '' },
      from:          msg.from,
      toRecipients:  msg.toRecipients  || [],
      ccRecipients:  msg.ccRecipients  || [],
      bccRecipients: msg.bccRecipients || [],
      replyTo:       msg.replyTo       || [],
      receivedDateTime: msg.receivedDateTime,
      sentDateTime:     msg.sentDateTime,
      isRead:     msg.isRead,
      flag:       msg.flag,
      importance: msg.importance || 'normal',
      categories: msg.categories?.length ? msg.categories : undefined,
      // internetMessageHeaders: preserve up to 5 x- headers (Graph API limit)
      // Also filter values > 995 chars (Graph API rejects longer values)
      ...(msg.internetMessageHeaders?.length ? {
        internetMessageHeaders: msg.internetMessageHeaders
          .filter(h => h.name?.toLowerCase().startsWith('x-'))
          .filter(h => (h.value || '').length <= 995)
          .slice(0, 5)
      } : {}),
      singleValueExtendedProperties: [
        originalDate     && { id: 'SystemTime 0x0E06', value: originalDate },
        msg.sentDateTime && { id: 'SystemTime 0x0039', value: msg.sentDateTime },
        { id: 'String 0x001A',  value: 'IPM.Note' }, // message class
        { id: 'Integer 0x0E07', value: '5' },         // Read(1)+Submit(4) = non-draft
        { id: 'Integer 0x0E17', value: '1' },         // PR_MESSAGE_STATE: not draft
        { id: MIGRATION_PROPERTY_ID, value: msg.id }  // source ID for dedup
      ].filter(Boolean)
    };

    // Inline attachments ≤ 3MB — larger ones go via _uploadLargeAttachment
    if (msg.attachments?.length > 0) {
      const small = msg.attachments.filter(a => a.contentBytes && a.size <= INLINE_LIMIT);
      if (small.length > 0) {
        payload.attachments = small.map(a => ({
          '@odata.type': '#microsoft.graph.fileAttachment',
          name:         a.name,
          contentType:  a.contentType,
          contentBytes: a.contentBytes,
          isInline:     a.isInline  || false,
          contentId:    a.contentId || null
        }));
      }
    }

    return payload;
  }

  async migrate(sourceEmail, targetEmail, checkpoint = {}) {
    this.logger.info(`Starting email migration: ${sourceEmail} → ${targetEmail}`);

    const stats = {
      folders_total: 0, folders_done: 0,
      messages_total: 0, messages_migrated: 0,
      messages_skipped: 0, messages_failed: 0,
      attachments_inline: 0, attachments_large: 0, attachments_failed: 0,
      bytes_total: 0
    };

    try {
      const folders = await this._getAllFolders(sourceEmail);
      stats.folders_total = folders.length;
      this.logger.info(`Found ${folders.length} folders`);

      this._tgtFolderCache = await this._loadTargetFolders(targetEmail);
      this.logger.info(`Target has ${Object.keys(this._tgtFolderCache).length} folders`);

      // Scan mailbox sizes
      this.logger.info('📊 Scanning mailbox...');
      let totalMessages = 0, totalBytes = 0;
      const folderSizes = {};

      for (const folder of folders) {
        try {
          const detail = await this.src.get(
            `/users/${sourceEmail}/mailFolders/${folder.id}`,
            { '$select': 'id,displayName,totalItemCount,sizeInBytes' }
          );
          const count = detail.totalItemCount || 0;
          let bytes = detail.sizeInBytes || 0;
          if (bytes === 0 && count > 0) bytes = count * 150 * 1024;
          folderSizes[folder.id] = { count, bytes, estimated: detail.sizeInBytes === 0 };
          totalMessages += count;
          totalBytes    += bytes;
        } catch (e) {
          folderSizes[folder.id] = { count: 0, bytes: 0, estimated: false };
        }
      }

      stats.messages_total = totalMessages;
      stats.bytes_total    = totalBytes;

      const hasEst = Object.values(folderSizes).some(f => f.estimated);
      this.logger.info(`📊 Scan: ${totalMessages.toLocaleString()} msgs | ${this._formatBytes(totalBytes)}${hasEst ? ' (est)' : ''}`);

      if (totalMessages > 0) {
        const mins = Math.ceil(totalMessages / 500); // conservative with attachment fetches
        const h = Math.floor(mins / 60), m = mins % 60;
        this.logger.info(`⏱️  ETA: ~${h > 0 ? h + 'h ' : ''}${m}min`);
      }

      for (const folder of folders) {
        const sz = folderSizes[folder.id];
        if (sz?.count > 0) {
          const s = sz.estimated ? `~${this._formatBytes(sz.bytes)}` : this._formatBytes(sz.bytes);
          this.logger.info(`   📁 ${folder.displayName.padEnd(30)} ${String(sz.count).padStart(6)} msgs | ${s}`);
        }
      }

      // Migrate folder by folder
      const startTime = Date.now();
      let processedMessages = 0;

      for (const folder of folders) {
        const folderKey = `email_folder_${folder.id}`;

        if (checkpoint[folderKey] === 'done' && !this.config.sync) {
          this.logger.info(`⏭  Skipping: ${folder.displayName}`);
          stats.folders_done++;
          processedMessages += folderSizes[folder.id]?.count || 0;
          continue;
        }

        if (checkpoint[folderKey] === 'done' && this.config.sync) {
          this.logger.info(`🔄 SYNC: Re-checking ${folder.displayName}...`);
        }

        const sz        = folderSizes[folder.id] || { count: 0, bytes: 0 };
        const globalPct = totalMessages > 0 ? Math.round((processedMessages / totalMessages) * 100) : 0;
        this.logger.info(
          `\n📂 [${stats.folders_done + 1}/${folders.length}] ${folder.displayName} (${sz.count} msgs / ${this._formatBytes(sz.bytes)}) | Global: ${globalPct}%`
        );

        const targetFolderId = await this._ensureFolder(targetEmail, folder.displayName);
        const targetIndex    = await this._buildTargetIndex(targetEmail, targetFolderId, folder.displayName);

        const totalProtected = targetIndex.ids.size + targetIndex.fallbackKeys.size;
        this.logger.info(`   ✅ ${totalProtected} existing msgs in target (dedup protection)`);

        const folderStats = await this._migrateFolder(
          sourceEmail, folder.id, targetEmail, targetFolderId,
          checkpoint, targetIndex, sz.count
        );

        stats.messages_migrated    += folderStats.migrated;
        stats.messages_skipped     += folderStats.skipped;
        stats.messages_failed      += folderStats.failed;
        stats.attachments_inline   += folderStats.attachments_inline   || 0;
        stats.attachments_large    += folderStats.attachments_large    || 0;
        stats.attachments_failed   += folderStats.attachments_failed   || 0;
        stats.folders_done++;
        processedMessages += sz.count;
        checkpoint[folderKey] = 'done';

        const elapsed   = (Date.now() - startTime) / 60000;
        const speed     = elapsed > 0 ? Math.round(processedMessages / elapsed) : 0;
        const remaining = speed > 0 ? Math.ceil((totalMessages - processedMessages) / speed) : 0;
        this.logger.info(
          `✅ ${folder.displayName}: ${folderStats.migrated} migrated | ` +
          `📎 ${folderStats.attachments_inline||0} inline + ${folderStats.attachments_large||0} large | ` +
          `Speed: ${speed} msgs/min | ETA: ${remaining}min`
        );

        if (this.checkpointManager) {
          this.checkpointManager.save();
          this.logger.info(`   💾 Checkpoint saved`);
        }
      }

      this.logger.success(
        `Email migration complete: ${stats.messages_migrated} migrated, ` +
        `${stats.messages_skipped} skipped, ${stats.messages_failed} failed | ` +
        `Attachments: ${stats.attachments_inline} inline + ${stats.attachments_large} large + ${stats.attachments_failed} failed`
      );
      return { success: true, stats };

    } catch (err) {
      this.logger.error(`Email migration failed: ${err.message}`);
      return { success: false, error: err.message, stats };
    }
  }

  // ── Migrate messages in a folder ──────────────────────────────────────────
  async _migrateFolder(srcEmail, srcFolderId, tgtEmail, tgtFolderId, checkpoint, targetIndex, expectedCount = 0) {
    const stats = {
      total: 0, migrated: 0, skipped: 0, failed: 0,
      attachments_inline: 0, attachments_large: 0, attachments_failed: 0
    };
    let skip = 0;
    let processedCount = 0;
    let messagesSinceLastSave = 0;

    while (true) {
      // Fetch messages WITHOUT inline attachments — avoids huge payloads
      const result = await this.src.get(
        `/users/${srcEmail}/mailFolders/${srcFolderId}/messages`,
        {
          '$top':    this.pageSize,
          '$skip':   skip,
          '$select': 'id,subject,receivedDateTime,sentDateTime,isRead,isDraft,flag,importance,body,from,toRecipients,ccRecipients,bccRecipients,replyTo,hasAttachments,categories,conversationId,internetMessageHeaders'
        }
      );

      const messages = result.value || [];
      if (messages.length === 0) break;
      stats.total += messages.length;

      // Filter: skip already done or duplicates
      const toMigrate = [];
      for (const msg of messages) {
        const msgKey = `email_msg_${msg.id}`;
        if (checkpoint[msgKey] && !this.config.sync) {
          stats.skipped++; processedCount++; continue;
        }
        let isDuplicate = targetIndex.ids.has(msg.id);
        if (!isDuplicate && targetIndex.fallbackKeys.size > 0) {
          isDuplicate = targetIndex.fallbackKeys.has(`${msg.subject}|${msg.receivedDateTime}`);
        }
        if (isDuplicate) {
          checkpoint[msgKey] = 'done';
          stats.skipped++; processedCount++; messagesSinceLastSave++;
          continue;
        }
        if (this.config.dry_run) {
          this.logger.info(`[DRY RUN] Would migrate: ${msg.subject}`);
          stats.migrated++; processedCount++; continue;
        }
        toMigrate.push(msg);
      }

      if (toMigrate.length > 0) {
        // Fetch attachments for messages that have them (concurrency 3)
        const withAtts = toMigrate.filter(m => m.hasAttachments);
        if (withAtts.length > 0) {
          this.logger.info(`   📎 Fetching attachments for ${withAtts.length} messages...`);
          await this._enrichWithAttachments(toMigrate, srcEmail);
        }

        // Batch CREATE with quota retry (same pattern as fixDrafts)
        const chunks = this._chunk(toMigrate, BATCH_SIZE);

        for (let i = 0; i < chunks.length; i++) {
          const chunk = chunks[i];

          const buildRequests = () => chunk.map((msg, idx) => ({
            id:     String(idx), // short numeric ID — avoids Graph batch ID issues
            method: 'POST',
            url:    `/users/${tgtEmail}/mailFolders/${tgtFolderId}/messages`,
            headers: { 'Content-Type': 'application/json' },
            body:   this._buildPayload(msg)
          }));

          // Send with quota retry
          let batchResults = {};
          for (let attempt = 0; attempt <= MAX_BATCH_RETRIES; attempt++) {
            batchResults = await this._sendBatch(this.tgt.auth, buildRequests());

            const hasQuota = chunk.some((msg, idx) => {
              const r   = batchResults[String(idx)];
              const err = r?.body?.error?.message || '';
              return !r || err.includes('Request limit') || err.includes('MailboxConcurrency');
            });

            if (!hasQuota) break;

            if (attempt < MAX_BATCH_RETRIES) {
              this.logger.warn(`   ⏸️  Quota hit on batch ${i+1} — pausing 60s and retrying (attempt ${attempt+1}/${MAX_BATCH_RETRIES})...`);
              await this._sleep(QUOTA_RETRY_WAIT_MS);
            }
          }

          // Process results
          for (let idx = 0; idx < chunk.length; idx++) {
            const msg    = chunk[idx];
            const r      = batchResults[String(idx)];
            const msgKey = `email_msg_${msg.id}`;

            if (r?.status === 201) {
              const newMessageId = r.body?.id;
              targetIndex.ids.add(msg.id);
              if (msg.subject && msg.receivedDateTime) {
                targetIndex.fallbackKeys.add(`${msg.subject}|${msg.receivedDateTime}`);
              }
              checkpoint[msgKey] = 'done';
              stats.migrated++; processedCount++; messagesSinceLastSave++;

              // Count inline attachments
              if (msg.attachments?.length > 0) {
                stats.attachments_inline += msg.attachments.filter(a => a.contentBytes && a.size <= INLINE_LIMIT).length;
              }

              // Upload large attachments via upload session
              if (newMessageId && msg.attachments?.length > 0) {
                const largeAtts = msg.attachments.filter(a => a.contentBytes && a.size > INLINE_LIMIT);
                for (const att of largeAtts) {
                  try {
                    await this._uploadLargeAttachment(tgtEmail, newMessageId, att);
                    this.logger.info(`   📎 Uploaded large attachment "${att.name}" (${(att.size/1024/1024).toFixed(1)}MB)`);
                    stats.attachments_large++;
                  } catch (err) {
                    this.logger.error(`   ✗ Large attachment failed "${att.name}": ${err.message}`);
                    stats.attachments_failed++;
                  }
                }
              }

            } else {
              const errMsg = r?.body?.error?.message || (r ? `status ${r.status}` : 'no response');
              if (!r || errMsg.includes('Request limit') || errMsg.includes('MailboxConcurrency')) {
                this.logger.warn(`   ⚠️  Quota persists for "${msg.subject}" after ${MAX_BATCH_RETRIES} retries — next run will fix`);
              } else {
                this.logger.error(`   ✗ Failed to migrate "${msg.subject}": ${errMsg}`);
              }
              stats.failed++; processedCount++;
            }
          }

          const progress = Math.min((i + 1) * BATCH_SIZE, toMigrate.length);
          this.logger.info(`   ✉️  [${progress}/${toMigrate.length}] ${Math.round(progress/toMigrate.length*100)}% — batch ${i+1}/${chunks.length}`);

          if (messagesSinceLastSave >= 10 && this.checkpointManager) {
            this.checkpointManager.save();
            messagesSinceLastSave = 0;
          }

          // Pause between batches (quota protection — same as fixDrafts)
          if (i < chunks.length - 1) {
            if ((i + 1) % BATCH_PAUSE_EVERY === 0) {
              this.logger.info(`   ⏸️  Pausing ${BATCH_PAUSE_MS/1000}s after ${(i+1) * BATCH_SIZE} messages...`);
              await this._sleep(BATCH_PAUSE_MS);
            } else {
              await this._sleep(BATCH_DELAY_MS);
            }
          }
        }
      }

      // Progress
      if (processedCount % 100 === 0 && expectedCount > 0) {
        const pct = Math.min(100, Math.round((processedCount / expectedCount) * 100));
        this.logger.info(`   ⏳ ${processedCount}/${expectedCount} (${pct}%) | ✓${stats.migrated} ⏭${stats.skipped} ✗${stats.failed}`);
      }

      if (messages.length < this.pageSize) break;
      skip += this.pageSize;
    }

    if (messagesSinceLastSave > 0 && this.checkpointManager) {
      this.checkpointManager.save();
    }

    return stats;
  }

  // ── Load ALL target folders once ──────────────────────────────────────────
  async _loadTargetFolders(userEmail) {
    const map = {};
    const WELL_KNOWN = {
      'Inbox': 'inbox', 'Caixa de Entrada': 'inbox',
      'Sent Items': 'sentitems', 'Itens Enviados': 'sentitems',
      'Deleted Items': 'deleteditems', 'Itens Excluídos': 'deleteditems',
      'Drafts': 'drafts', 'Rascunhos': 'drafts',
      'Junk Email': 'junkemail', 'Lixo Eletrônico': 'junkemail',
      'Archive': 'archive', 'Arquivo Morto': 'archive',
      'Outbox': 'outbox'
    };
    for (const [name, wk] of Object.entries(WELL_KNOWN)) {
      try {
        const f = await this.tgt.get(`/users/${userEmail}/mailFolders/${wk}`);
        map[name] = f.id;
        map[f.displayName] = f.id;
      } catch (e) { /* skip */ }
    }
    for await (const f of this.tgt.paginate(`/users/${userEmail}/mailFolders`, { '$expand': 'childFolders', '$top': 100 })) {
      map[f.displayName] = f.id;
      if (f.childFolders?.length) {
        for (const c of f.childFolders) map[c.displayName] = c.id;
      }
    }
    return map;
  }

  // ── Build dedup index ─────────────────────────────────────────────────────
  async _buildTargetIndex(userEmail, folderId, folderName = 'folder') {
    const ids = new Set();
    const fallbackKeys = new Set();
    try {
      this.logger.info(`   🔍 Building dedup index for "${folderName}"...`);
      const expand = `singleValueExtendedProperties($filter=id eq '${MIGRATION_PROPERTY_ID}')`;
      for await (const msg of this.tgt.paginate(
        `/users/${userEmail}/mailFolders/${folderId}/messages`,
        { '$expand': expand, '$select': 'id,subject,receivedDateTime', '$top': 500 }
      )) {
        const prop = msg.singleValueExtendedProperties?.find(p => p.id === MIGRATION_PROPERTY_ID);
        if (prop?.value) {
          ids.add(prop.value);
        } else if (msg.subject && msg.receivedDateTime) {
          fallbackKeys.add(`${msg.subject}|${msg.receivedDateTime}`);
        }
      }
    } catch (e) {
      this.logger.warn(`Could not build target index: ${e.message}`);
    }
    return { ids, fallbackKeys };
  }

  // ── Get all source folders — deduplicated by ID ───────────────────────────
  async _getAllFolders(userEmail) {
    const map = new Map();
    for await (const f of this.src.paginate(
      `/users/${userEmail}/mailFolders`,
      { '$expand': 'childFolders', '$top': 100 }
    )) {
      map.set(f.id, f);
      if (f.childFolders?.length) {
        for (const c of f.childFolders) map.set(c.id, c);
      }
    }
    return [...map.values()];
  }

  // ── Find or create folder in target ──────────────────────────────────────
  async _ensureFolder(userEmail, folderName) {
    if (this._tgtFolderCache?.[folderName]) return this._tgtFolderCache[folderName];
    try {
      const newFolder = await this.tgt.post(`/users/${userEmail}/mailFolders`, { displayName: folderName });
      if (this._tgtFolderCache) this._tgtFolderCache[folderName] = newFolder.id;
      return newFolder.id;
    } catch (err) {
      this.logger.warn(`Could not create folder "${folderName}", using inbox: ${err.message}`);
      return this._tgtFolderCache?.['Inbox'] || this._tgtFolderCache?.['Caixa de Entrada'];
    }
  }

  _formatBytes(bytes) {
    if (!bytes || bytes === 0) return '0 B';
    const u = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${u[i]}`;
  }
}

module.exports = EmailMigrator;
