/**
 * fixAttachments.js — v4 (corrected root cause)
 *
 * ROOT CAUSE of previous failures:
 *   hasAttachments is a CALCULATED field in Exchange/Graph API.
 *   It is derived from the MAPI AllAttachmentsHidden property — NOT from
 *   what you pass in the POST payload. When we migrated messages without
 *   attachments, the Exchange server correctly set hasAttachments=false on
 *   the destination, regardless of what was in the source.
 *
 *   Previous attempts filtered TARGET by hasAttachments=true or hasAttachments=false
 *   and both failed:
 *   - hasAttachments=false: correct filter, but messages without SourceMessageId
 *     were silently skipped; also, Exchange may mark some as false even with attachments
 *   - hasAttachments=true: wrong — these already HAVE attachments or are fine
 *
 * CORRECT APPROACH (this version):
 *   1. Iterate SOURCE folders — source is ground truth
 *   2. For each source message with hasAttachments=true, look up the
 *      corresponding TARGET message via SourceMessageId property
 *   3. Fetch ACTUAL attachments from TARGET (not the hasAttachments flag)
 *   4. If target message has 0 actual attachments → fix it
 *   5. For target messages without SourceMessageId → fallback match by
 *      internetMessageId (unique per email, preserved in header), then
 *      subject+receivedDateTime
 *
 * This mirrors fixDrafts.js: batch ops, quota retry, 60s between users, 5s between folders.
 */

'use strict';

const fs          = require('fs');
const path        = require('path');
const axios       = require('axios');
const chalk       = require('chalk');
const pLimit      = require('p-limit');
const Logger      = require('./logger');
const TenantAuth  = require('./auth');
const GraphClient = require('./graphClient');
const UserLoader  = require('./userLoader');

// ── Constants ─────────────────────────────────────────────────────────────────
const GRAPH_BASE           = 'https://graph.microsoft.com/v1.0';
const GRAPH_BATCH_URL      = `${GRAPH_BASE}/$batch`;
const MIGRATION_PROPERTY_ID = 'String {8ECCC264-6880-4EBE-992F-8888D2EEAA1D} Name SourceMessageId';

const BATCH_SIZE              = 20;
const PAGE_SIZE               = 100;
const ATTACHMENT_INLINE_LIMIT = 3 * 1024 * 1024; // 3MB
const CONCURRENCY             = 4;

const BATCH_DELAY_MS      = 1500;
const BATCH_PAUSE_EVERY   = 5;
const PHASE_DELAY_MS      = 2000;
const FOLDER_DELAY_MS     = 5000;
const USER_DELAY_MS       = 60000;
const QUOTA_RETRY_WAIT_MS = 60000;
const MAX_BATCH_RETRIES   = 3;

const DRY_RUN = process.argv.includes('--dry-run');

// ── Config ────────────────────────────────────────────────────────────────────
let config;
try {
  config = JSON.parse(fs.readFileSync(path.resolve(process.cwd(), 'config.json'), 'utf8'));
} catch (e) {
  console.error(chalk.red('❌ config.json not found'));
  process.exit(1);
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── Build payload (same MAPI flags as emailMigrator/fixDrafts) ────────────────
function buildPayload(msg, sourceId) {
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
    ...(msg.internetMessageHeaders?.length ? (() => {
      // Deduplicate by name (case-insensitive), keep first occurrence
      // Filter: only x- prefix, max 995 chars value, max 5 headers
      const seen = new Set();
      const headers = msg.internetMessageHeaders
        .filter(h => h.name?.toLowerCase().startsWith('x-'))
        .filter(h => (h.value || '').length <= 995)
        .filter(h => {
          const key = h.name.toLowerCase();
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        })
        .slice(0, 5);
      return headers.length ? { internetMessageHeaders: headers } : {};
    })() : {}),
    singleValueExtendedProperties: [
      originalDate     && { id: 'SystemTime 0x0E06', value: originalDate },
      msg.sentDateTime && { id: 'SystemTime 0x0039', value: msg.sentDateTime },
      { id: 'String 0x001A',  value: 'IPM.Note' },
      { id: 'Integer 0x0E07', value: '5' },
      { id: 'Integer 0x0E17', value: '1' },
      sourceId && { id: MIGRATION_PROPERTY_ID, value: sourceId }
    ].filter(Boolean)
  };

  Object.keys(payload).forEach(k => payload[k] === undefined && delete payload[k]);
  return payload;
}

// ── JSON Batch ────────────────────────────────────────────────────────────────
async function sendBatch(authInstance, requests) {
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
    await sleep(wait);
    return sendBatch(authInstance, requests);
  }
  const results = {};
  for (const r of (response.data?.responses || [])) {
    results[r.id] = { status: r.status, body: r.body };
  }
  return results;
}

function chunk(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) chunks.push(arr.slice(i, i + size));
  return chunks;
}

// ── Fetch all pages of messages from a folder ─────────────────────────────────
async function fetchAllMessages(client, userEmail, folderId, selectFields, expandStr = null) {
  const messages = [];
  let skip = 0;
  while (true) {
    const params = {
      '$top':    PAGE_SIZE,
      '$skip':   skip,
      '$select': selectFields
    };
    if (expandStr) params['$expand'] = expandStr;

    const result = await client.get(
      `/users/${userEmail}/mailFolders/${folderId}/messages`,
      params
    );
    const msgs = result.value || [];
    messages.push(...msgs);
    if (msgs.length < PAGE_SIZE) break;
    skip += PAGE_SIZE;
  }
  return messages;
}

// ── Build a lookup map for target folder messages ─────────────────────────────
// Returns two maps: bySourceId (SourceMessageId → tgtMsg) and
//                   byInternetMsgId (internetMessageId → tgtMsg)
async function buildTargetLookup(tgtClient, tgtEmail, tgtFolderId, logger) {
  logger.info(`   🗂️  Building target lookup index...`);

  const expand = `singleValueExtendedProperties($filter=id eq '${MIGRATION_PROPERTY_ID}')`;
  const bySourceId      = new Map(); // sourceMessageId → target message
  const byInternetMsgId = new Map(); // internetMessageId → target message

  // IMPORTANT: Graph API $select + $expand limitation
  // When using $expand on singleValueExtendedProperties, some $select fields
  // like internetMessageId may not be returned in the same call.
  // Solution: two separate fetches — one for extended props, one for message fields.
  
  // Pass 1: get SourceMessageId extended property
  const tgtWithProps = await fetchAllMessages(
    tgtClient, tgtEmail, tgtFolderId,
    'id,singleValueExtendedProperties',
    expand
  );
  
  // Pass 2: get message fields for lookup (no $expand needed)
  const tgtMessages = await fetchAllMessages(
    tgtClient, tgtEmail, tgtFolderId,
    'id,subject,receivedDateTime,internetMessageId,hasAttachments'
  );

  // Merge: map by id
  const propsById = new Map();
  for (const m of tgtWithProps) propsById.set(m.id, m);
  for (const m of tgtMessages) {
    const withProps = propsById.get(m.id);
    if (withProps) m.singleValueExtendedProperties = withProps.singleValueExtendedProperties;
  }

  // Helper: normalize internetMessageId — strip angle brackets for consistent matching
  // Source: <abc@domain>  Target might also be <abc@domain> but normalize both to be safe
  const normalizeId = id => id ? id.replace(/^<|>$/g, '').trim().toLowerCase() : null;

  for (const msg of tgtMessages) {
    // Index by SourceMessageId property (set during migration)
    const prop = msg.singleValueExtendedProperties?.find(p => p.id === MIGRATION_PROPERTY_ID);
    if (prop?.value) {
      bySourceId.set(prop.value, msg);
    }
    // Index by internetMessageId — normalized
    if (msg.internetMessageId) {
      byInternetMsgId.set(normalizeId(msg.internetMessageId), msg);
    }
    // Also index by subject+receivedDateTime as last-resort fallback
    if (msg.subject && msg.receivedDateTime) {
      byInternetMsgId.set(`subj:${msg.subject}|${msg.receivedDateTime}`, msg);
    }
  }

  logger.info(`   ✓ Target index: ${bySourceId.size} by sourceId, ${byInternetMsgId.size} by internetMsgId, ${tgtMessages.length} total`);
  return { bySourceId, byInternetMsgId, tgtMessages };
}

// ── Fetch actual attachment list for a message ────────────────────────────────
async function getAttachmentCount(client, userEmail, messageId) {
  try {
    const result = await client.get(
      `/users/${userEmail}/messages/${messageId}/attachments`,
      { '$select': 'id,name,size' }
    );
    return (result.value || []).length;
  } catch (e) {
    return -1; // -1 = error, treat as unknown
  }
}

// ── Fetch full attachment details from source ─────────────────────────────────
// ── List attachment metadata for one message (no contentBytes) ───────────────
async function listAttachmentMeta(srcClient, srcEmail, messageId, logger = null) {
  try {
    const result = await srcClient.get(
      `/users/${srcEmail}/messages/${messageId}/attachments`,
      { '$select': 'id,name,contentType,size,isInline,lastModifiedDateTime' }
    );
    return result.value || [];
  } catch (e) {
    if (logger) logger.warn(`   ⚠️  listAttachmentMeta error for ${messageId}: ${e.message}`);
    return [];
  }
}

// ── Batch GET attachment content — 20 GETs per HTTP call (mirrors fixDrafts) ──
// items: array of { messageId, attId, attMeta }
// Returns: Map of attId → full attachment object with contentBytes
async function batchFetchAttachmentContent(srcAuth, srcEmail, items, logger = null) {
  const results = new Map();
  if (items.length === 0) return results;

  const chunks = [];
  for (let i = 0; i < items.length; i += BATCH_SIZE) chunks.push(items.slice(i, i + BATCH_SIZE));

  let batchNum = 0;
  for (const ch of chunks) {
    batchNum++;
    const requests = ch.map((item, idx) => ({
      id:     String(idx),
      method: 'GET',
      url:    `/users/${srcEmail}/messages/${item.messageId}/attachments/${item.attId}`
    }));

    let batchResp;
    try {
      const token = await srcAuth.getToken();
      batchResp = await axios.post(
        GRAPH_BATCH_URL,
        { requests },
        {
          headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
          validateStatus: null
        }
      );
    } catch (e) {
      if (logger) logger.warn(`   ⚠️  batchFetch network error: ${e.message}`);
      continue;
    }

    if (batchResp.status === 429) {
      const wait = parseInt(batchResp.headers['retry-after'] || '10') * 1000;
      if (logger) logger.warn(`   ⏸️  Batch GET rate limited, waiting ${wait/1000}s...`);
      await sleep(wait);
      const retry = await batchFetchAttachmentContent(srcAuth, srcEmail, ch, logger);
      for (const [k, v] of retry) results.set(k, v);
      continue;
    }

    for (const r of (batchResp.data?.responses || [])) {
      const item = ch[parseInt(r.id)];
      if (r.status === 200 && item) {
        results.set(item.attId, r.body);
      } else if (item && logger && r.status !== 200) {
        logger.warn(`   ⚠️  Batch GET att "${item.attMeta?.name}": status ${r.status}`);
      }
    }

    if (logger && (batchNum % 20 === 0 || batchNum === chunks.length)) {
      const fetched = Math.min(batchNum * BATCH_SIZE, items.length);
      logger.info(`   ⬇️  Step 3b: [${fetched}/${items.length}] ${Math.round(fetched / items.length * 100)}% — batch ${batchNum}/${chunks.length}`);
    }

    await sleep(300); // small pause between batch GET calls
  }

  return results;
}

// ── Upload large attachment via upload session ────────────────────────────────
async function uploadLargeAttachment(tgtAuth, tgtEmail, messageId, attachment, logger) {
  const token = await tgtAuth.getToken();
  // Decode once to get REAL size — attachment.size metadata may differ from decoded base64
  const fileBytes  = Buffer.from(attachment.contentBytes, 'base64');
  const actualSize = fileBytes.length;

  // Retry upload session creation — Exchange may need a moment after batch create
  let sessionResp;
  for (let attempt = 0; attempt < 3; attempt++) {
    sessionResp = await axios.post(
      `${GRAPH_BASE}/users/${tgtEmail}/messages/${messageId}/attachments/createUploadSession`,
      {
        AttachmentItem: {
          attachmentType: 'file',
          name:     attachment.name,
          size:     actualSize,          // use ACTUAL decoded size
          isInline: attachment.isInline || false
        }
      },
      {
        headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
        validateStatus: null
      }
    );
    if (sessionResp.status === 201) break;
    if (sessionResp.status === 429) {
      const wait = parseInt(sessionResp.headers?.['retry-after'] || '10') * 1000;
      await sleep(wait);
    } else if (attempt < 2) {
      await sleep(3000);
    }
  }

  if (sessionResp.status !== 201) {
    throw new Error(`Upload session failed: HTTP ${sessionResp.status}`);
  }

  const uploadUrl = sessionResp.data.uploadUrl;
  const totalSize = actualSize;  // already decoded above
  const chunkSize = 4 * 1024 * 1024;

  // If file fits in one chunk, send in a single PUT
  if (totalSize <= chunkSize) {
    const resp = await axios.put(uploadUrl, fileBytes, {
      headers: {
        'Content-Type':   'application/octet-stream',
        'Content-Length': String(totalSize),
        'Content-Range':  `bytes 0-${totalSize - 1}/${totalSize}`
      },
      validateStatus: null,
      maxBodyLength: Infinity
    });
    if (resp.status !== 200 && resp.status !== 201 && resp.status !== 202) {
      const errMsg = resp.data?.error?.message || JSON.stringify(resp.data) || `HTTP ${resp.status}`;
      throw new Error(`Single chunk upload failed: ${errMsg}`);
    }
    return;
  }

  // Multi-chunk upload — retry each chunk up to 3x on transient errors
  // (changeKey mismatch, 429, 503 can occur between chunks)
  let offset = 0;
  while (offset < totalSize) {
    const end        = Math.min(offset + chunkSize, totalSize);
    const chunkBytes = fileBytes.slice(offset, end);

    let chunkOk = false;
    for (let attempt = 0; attempt < 3; attempt++) {
      const resp = await axios.put(uploadUrl, chunkBytes, {
        headers: {
          'Content-Type':   'application/octet-stream',
          'Content-Range':  `bytes ${offset}-${end - 1}/${totalSize}`,
          'Content-Length': chunkBytes.length
        },
        validateStatus: null,
        maxBodyLength: Infinity
      });

      if (resp.status === 200 || resp.status === 201 || resp.status === 202) {
        chunkOk = true;
        break;
      }

      const errMsg = resp.data?.error?.message || JSON.stringify(resp.data) || `HTTP ${resp.status}`;
      const isRetryable = resp.status === 429 || resp.status === 503 || resp.status === 504
        || errMsg.includes('change key') || errMsg.includes('changeKey');

      if (isRetryable && attempt < 2) {
        const wait = resp.status === 429
          ? parseInt(resp.headers?.['retry-after'] || '10') * 1000
          : 5000;
        await sleep(wait);
      } else {
        throw new Error(`Chunk upload failed at offset ${offset}: ${errMsg}`);
      }
    }

    offset = end;
  }
}

// ── Core: scan source, find missing in target, fix ────────────────────────────
async function fixFolder(srcClient, tgtClient, srcAuth, tgtAuth,
                         srcEmail, tgtEmail, srcFolderId, tgtFolderId,
                         folderName, logger, stats) {

  logger.info(`\n   📁 ${folderName}: scanning...`);

  // ── PHASE 1: Get source messages with attachments ──────────────────────────
  logger.info(`   📥 Phase 1/4: Loading source messages with hasAttachments=true...`);

  const srcMessages = await fetchAllMessages(
    srcClient, srcEmail, srcFolderId,
    'id,subject,receivedDateTime,sentDateTime,hasAttachments,internetMessageId,body,' +
    'from,toRecipients,ccRecipients,bccRecipients,replyTo,flag,importance,isRead,categories,internetMessageHeaders'
  );

  const srcWithAttachments = srcMessages.filter(m => m.hasAttachments);

  if (srcWithAttachments.length === 0) {
    logger.info(`   ✅ No source messages with attachments in this folder`);
    return;
  }

  logger.info(`   ✓ Source: ${srcMessages.length} total, ${srcWithAttachments.length} with attachments`);

  // ── PHASE 2: Build target lookup, find which are missing attachments ────────
  logger.info(`   🔍 Phase 2/4: Checking target for missing attachments...`);

  const { bySourceId, byInternetMsgId } = await buildTargetLookup(
    tgtClient, tgtEmail, tgtFolderId, logger
  );

  const limit = pLimit(CONCURRENCY);
  let checked = 0;
  const toFix = []; // { srcMsg, tgtMsg }

  // Diagnostic: log a sample of internetMessageId format from both sides
  const srcSample = srcWithAttachments.slice(0, 3).map(m => m.internetMessageId || 'null');
  const tgtSample = [...byInternetMsgId.keys()].slice(0, 3);
  logger.info(`   🔬 Diag — src internetMsgId samples: ${JSON.stringify(srcSample)}`);
  logger.info(`   🔬 Diag — tgt internetMsgId samples: ${JSON.stringify(tgtSample)}`);
  logger.info(`   🔬 Diag — bySourceId size: ${bySourceId.size}, byInternetMsgId size: ${byInternetMsgId.size}`);

  let notFoundCount = 0, foundCount = 0, attCheckCount = 0;

  await Promise.all(srcWithAttachments.map(srcMsg => limit(async () => {
    // Find corresponding target message
    const normalizeId = id => id ? id.replace(/^<|>$/g, '').trim().toLowerCase() : null;
    let tgtMsg = bySourceId.get(srcMsg.id);

    // Try normalized internetMessageId
    if (!tgtMsg && srcMsg.internetMessageId) {
      tgtMsg = byInternetMsgId.get(normalizeId(srcMsg.internetMessageId));
    }

    // Last resort: subject + receivedDateTime
    if (!tgtMsg && srcMsg.subject && srcMsg.receivedDateTime) {
      tgtMsg = byInternetMsgId.get(`subj:${srcMsg.subject}|${srcMsg.receivedDateTime}`);
    }

    if (!tgtMsg) {
      // Not found in target at all — not yet migrated, skip
      notFoundCount++;
      if (notFoundCount <= 3) {
        logger.info(`   🔬 Not found in target: "${srcMsg.subject}" | internetMsgId: ${srcMsg.internetMessageId || 'null'}`);
      }
      checked++;
      return;
    }

    foundCount++;

    // Check actual attachment count in target
    const attCount = await getAttachmentCount(tgtClient, tgtEmail, tgtMsg.id);
    attCheckCount++;

    if (attCount === 0) {
      // Target has 0 actual attachments but source has some → needs fix
      toFix.push({ srcMsg, tgtMsg });
    } else if (attCount === -1) {
      // Error fetching — log for diagnosis
      logger.warn(`   🔬 Attachment check error for "${srcMsg.subject}" tgtId: ${tgtMsg.id}`);
    }

    checked++;
    if (checked % 100 === 0 || checked === srcWithAttachments.length) {
      logger.info(`   🔍 [${checked}/${srcWithAttachments.length}] found:${foundCount} notFound:${notFoundCount} checked:${attCheckCount} needFix:${toFix.length}`);
    }
  })));

  logger.info(`   🔬 Final: ${foundCount} found, ${notFoundCount} not found in target, ${attCheckCount} attachment checks, ${toFix.length} need fix`);

  if (toFix.length === 0) {
    logger.info(`   ✅ No missing attachments (all ${srcWithAttachments.length} already have them)`);
    return;
  }

  logger.info(`   📎 Found ${chalk.yellow(toFix.length + ' messages')} missing attachments`);

  if (DRY_RUN) {
    logger.info(`   [DRY RUN] Would fix ${toFix.length} messages`);
    stats.fixed += toFix.length;
    return;
  }

  // ── PHASE 3: Fetch full attachments from source ─────────────────────────────
  // ── PHASE 3+4: Process in chunks to avoid OOM ──────────────────────────────
  // Loading all contentBytes at once (4000+ attachments) crashes Node with OOM.
  // Solution: process MSG_CHUNK_SIZE messages at a time — fetch+delete+create per chunk.
  const MSG_CHUNK_SIZE = 50;
  logger.info(`   ⬇️  Phase 3+4: Processing ${toFix.length} messages in chunks of ${MSG_CHUNK_SIZE}...`);

  const metaLimit = pLimit(3);
  let totalCreated = 0, totalFailed = 0, chunkNum = 0;

  for (const msgChunk of chunk(toFix, MSG_CHUNK_SIZE)) {
    chunkNum++;
    logger.info(`   📦 Chunk ${chunkNum}/${Math.ceil(toFix.length / MSG_CHUNK_SIZE)} (${msgChunk.length} msgs)...`);

    // 3a: metadata
    const withMeta = await Promise.all(
      msgChunk.map(({ srcMsg, tgtMsg }) => metaLimit(async () => ({
        srcMsg, tgtMsg,
        metas: await listAttachmentMeta(srcClient, srcEmail, srcMsg.id, logger)
      })))
    );

    // 3b: batch GET content
    const allAttItems = [];
    for (const { srcMsg, tgtMsg, metas } of withMeta) {
      for (const meta of metas) allAttItems.push({ messageId: srcMsg.id, attId: meta.id, attMeta: meta, srcMsg, tgtMsg });
    }
    if (allAttItems.length === 0) continue;

    const contentMap = await batchFetchAttachmentContent(srcAuth, srcEmail, allAttItems, logger);

    const enriched = withMeta.map(({ srcMsg, tgtMsg, metas }) => ({
      srcMsg, tgtMsg,
      attachments: metas.map(m => ({ ...m, ...(contentMap.get(m.id) || {}) })).filter(a => a.contentBytes)
    }));

    const withAtts = enriched.filter(e => e.attachments.length > 0);
    if (withAtts.length === 0) { contentMap.clear(); continue; }

    // 4a: delete
    const deleted = new Set();
    for (const c of chunk(withAtts, BATCH_SIZE)) {
      const reqs = c.map((e, idx) => ({ id: String(idx), method: 'DELETE', url: `/users/${tgtEmail}/messages/${e.tgtMsg.id}` }));
      const res  = await sendBatch(tgtAuth, reqs);
      c.forEach((e, idx) => { const r = res[String(idx)]; if (r && (r.status === 204 || r.status === 404)) deleted.add(e.tgtMsg.id); });
    }
    await sleep(PHASE_DELAY_MS);

    const toCreate = withAtts.filter(e => deleted.has(e.tgtMsg.id));
    if (toCreate.length === 0) { contentMap.clear(); continue; }

    // 4b: recreate
    const BATCH_PAYLOAD_LIMIT = 2.5 * 1024 * 1024;
    const buildMsgPayload = (e) => {
      const p = buildPayload(e.srcMsg, e.srcMsg.id);
      const small = e.attachments.filter(a => a.contentBytes && a.size <= ATTACHMENT_INLINE_LIMIT);
      if (small.length > 0) p.attachments = small.map(a => ({
        '@odata.type': '#microsoft.graph.fileAttachment',
        name: a.name, contentType: a.contentType, contentBytes: a.contentBytes,
        isInline: a.isInline || false, contentId: a.contentId || null
      }));
      return p;
    };

    const canBatch   = toCreate.filter(e => e.attachments.filter(a => a.size <= ATTACHMENT_INLINE_LIMIT).reduce((s, a) => s + a.size, 0) <= BATCH_PAYLOAD_LIMIT);
    const mustSingle = toCreate.filter(e => e.attachments.filter(a => a.size <= ATTACHMENT_INLINE_LIMIT).reduce((s, a) => s + a.size, 0) >  BATCH_PAYLOAD_LIMIT);
    let chunkCreated = 0, chunkFailed = 0;

    for (const e of mustSingle) {
      try {
        const token = await tgtAuth.getToken();
        const resp  = await require('axios').post(`${GRAPH_BASE}/users/${tgtEmail}/mailFolders/${tgtFolderId}/messages`, buildMsgPayload(e),
          { headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' }, validateStatus: null });
        if (resp.status === 201) {
          chunkCreated++;
          if (resp.data?.id) for (const att of e.attachments.filter(a => a.size > ATTACHMENT_INLINE_LIMIT))
            try { await uploadLargeAttachment(tgtAuth, tgtEmail, resp.data.id, att, logger); }
            catch (err) { logger.error(`   ✗ Large att "${att.name}": ${err.message}`); }
        } else { logger.error(`   ✗ "${e.srcMsg.subject}": ${resp.data?.error?.message || resp.status}`); chunkFailed++; }
      } catch (err) { logger.error(`   ✗ "${e.srcMsg.subject}": ${err.message}`); chunkFailed++; }
      await sleep(300);
    }

    for (let bi = 0; bi < chunk(canBatch, BATCH_SIZE).length; bi++) {
      const batch = chunk(canBatch, BATCH_SIZE)[bi];
      const buildReqs = () => batch.map((e, idx) => ({ id: String(idx), method: 'POST', url: `/users/${tgtEmail}/mailFolders/${tgtFolderId}/messages`, headers: { 'Content-Type': 'application/json' }, body: buildMsgPayload(e) }));
      let results = {};
      for (let attempt = 0; attempt <= MAX_BATCH_RETRIES; attempt++) {
        results = await sendBatch(tgtAuth, buildReqs());
        const hasQuota = batch.some((e, idx) => { const r = results[String(idx)]; const err = r?.body?.error?.message || ''; return !r || err.includes('Request limit') || err.includes('MailboxConcurrency') || err.includes('IncomingBytes'); });
        if (!hasQuota) break;
        if (attempt < MAX_BATCH_RETRIES) { logger.warn(`   ⏸️  Quota — pausing 60s...`); await sleep(QUOTA_RETRY_WAIT_MS); }
      }
      for (let idx = 0; idx < batch.length; idx++) {
        const e = batch[idx]; const r = results[String(idx)];
        if (r?.status === 201) {
          chunkCreated++;
          if (r.body?.id) for (const att of e.attachments.filter(a => a.size > ATTACHMENT_INLINE_LIMIT))
            try { await uploadLargeAttachment(tgtAuth, tgtEmail, r.body.id, att, logger); }
            catch (err) { logger.error(`   ✗ Large att "${att.name}": ${err.message}`); }
        } else {
          const errMsg = r?.body?.error?.message || (r ? `HTTP ${r.status}` : 'no response');
          if (!r || errMsg.includes('Request limit') || errMsg.includes('MailboxConcurrency') || errMsg.includes('IncomingBytes'))
            logger.warn(`   ⚠️  Quota: "${e.srcMsg.subject}" — next run will fix`);
          else logger.error(`   ✗ "${e.srcMsg.subject}": ${errMsg}`);
          chunkFailed++;
        }
      }
      if (bi < chunk(canBatch, BATCH_SIZE).length - 1) {
        if ((bi + 1) % BATCH_PAUSE_EVERY === 0) await sleep(8000); else await sleep(BATCH_DELAY_MS);
      }
    }

    logger.info(`   ✓ Chunk ${chunkNum}: ${chunkCreated} created, ${chunkFailed} failed`);
    totalCreated += chunkCreated; totalFailed += chunkFailed;
    contentMap.clear(); // free memory
  }

  logger.info(`   ✓ Phase 3+4: ${totalCreated} recreated, ${totalFailed} failed`);
  stats.fixed  += totalCreated;
  stats.failed += totalFailed;

}

// ── Process a single user ─────────────────────────────────────────────────────
async function processUser(srcClient, tgtClient, srcAuth, tgtAuth, user, logger) {
  const stats = { fixed: 0, failed: 0 };
  logger.info(`\n👤 ${user.sourceEmail} → ${user.targetEmail}`);
  logger.info('   📂 Loading folders...');

  // Load and dedup source folders
  const srcFolderMap = new Map();
  for await (const f of srcClient.paginate(
    `/users/${user.sourceEmail}/mailFolders`,
    { '$top': 100, '$expand': 'childFolders' }, 'folders'
  )) {
    srcFolderMap.set(f.id, f);
    for (const c of (f.childFolders || [])) srcFolderMap.set(c.id, c);
  }

  // Load and dedup target folders
  const tgtFolderMap = new Map(); // displayName.toLowerCase() → id
  for await (const f of tgtClient.paginate(
    `/users/${user.targetEmail}/mailFolders`,
    { '$top': 100, '$expand': 'childFolders' }, 'folders'
  )) {
    tgtFolderMap.set(f.displayName.toLowerCase(), f.id);
    for (const c of (f.childFolders || [])) tgtFolderMap.set(c.displayName.toLowerCase(), c.id);
  }

  const srcFolders = [...srcFolderMap.values()];
  logger.info(`   ✓ ${srcFolders.length} source folders, ${tgtFolderMap.size} target folders`);

  for (let fi = 0; fi < srcFolders.length; fi++) {
    const srcFolder  = srcFolders[fi];
    const tgtFolderId = tgtFolderMap.get(srcFolder.displayName.toLowerCase());
    if (!tgtFolderId) continue; // not migrated yet

    const before = stats.fixed + stats.failed;
    await fixFolder(
      srcClient, tgtClient, srcAuth, tgtAuth,
      user.sourceEmail, user.targetEmail,
      srcFolder.id, tgtFolderId,
      srcFolder.displayName, logger, stats
    );
    const processed = (stats.fixed + stats.failed) - before;
    if (processed > 0 && fi < srcFolders.length - 1) await sleep(FOLDER_DELAY_MS);
  }

  logger.info(`\n   ✅ Done: ${stats.fixed} fixed, ${stats.failed} failed`);
  return stats;
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  const mainLogger = new Logger(config.logs_dir || './logs', 'fix-attachments');

  mainLogger.info(chalk.cyan('\n📎 Attachment Fixer v4') + (DRY_RUN ? chalk.yellow(' [DRY RUN]') : ''));
  mainLogger.info(`   Source: ${config.source_tenant.domain}`);
  mainLogger.info(`   Target: ${config.target_tenant.domain}`);

  const userLoader = new UserLoader(config.users_csv);
  const users      = userLoader.load();
  mainLogger.info(`   Users: ${users.length}`);

  const srcAuth = new TenantAuth(config.source_tenant, 'source');
  await srcAuth.getToken();
  const srcClient = new GraphClient(srcAuth, config.migration, mainLogger);

  const tgtAuth = new TenantAuth(config.target_tenant, 'target');
  await tgtAuth.getToken();
  const tgtClient = new GraphClient(tgtAuth, config.migration, mainLogger);

  mainLogger.success('Both tenants authenticated ✓');

  const globalStats = { fixed: 0, failed: 0 };

  for (let i = 0; i < users.length; i++) {
    const user   = users[i];
    const logger = new Logger(
      config.logs_dir || './logs',
      user.sourceEmail.replace('@', '_').replace(/\./g, '_')
    );

    try {
      const s = await processUser(srcClient, tgtClient, srcAuth, tgtAuth, user, logger);
      globalStats.fixed  += s.fixed;
      globalStats.failed += s.failed;
    } catch (err) {
      logger.error(`Fatal error for ${user.sourceEmail}: ${err.message}`);
    }

    if (i < users.length - 1) {
      mainLogger.info(`\n   ⏳ Waiting 60s before next user (quota recovery)...`);
      await sleep(USER_DELAY_MS);
    }
  }

  mainLogger.info(chalk.green(`\n✅ All done: ${globalStats.fixed} fixed, ${globalStats.failed} failed`));
}

main().catch(err => {
  console.error(chalk.red('Fatal:'), err.message);
  process.exit(1);
});