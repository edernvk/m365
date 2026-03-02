/**
 * OneDrive Migration Module
 * Migrates files and folders preserving structure and metadata
 */

const axios = require('axios');

class OneDriveMigrator {
  constructor(sourceClient, targetClient, config, logger) {
    this.src = sourceClient;
    this.tgt = targetClient;
    this.config = config;
    this.logger = logger;
    this.pageSize = config.onedrive_page_size || 200;
  }

  async migrate(sourceEmail, targetEmail, checkpoint = {}) {
    this.logger.info(`Starting OneDrive migration: ${sourceEmail} → ${targetEmail}`);

    const stats = {
      folders_created: 0,
      files_total: 0,
      files_migrated: 0,
      files_skipped: 0,
      files_failed: 0,
      bytes_migrated: 0
    };

    try {
      // Get source drive root
      const srcDrive = await this.src.get(`/users/${sourceEmail}/drive/root`);
      this.logger.info(`Source OneDrive root: ${srcDrive.id}`);

      // Get target drive root
      const tgtDrive = await this.tgt.get(`/users/${targetEmail}/drive/root`);
      this.logger.info(`Target OneDrive root: ${tgtDrive.id}`);

      // Migrate recursively from root
      await this._migrateFolder(
        sourceEmail, 'root', '/',
        targetEmail, 'root',
        checkpoint, stats
      );

      this.logger.success(
        `OneDrive migration complete: ${stats.files_migrated} files (${this._formatBytes(stats.bytes_migrated)}), ${stats.files_failed} failed`
      );

      return { success: true, stats };

    } catch (err) {
      this.logger.error(`OneDrive migration failed: ${err.message}`);
      return { success: false, error: err.message, stats };
    }
  }

  async _migrateFolder(srcEmail, srcFolderId, srcPath, tgtEmail, tgtFolderId, checkpoint, stats) {
    // List children of this folder
    const items = [];
    for await (const item of this.src.paginate(
      `/users/${srcEmail}/drive/items/${srcFolderId}/children`,
      { '$top': this.pageSize }
    )) {
      items.push(item);
    }

    this.logger.info(`Processing folder: ${srcPath} (${items.length} items)`);

    for (const item of items) {
      const itemPath = `${srcPath}/${item.name}`.replace('//', '/');

      if (item.folder) {
        // Create folder in target
        const folderKey = `drive_folder_${item.id}`;
        let tgtSubFolderId;

        if (checkpoint[folderKey]) {
          tgtSubFolderId = checkpoint[folderKey];
        } else {
          if (!this.config.dry_run) {
            const newFolder = await this._ensureFolder(tgtEmail, tgtFolderId, item.name);
            tgtSubFolderId = newFolder.id;
            checkpoint[folderKey] = tgtSubFolderId;
            stats.folders_created++;
          } else {
            tgtSubFolderId = `dry_run_${item.id}`;
          }
        }

        // Recurse into subfolder
        await this._migrateFolder(
          srcEmail, item.id, itemPath,
          tgtEmail, tgtSubFolderId,
          checkpoint, stats
        );

      } else if (item.file) {
        const fileKey = `drive_file_${item.id}`;

        if (checkpoint[fileKey] === 'done') {
          stats.files_skipped++;
          continue;
        }

        stats.files_total++;

        if (this.config.dry_run) {
          this.logger.info(`[DRY RUN] Would migrate file: ${itemPath} (${this._formatBytes(item.size)})`);
          stats.files_migrated++;
          continue;
        }

        try {
          await this._migrateFile(srcEmail, item, tgtEmail, tgtFolderId, itemPath);
          checkpoint[fileKey] = 'done';
          stats.files_migrated++;
          stats.bytes_migrated += item.size || 0;
        } catch (err) {
          this.logger.error(`Failed to migrate file "${itemPath}": ${err.message}`);
          stats.files_failed++;
        }
      }
    }
  }

  async _ensureFolder(userEmail, parentFolderId, folderName) {
    try {
      const result = await this.tgt.post(
        `/users/${userEmail}/drive/items/${parentFolderId}/children`,
        {
          name: folderName,
          folder: {},
          '@microsoft.graph.conflictBehavior': 'rename'
        }
      );
      return result;
    } catch (err) {
      // Folder might already exist
      for await (const item of this.tgt.paginate(
        `/users/${userEmail}/drive/items/${parentFolderId}/children`
      )) {
        if (item.name === folderName && item.folder) return item;
      }
      throw err;
    }
  }

  async _migrateFile(srcEmail, srcItem, tgtEmail, tgtFolderId, itemPath) {
    // Get download URL for the source file
    const srcItemDetail = await this.src.get(
      `/users/${srcEmail}/drive/items/${srcItem.id}`,
      { '$select': 'id,name,size,@microsoft.graph.downloadUrl' }
    );

    const downloadUrl = srcItemDetail['@microsoft.graph.downloadUrl'];
    if (!downloadUrl) {
      throw new Error(`No download URL for file: ${itemPath}`);
    }

    const fileSize = srcItem.size || 0;

    if (fileSize <= 4 * 1024 * 1024) {
      // Small file: direct upload (< 4MB)
      await this._uploadSmallFile(tgtEmail, tgtFolderId, srcItem.name, downloadUrl, fileSize);
    } else {
      // Large file: resumable upload session
      await this._uploadLargeFile(tgtEmail, tgtFolderId, srcItem.name, downloadUrl, fileSize);
    }

    this.logger.info(`✓ ${itemPath} (${this._formatBytes(fileSize)})`);
  }

  async _uploadSmallFile(tgtEmail, tgtFolderId, fileName, downloadUrl, fileSize) {
    // Download file content
    const downloadResponse = await axios.get(downloadUrl, {
      responseType: 'arraybuffer',
      timeout: 60000
    });

    const fileBuffer = Buffer.from(downloadResponse.data);

    // Upload to target
    const tgtHeaders = await this.tgt.auth.getHeaders();
    await axios.put(
      `https://graph.microsoft.com/v1.0/users/${tgtEmail}/drive/items/${tgtFolderId}:/${encodeURIComponent(fileName)}:/content`,
      fileBuffer,
      {
        headers: {
          ...tgtHeaders,
          'Content-Type': 'application/octet-stream',
          'Content-Length': fileBuffer.length
        },
        maxBodyLength: Infinity
      }
    );
  }

  async _uploadLargeFile(tgtEmail, tgtFolderId, fileName, downloadUrl, fileSize) {
    const tgtHeaders = await this.tgt.auth.getHeaders();

    // Create upload session
    const sessionResponse = await axios.post(
      `https://graph.microsoft.com/v1.0/users/${tgtEmail}/drive/items/${tgtFolderId}:/${encodeURIComponent(fileName)}:/createUploadSession`,
      {
        item: {
          '@microsoft.graph.conflictBehavior': 'rename',
          name: fileName
        }
      },
      { headers: tgtHeaders }
    );

    const uploadUrl = sessionResponse.data.uploadUrl;
    const CHUNK_SIZE = 5 * 1024 * 1024; // 5MB chunks

    // Download and upload in chunks
    let offset = 0;
    while (offset < fileSize) {
      const end = Math.min(offset + CHUNK_SIZE - 1, fileSize - 1);

      const chunkResponse = await axios.get(downloadUrl, {
        headers: { Range: `bytes=${offset}-${end}` },
        responseType: 'arraybuffer',
        timeout: 120000
      });

      const chunk = Buffer.from(chunkResponse.data);

      await axios.put(uploadUrl, chunk, {
        headers: {
          'Content-Length': chunk.length,
          'Content-Range': `bytes ${offset}-${end}/${fileSize}`
        },
        maxBodyLength: Infinity,
        validateStatus: s => s < 500
      });

      offset += CHUNK_SIZE;
    }
  }

  _formatBytes(bytes) {
    if (!bytes) return '0 B';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return `${(bytes / Math.pow(1024, i)).toFixed(1)} ${units[i]}`;
  }
}

module.exports = OneDriveMigrator;
