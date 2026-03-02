const fs = require('fs');

class CheckpointManager {
  constructor(checkpointFile) {
    this.file = checkpointFile;
    this.data = this._load();
  }

  _load() {
    if (fs.existsSync(this.file)) {
      try {
        return JSON.parse(fs.readFileSync(this.file, 'utf8'));
      } catch (e) {
        return {};
      }
    }
    return {};
  }

  save() {
    fs.writeFileSync(this.file, JSON.stringify(this.data, null, 2));
  }

  getUserCheckpoint(userEmail, workload) {
    if (!this.data[userEmail]) this.data[userEmail] = {};
    if (!this.data[userEmail][workload]) this.data[userEmail][workload] = {};
    return this.data[userEmail][workload];
  }

  markUserDone(userEmail, workload) {
    if (!this.data[userEmail]) this.data[userEmail] = {};
    this.data[userEmail][`${workload}_completed`] = new Date().toISOString();
    this.save();
  }

  isUserDone(userEmail, workload) {
    return !!(this.data[userEmail] && this.data[userEmail][`${workload}_completed`]);
  }

  reset(userEmail = null) {
    if (userEmail) {
      delete this.data[userEmail];
    } else {
      this.data = {};
    }
    this.save();
  }

  getProgress() {
    const result = {};
    for (const [user, workloads] of Object.entries(this.data)) {
      result[user] = {};
      for (const [key, val] of Object.entries(workloads)) {
        if (key.endsWith('_completed')) {
          result[user][key.replace('_completed', '')] = 'DONE';
        }
      }
    }
    return result;
  }
}

module.exports = CheckpointManager;
