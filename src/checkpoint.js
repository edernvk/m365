const fs = require('fs');
const path = require('path');

class CheckpointManager {
  constructor(checkpointFile) {
    this.file = checkpointFile;
    
    // Ensure parent directory exists
    const dir = path.dirname(this.file);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    
    this.data = this._load();
  }

  _load() {
    if (fs.existsSync(this.file)) {
      // Se for pasta, retorna vazio (não tenta apagar)
      try {
        const stats = fs.statSync(this.file);
        if (stats.isDirectory()) {
          return {};
        }
        return JSON.parse(fs.readFileSync(this.file, 'utf8'));
      } catch (e) {
        return {};
      }
    }
    return {};
  }

  // Método load() público
  load() {
    return this.data;
  }

  save() {
    try {
      // Se resume.json é pasta, não salva
      if (fs.existsSync(this.file) && fs.statSync(this.file).isDirectory()) {
        return;
      }
      fs.writeFileSync(this.file, JSON.stringify(this.data, null, 2));
    } catch (e) {
      // Ignora erros de salvamento
    }
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