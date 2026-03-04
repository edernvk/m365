const fs = require('fs');

class UserLoader {
  constructor(csvPath) {
    this.csvPath = csvPath;
  }

  load() {
    const csv = fs.readFileSync(this.csvPath, 'utf8');
    const lines = csv.trim().split('\n');
    
    // Skip header
    const users = [];
    for (let i = 1; i < lines.length; i++) {
      const [sourceEmail, targetEmail, displayName] = lines[i].split(',');
      if (sourceEmail && targetEmail) {
        users.push({
          sourceEmail: sourceEmail.trim(),
          targetEmail: targetEmail.trim(),
          displayName: (displayName || '').trim()
        });
      }
    }
    
    return users;
  }
}

module.exports = UserLoader;