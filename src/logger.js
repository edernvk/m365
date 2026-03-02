const fs = require('fs');
const path = require('path');
const chalk = require('chalk');

class Logger {
  constructor(logsDir, userName = 'general') {
    this.logsDir = logsDir;
    this.userName = userName;
    this.logFile = path.join(logsDir, `${userName.replace(/@/g, '_')}.log`);
    this.summaryFile = path.join(logsDir, 'summary.json');

    if (!fs.existsSync(logsDir)) {
      fs.mkdirSync(logsDir, { recursive: true });
    }
  }

  _timestamp() {
    return new Date().toISOString();
  }

  _write(level, message, data = null) {
    const entry = {
      timestamp: this._timestamp(),
      level,
      user: this.userName,
      message,
      ...(data && { data })
    };

    const line = JSON.stringify(entry) + '\n';
    fs.appendFileSync(this.logFile, line);

    const color = { INFO: chalk.blue, SUCCESS: chalk.green, WARN: chalk.yellow, ERROR: chalk.red };
    const prefix = `[${entry.timestamp}] [${level}] [${this.userName}]`;
    console.log((color[level] || chalk.white)(prefix), message);
    if (data && process.env.VERBOSE) console.log(chalk.gray(JSON.stringify(data, null, 2)));
  }

  info(msg, data) { this._write('INFO', msg, data); }
  success(msg, data) { this._write('SUCCESS', msg, data); }
  warn(msg, data) { this._write('WARN', msg, data); }
  error(msg, data) { this._write('ERROR', msg, data); }

  updateSummary(userEmail, workload, stats) {
    let summary = {};
    if (fs.existsSync(this.summaryFile)) {
      summary = JSON.parse(fs.readFileSync(this.summaryFile, 'utf8'));
    }
    if (!summary[userEmail]) summary[userEmail] = {};
    summary[userEmail][workload] = { ...stats, updatedAt: this._timestamp() };
    fs.writeFileSync(this.summaryFile, JSON.stringify(summary, null, 2));
  }
}

module.exports = Logger;
