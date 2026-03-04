const fs = require('fs');
const path = require('path');

class Logger {
  constructor(name = 'main', logsDir = './logs') {
    this.name = name;
    this.logsDir = logsDir;
    
    // Garante que a pasta de logs existe
    if (!fs.existsSync(logsDir)) {
      fs.mkdirSync(logsDir, { recursive: true });
    }
    
    this.logFile = path.join(logsDir, `${name}.log`);
  }

  _timestamp() {
    // Usa timezone local (America/Sao_Paulo via TZ env var)
    const now = new Date();
    
    // Formata: YYYY-MM-DDTHH:mm:ss.sssZ mas com offset local
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');
    const seconds = String(now.getSeconds()).padStart(2, '0');
    const ms = String(now.getMilliseconds()).padStart(3, '0');
    
    // Calcula offset do timezone (-03:00 para Brasília)
    const offset = -now.getTimezoneOffset();
    const offsetHours = String(Math.floor(Math.abs(offset) / 60)).padStart(2, '0');
    const offsetMinutes = String(Math.abs(offset) % 60).padStart(2, '0');
    const offsetSign = offset >= 0 ? '+' : '-';
    
    return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}.${ms}${offsetSign}${offsetHours}:${offsetMinutes}`;
  }

  _log(level, message) {
    const timestamp = this._timestamp();
    const logMessage = `[${timestamp}] [${level}] [${this.name}] ${message}`;
    
    // Console
    console.log(logMessage);
    
    // File
    try {
      fs.appendFileSync(this.logFile, logMessage + '\n');
    } catch (err) {
      console.error(`Failed to write to log file: ${err.message}`);
    }
  }

  info(message) {
    this._log('INFO', message);
  }

  success(message) {
    this._log('SUCCESS', message);
  }

  warn(message) {
    this._log('WARN', message);
  }

  error(message) {
    this._log('ERROR', message);
  }

  debug(message) {
    if (process.env.VERBOSE === 'true') {
      this._log('DEBUG', message);
    }
  }
}

module.exports = Logger;