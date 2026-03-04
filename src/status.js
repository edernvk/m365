#!/usr/bin/env node
/**
 * Migration status checker
 * Usage: node src/status.js
 */
const fs = require('fs');
const path = require('path');
const chalk = require('chalk');

const CONFIG_PATH = path.resolve(process.cwd(), 'config.json');
const config = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
const summaryFile = path.join(config.logs_dir || './logs', 'summary.json');
const checkpointFile = config.checkpoint_file || './resume.json';

console.log(chalk.bold.cyan('\n📊 Migration Status Report\n'));

if (!fs.existsSync(summaryFile)) {
  console.log(chalk.yellow('No migration data found yet. Run npm start to begin.'));
  process.exit(0);
}

const summary = JSON.parse(fs.readFileSync(summaryFile, 'utf8'));
const checkpoint = fs.existsSync(checkpointFile)
  ? JSON.parse(fs.readFileSync(checkpointFile, 'utf8'))
  : {};

let totalEmails = 0, totalFiles = 0, totalFailed = 0;

for (const [user, workloads] of Object.entries(summary)) {
  console.log(chalk.bold(`👤 ${user}`));

  for (const [wl, stats] of Object.entries(workloads)) {
    const isDone = checkpoint[user] && checkpoint[user][`${wl}_completed`];
    const status = isDone ? chalk.green('✓ DONE') : chalk.yellow('⏳ IN PROGRESS');

    console.log(`   ${status} ${chalk.cyan(wl.padEnd(12))}`);

    if (stats.messages_migrated !== undefined) {
      console.log(`          Emails: ${stats.messages_migrated} migrated, ${stats.messages_failed || 0} failed`);
      totalEmails += stats.messages_migrated || 0;
      totalFailed += stats.messages_failed || 0;
    }
    if (stats.files_migrated !== undefined) {
      const bytes = stats.bytes_migrated || 0;
      const size = bytes > 1073741824 ? `${(bytes/1073741824).toFixed(1)} GB`
                 : bytes > 1048576 ? `${(bytes/1048576).toFixed(1)} MB`
                 : `${(bytes/1024).toFixed(0)} KB`;
      console.log(`          Files: ${stats.files_migrated} migrated (${size}), ${stats.files_failed || 0} failed`);
      totalFiles += stats.files_migrated || 0;
      totalFailed += stats.files_failed || 0;
    }
    if (stats.migrated !== undefined && stats.messages_migrated === undefined && stats.files_migrated === undefined) {
      console.log(`          Items: ${stats.migrated} migrated, ${stats.failed || 0} failed`);
    }

    if (stats.updatedAt) {
      console.log(chalk.gray(`          Last updated: ${stats.updatedAt}`));
    }
  }
  console.log('');
}

console.log(chalk.bold('─'.repeat(40)));
console.log(chalk.bold(`Total emails migrated: ${totalEmails}`));
console.log(chalk.bold(`Total files migrated:  ${totalFiles}`));
if (totalFailed > 0) console.log(chalk.red(`Total failures:        ${totalFailed}`));
console.log('');
