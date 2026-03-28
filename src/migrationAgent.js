/**
 * migrationAgent.js — Autonomous Migration Agent
 *
 * Runs all migration steps for a user, verifies the result,
 * and retries automatically until source matches target (or max retries).
 *
 * Mission: EVERYTHING in source MUST exist in target — no broken attachments,
 *          no draft flags, no duplicates, no missing inline images.
 *
 * Flow per user (loops up to MAX_RETRIES):
 *   1. Migrate emails
 *   2. Fix drafts
 *   3. Fix attachments (incl. inline images)
 *   4. Remove duplicates
 *   5. Verify source vs target
 *   → If match < THRESHOLD → retry from step 1
 *   → If match >= THRESHOLD → SUCCESS, next user
 */

'use strict';

const fs         = require('fs');
const path       = require('path');
const { spawn }  = require('child_process');
const chalk      = require('chalk');

const MAX_RETRIES = 3;
const MATCH_THRESHOLD = 99.0; // percent — 99% = success

const STEPS = [
  { name: 'migrate',         script: 'migrator.js',         args: ['--workload', 'email'], label: 'Migrar Emails' },
  { name: 'fix-drafts',      script: 'fixDrafts.js',        args: [],                      label: 'Corrigir Rascunhos' },
  { name: 'fix-attachments', script: 'fixAttachments.js',    args: [],                      label: 'Corrigir Anexos' },
  { name: 'dedup',           script: 'dedupEmails.js',       args: [],                      label: 'Remover Duplicados' },
  { name: 'verify',          script: 'verifyMigration.js',   args: [],                      label: 'Verificar' },
];

const ONLY_USER = (() => {
  const idx = process.argv.indexOf('--user');
  if (idx !== -1 && process.argv[idx + 1]) return process.argv[idx + 1];
  const eq = process.argv.find(a => a.startsWith('--user='));
  return eq ? eq.split('=')[1] : null;
})();

const MAX_RETRIES_ARG = (() => {
  const idx = process.argv.indexOf('--max-retries');
  if (idx !== -1 && process.argv[idx + 1]) return parseInt(process.argv[idx + 1]);
  return MAX_RETRIES;
})();

// ── Load config + users ──────────────────────────────────────────────────────
let config;
try {
  config = JSON.parse(fs.readFileSync(path.resolve(process.cwd(), 'config.json'), 'utf8'));
} catch (e) {
  console.error(chalk.red('❌ config.json not found'));
  process.exit(1);
}

function loadUsers() {
  const csvPath = path.resolve(process.cwd(), config.users_csv || 'users.csv');
  if (!fs.existsSync(csvPath)) return [];
  const lines = fs.readFileSync(csvPath, 'utf8').trim().split('\n');
  const users = [];
  for (let i = 1; i < lines.length; i++) {
    const [sourceEmail, targetEmail, displayName] = lines[i].split(',');
    if (sourceEmail && targetEmail) {
      users.push({ sourceEmail: sourceEmail.trim(), targetEmail: targetEmail.trim(), displayName: (displayName || '').trim() });
    }
  }
  return users;
}

function log(msg) {
  const ts = new Date().toISOString();
  console.log(`[${ts}] ${msg}`);
}

// ── Spawn a step and capture output ──────────────────────────────────────────
function runStep(step, userEmail) {
  return new Promise((resolve) => {
    const args = [path.join(__dirname, step.script), ...step.args, '--user', userEmail];

    const child = spawn('node', args, {
      cwd: path.resolve(__dirname, '..'),
      env: { ...process.env, FORCE_COLOR: '0', TZ: 'America/Sao_Paulo' },
      stdio: ['ignore', 'pipe', 'pipe']
    });

    let output = '';
    let fixed = 0, failed = 0;
    let matchPercent = null;
    let totalSource = 0, totalTarget = 0, totalMissing = 0;
    let fatalError = null; // mailbox not found, etc

    const handleData = (data) => {
      const text = data.toString();
      output += text;

      // Pass through to stdout for dashboard/real-time viewing
      process.stdout.write(text);

      // Parse key metrics
      for (const line of text.split('\n')) {
        // User done: X fixed, Y failed
        const doneMatch = line.match(/User done:\s*(\d+)\s*fixed.*?(\d+)\s*failed/);
        if (doneMatch) {
          fixed = parseInt(doneMatch[1]);
          failed = parseInt(doneMatch[2]);
        }

        // Email done: X migrated
        const emailMatch = line.match(/Email done:\s*(\d+)\s*migrated/);
        if (emailMatch) {
          fixed = parseInt(emailMatch[1]);
        }

        // Fatal error: mailbox not found
        if (line.includes('inactive, soft-deleted') || line.includes('MailboxNotFound') || line.includes('MailboxNotEnabledForRESTAPI')) {
          fatalError = 'Mailbox not found or inactive in target';
        }

        // TOTAL: X source / Y target — Z missing (NN.N% match)
        const verifyMatch = line.match(/TOTAL:\s*(\d+)\s*source\s*\/\s*(\d+)\s*target.*?(\d+)\s*missing\s*\((\d+\.?\d*)%/);
        if (verifyMatch) {
          totalSource = parseInt(verifyMatch[1]);
          totalTarget = parseInt(verifyMatch[2]);
          totalMissing = parseInt(verifyMatch[3]);
          matchPercent = parseFloat(verifyMatch[4]);
        }
      }
    };

    child.stdout.on('data', handleData);
    child.stderr.on('data', handleData);

    child.on('close', (code) => {
      resolve({
        ok: code === 0,
        exitCode: code,
        fixed,
        failed,
        matchPercent,
        totalSource,
        totalTarget,
        totalMissing,
        fatalError,
        output
      });
    });

    child.on('error', (err) => {
      resolve({ ok: false, error: err.message, fixed: 0, failed: 0, matchPercent: null, output });
    });
  });
}

// ── Process one user ─────────────────────────────────────────────────────────
async function processUser(user) {
  const email = user.sourceEmail;

  log(`👤 ${user.sourceEmail} → ${user.targetEmail}`);

  let lastMatch = 0;
  let totalFixed = 0;
  let totalFailed = 0;
  let success = false;

  for (let attempt = 1; attempt <= MAX_RETRIES_ARG; attempt++) {
    log(`🤖 Agent: === Attempt ${attempt}/${MAX_RETRIES_ARG} ===`);

    for (let si = 0; si < STEPS.length; si++) {
      const step = STEPS[si];
      log(`🤖 Agent: Step ${si + 1}/${STEPS.length} — ${step.label}`);

      const result = await runStep(step, email);

      // Fatal error: mailbox not found — skip immediately, no retries
      if (result.fatalError) {
        log(`🤖 Agent: ⛔ FATAL: ${result.fatalError} — skipping user (no retry)`);
        return { success: false, matchPercent: 0, totalFixed: 0, totalFailed: 1, attempts: attempt, fatalError: result.fatalError };
      }

      if (!result.ok && step.name === 'migrate') {
        log(`🤖 Agent: ❌ Migration FAILED (exit ${result.exitCode}) — aborting this user`);
        totalFailed++;
        return { success: false, matchPercent: 0, totalFixed, totalFailed, attempts: attempt };
      }

      if (step.name !== 'verify') {
        totalFixed += result.fixed || 0;
        totalFailed += result.failed || 0;
        log(`🤖 Agent: ${step.label} done — ${result.fixed || 0} ok, ${result.failed || 0} errors`);
      }

      // Verify step — check match
      if (step.name === 'verify') {
        lastMatch = result.matchPercent || 0;
        log(`🤖 Agent: 📊 Verification: ${result.totalSource} source / ${result.totalTarget} target — ${result.totalMissing} missing (${lastMatch}% match)`);

        if (lastMatch >= MATCH_THRESHOLD) {
          log(`🤖 Agent: ✅ Match ${lastMatch}% >= ${MATCH_THRESHOLD}% — SUCCESS`);
          success = true;
          break;
        } else if (attempt < MAX_RETRIES_ARG) {
          log(`🤖 Agent: ⚠️ Match ${lastMatch}% < ${MATCH_THRESHOLD}% — will retry (attempt ${attempt + 1}/${MAX_RETRIES_ARG})`);
        } else {
          log(`🤖 Agent: ❌ Match ${lastMatch}% < ${MATCH_THRESHOLD}% after ${MAX_RETRIES_ARG} attempts — marking incomplete`);
        }
      }
    }

    if (success) break;
  }

  return { success, matchPercent: lastMatch, totalFixed, totalFailed, attempts: success ? undefined : MAX_RETRIES_ARG };
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  log(chalk.cyan('🤖 Migration Agent') + ` — max ${MAX_RETRIES_ARG} retries, threshold ${MATCH_THRESHOLD}%`);
  log(`   Source: ${config.source_tenant.domain}`);
  log(`   Target: ${config.target_tenant.domain}`);

  let users = loadUsers();
  if (ONLY_USER) {
    users = users.filter(u => u.sourceEmail.toLowerCase() === ONLY_USER.toLowerCase());
    if (!users.length) { log(chalk.red(`User not found: ${ONLY_USER}`)); process.exit(1); }
  }
  log(`   Users: ${users.length}${ONLY_USER ? ` (filtered: ${ONLY_USER})` : ''}`);

  const results = [];

  for (let i = 0; i < users.length; i++) {
    const user = users[i];
    log(`\n${'═'.repeat(60)}`);
    log(`🤖 Agent: User ${i + 1}/${users.length}`);

    const result = await processUser(user);
    results.push({ email: user.sourceEmail, ...result });

    if (result.success) {
      log(`🤖 Agent: ✅ ${user.sourceEmail} — COMPLETE (${result.matchPercent}% match)`);
    } else {
      log(`🤖 Agent: ❌ ${user.sourceEmail} — INCOMPLETE (${result.matchPercent}% match after ${result.attempts} attempts)`);
    }

    // Dashboard-compatible summary
    log(`\nUser done: ${result.totalFixed} fixed, ${result.totalFailed} failed`);

    if (i < users.length - 1) {
      log(`\n⏳ Waiting 60s before next user (quota recovery)...`);
      await new Promise(r => setTimeout(r, 60000));
    }
  }

  // Final report
  log(`\n${'═'.repeat(60)}`);
  log(chalk.cyan('🤖 Agent: FINAL REPORT'));
  log(`${'═'.repeat(60)}`);

  const ok = results.filter(r => r.success);
  const fail = results.filter(r => !r.success);

  for (const r of results) {
    const icon = r.success ? '✅' : '❌';
    log(`   ${icon} ${r.email} — ${r.matchPercent}% match`);
  }

  log(`\n   ✅ ${ok.length} complete, ❌ ${fail.length} incomplete out of ${results.length} users`);

  if (fail.length > 0) {
    log(chalk.yellow(`\n   ⚠️  Incomplete users need manual review:`));
    for (const r of fail) {
      log(`      - ${r.email} (${r.matchPercent}%)`);
    }
  }
}

main().catch(err => {
  console.error(chalk.red('Fatal:'), err.message);
  process.exit(1);
});
