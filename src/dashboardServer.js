'use strict';

const express    = require('express');
const path       = require('path');
const fs         = require('fs');
const { spawn }  = require('child_process');

const app  = express();
const PORT = process.env.PORT || 3001;

// ── State ────────────────────────────────────────────────────────────────────
const sseClients = new Set();
let logBuffer    = [];
const MAX_LOG_BUFFER = 500;

// Job queue: { id, task, users[], status, currentUser, currentUserIdx, userResults{}, startedAt }
let activeJob = null;
let runningProcess = null;
let cancelled = false;
let jobHistory = [];  // last 20 finished jobs

// ── SSE helpers ──────────────────────────────────────────────────────────────
function broadcast(event, data) {
  const payload = `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const res of sseClients) {
    try { res.write(payload); } catch { sseClients.delete(res); }
  }
}

// ── Log line parser → structured events ──────────────────────────────────────
function parseLine(raw) {
  const line = raw.toString().trim();
  if (!line) return null;

  logBuffer.push(line);
  if (logBuffer.length > MAX_LOG_BUFFER) logBuffer.shift();

  const ev = { raw: line, ts: new Date().toISOString() };

  // User start
  const userMatch = line.match(/👤\s*(?:\[(\d+)\/(\d+)\]\s*)?(\S+)\s*→\s*(\S+)/);
  if (userMatch) {
    ev.type = 'user_start';
    ev.sourceEmail = userMatch[3];
    ev.targetEmail = userMatch[4];
    return ev;
  }

  // Folder scan
  const folderMatch = line.match(/📁\s*(.+?):\s*scanning/);
  if (folderMatch) { ev.type = 'folder_start'; ev.folder = folderMatch[1].trim(); return ev; }

  if (line.includes('📂 Loading folders')) { ev.type = 'loading_folders'; return ev; }

  const foldersMatch = line.match(/✓\s*(\d+)\s*source folders.*?(\d+)\s*target folders/);
  if (foldersMatch) {
    ev.type = 'folders_loaded';
    ev.sourceFolders = parseInt(foldersMatch[1]);
    ev.targetFolders = parseInt(foldersMatch[2]);
    return ev;
  }

  // Phase
  const phaseMatch = line.match(/Phase\s*(\d+)\/(\d+)/);
  if (phaseMatch) { ev.type = 'phase'; ev.phase = parseInt(phaseMatch[1]); ev.phaseTotal = parseInt(phaseMatch[2]); return ev; }

  // Batch progress
  const batchMatch = line.match(/\[(\d+)\/(\d+)\]\s*(\d+)%/);
  if (batchMatch) { ev.type = 'progress'; ev.current = parseInt(batchMatch[1]); ev.total = parseInt(batchMatch[2]); ev.percent = parseInt(batchMatch[3]); return ev; }

  // Chunk
  const chunkMatch = line.match(/Chunk\s*(\d+)\/(\d+)\s*\((\d+)\s*msgs?\)/);
  if (chunkMatch) { ev.type = 'chunk'; ev.chunk = parseInt(chunkMatch[1]); ev.chunkTotal = parseInt(chunkMatch[2]); return ev; }

  // Chunk done
  const chunkDoneMatch = line.match(/✓\s*Chunk\s*(\d+):\s*(\d+)\s*created.*?(\d+)\s*failed/);
  if (chunkDoneMatch) { ev.type = 'chunk_done'; ev.created = parseInt(chunkDoneMatch[2]); ev.failed = parseInt(chunkDoneMatch[3]); return ev; }

  // Missing attachments
  const missingMatch = line.match(/Found\s*(\d+)\s*messages?\s*missing/i);
  if (missingMatch) { ev.type = 'missing_found'; ev.count = parseInt(missingMatch[1]); return ev; }

  // Check progress
  const checkMatch = line.match(/checked:(\d+)\s*needFix:(\d+)/);
  if (checkMatch) { ev.type = 'check_progress'; ev.checked = parseInt(checkMatch[1]); ev.needFix = parseInt(checkMatch[2]); return ev; }

  // User done
  const userDoneMatch = line.match(/User done:\s*(\d+)\s*fixed.*?(\d+)\s*failed/);
  if (userDoneMatch) { ev.type = 'user_done'; ev.fixed = parseInt(userDoneMatch[1]); ev.failed = parseInt(userDoneMatch[2]); return ev; }

  const emailDoneMatch = line.match(/Email done:\s*(\d+)\s*migrated/);
  if (emailDoneMatch) { ev.type = 'user_done'; ev.fixed = parseInt(emailDoneMatch[1]); ev.failed = 0; return ev; }

  // Drafts: N drafts in X folders
  const draftsMatch = line.match(/(\d+)\s*drafts?\s*in\s*(\d+)\s*folders?/);
  if (draftsMatch) { ev.type = 'drafts_found'; ev.drafts = parseInt(draftsMatch[1]); ev.folders = parseInt(draftsMatch[2]); return ev; }

  // Phase done:  ✓ Phase 3: X created, Y failed
  const phaseDoneMatch = line.match(/✓\s*Phase\s*\d+:\s*(\d+)\s*(?:created|deleted).*?(\d+)\s*failed/);
  if (phaseDoneMatch) { ev.type = 'phase_done'; ev.ok = parseInt(phaseDoneMatch[1]); ev.failed = parseInt(phaseDoneMatch[2]); return ev; }

  if (line.includes('[ERROR]') || line.includes('✗')) { ev.type = 'error'; return ev; }
  if (line.includes('[WARN]') || line.includes('⏸️')) { ev.type = 'warn'; return ev; }
  if (line.includes('All migrations completed') || line.includes('All users processed') || line.includes('Done!')) { ev.type = 'completed'; return ev; }

  ev.type = 'log';
  return ev;
}

// ── Script mapping ───────────────────────────────────────────────────────────
const SCRIPTS = {
  'migrate':         { script: 'migrator.js',       baseArgs: ['--workload', 'email'] },
  'fix-drafts':      { script: 'fixDrafts.js',      baseArgs: [] },
  'fix-attachments': { script: 'fixAttachments.js',  baseArgs: [] },
};

// ── Run one user for a task ──────────────────────────────────────────────────
function spawnForUser(task, userEmail) {
  return new Promise((resolve) => {
    const cfg = SCRIPTS[task];
    if (!cfg) return resolve({ ok: false, error: 'Unknown task' });

    const args = [path.join(__dirname, cfg.script), ...cfg.baseArgs, '--user', userEmail];

    const child = spawn('node', args, {
      cwd: path.resolve(__dirname, '..'),
      env: { ...process.env, FORCE_COLOR: '0', TZ: 'America/Sao_Paulo' },
      stdio: ['ignore', 'pipe', 'pipe']
    });

    runningProcess = child;
    let lastFolder = '';
    let fixed = 0, failed = 0;

    const handleData = (data) => {
      const lines = data.toString().split('\n');
      for (const line of lines) {
        const ev = parseLine(line);
        if (!ev) continue;
        ev.jobUser = userEmail;
        broadcast('event', ev);

        // Track per-user progress
        if (ev.type === 'folder_start') lastFolder = ev.folder;
        if (ev.type === 'user_done') { fixed = ev.fixed || 0; failed = ev.failed || 0; }
        if (ev.type === 'chunk_done') { fixed += ev.created || 0; failed += ev.failed || 0; }
      }
    };

    child.stdout.on('data', handleData);
    child.stderr.on('data', handleData);

    child.on('close', (code) => {
      runningProcess = null;
      resolve({ ok: code === 0, exitCode: code, fixed, failed, lastFolder });
    });

    child.on('error', (err) => {
      runningProcess = null;
      resolve({ ok: false, error: err.message, fixed: 0, failed: 0 });
    });
  });
}

// ── Process job queue: run users one by one ──────────────────────────────────
async function processJob(job) {
  activeJob = job;
  cancelled = false;
  logBuffer = [];

  broadcast('job', { type: 'job_start', jobId: job.id, task: job.task, users: job.users.map(u => u.sourceEmail) });

  for (let i = 0; i < job.users.length; i++) {
    if (cancelled) {
      // Mark remaining as cancelled
      for (let j = i; j < job.users.length; j++) {
        const email = job.users[j].sourceEmail;
        job.userResults[email] = { status: 'cancelled' };
      }
      break;
    }

    const user = job.users[i];
    const email = user.sourceEmail;
    job.currentUserIdx = i;
    job.currentUser = email;
    job.userResults[email] = { status: 'running', folder: '', fixed: 0, failed: 0 };

    broadcast('job', {
      type: 'user_running', jobId: job.id, email,
      userIdx: i, userTotal: job.users.length
    });

    const result = await spawnForUser(job.task, email);

    const status = cancelled ? 'cancelled' : result.ok ? 'done' : 'error';
    job.userResults[email] = {
      status,
      fixed: result.fixed || 0,
      failed: result.failed || 0,
      exitCode: result.exitCode
    };

    broadcast('job', {
      type: 'user_finished', jobId: job.id, email, status,
      fixed: result.fixed, failed: result.failed,
      userIdx: i, userTotal: job.users.length
    });
  }

  job.status = cancelled ? 'cancelled' : 'done';
  job.finishedAt = new Date().toISOString();

  broadcast('job', { type: 'job_finished', jobId: job.id, task: job.task, status: job.status, results: job.userResults });

  // Archive
  jobHistory.unshift({ ...job });
  if (jobHistory.length > 20) jobHistory.pop();
  activeJob = null;
}

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(express.json());
app.use(express.static(path.join(__dirname, 'dashboard')));

// ── API Routes ───────────────────────────────────────────────────────────────

// SSE stream
app.get('/api/events', (req, res) => {
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no'
  });

  // Send current state
  const state = { activeJob: activeJob ? {
    id: activeJob.id, task: activeJob.task,
    users: activeJob.users.map(u => u.sourceEmail),
    currentUser: activeJob.currentUser,
    currentUserIdx: activeJob.currentUserIdx,
    userResults: activeJob.userResults
  } : null };
  res.write(`event: connected\ndata: ${JSON.stringify(state)}\n\n`);

  // Replay recent logs
  for (const line of logBuffer.slice(-50)) {
    const ev = { type: 'log', raw: line, ts: new Date().toISOString(), replay: true };
    res.write(`event: event\ndata: ${JSON.stringify(ev)}\n\n`);
  }

  sseClients.add(res);
  req.on('close', () => sseClients.delete(res));
});

// Status
app.get('/api/status', (req, res) => {
  res.json({
    running: !!activeJob,
    job: activeJob ? {
      id: activeJob.id, task: activeJob.task,
      users: activeJob.users.map(u => u.sourceEmail),
      currentUser: activeJob.currentUser,
      currentUserIdx: activeJob.currentUserIdx,
      userResults: activeJob.userResults,
      startedAt: activeJob.startedAt
    } : null,
    history: jobHistory.slice(0, 5)
  });
});

// Users list
app.get('/api/users', (req, res) => {
  try {
    const users = readUsersCSV();
    res.json({ users });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Start a job: { task: 'migrate'|'fix-drafts'|'fix-attachments', users: ['email1','email2'] }
app.post('/api/jobs', (req, res) => {
  if (activeJob) return res.status(409).json({ error: `Job already running: ${activeJob.task}` });

  const { task, users: selectedEmails } = req.body;
  if (!SCRIPTS[task]) return res.status(400).json({ error: 'Invalid task' });
  if (!selectedEmails || !selectedEmails.length) return res.status(400).json({ error: 'No users selected' });

  const allUsers = readUsersCSV();
  const jobUsers = selectedEmails.map(email => allUsers.find(u => u.sourceEmail === email)).filter(Boolean);
  if (!jobUsers.length) return res.status(400).json({ error: 'No valid users found' });

  const job = {
    id: Date.now().toString(36),
    task,
    users: jobUsers,
    status: 'running',
    currentUser: null,
    currentUserIdx: 0,
    userResults: {},
    startedAt: new Date().toISOString(),
    finishedAt: null
  };

  // Start async processing
  processJob(job);

  res.json({ started: true, jobId: job.id, task, users: jobUsers.map(u => u.sourceEmail) });
});

// Cancel active job
app.post('/api/jobs/cancel', (req, res) => {
  if (!activeJob) return res.status(404).json({ error: 'No job running' });
  cancelled = true;
  if (runningProcess) runningProcess.kill('SIGTERM');
  res.json({ cancelled: true, jobId: activeJob.id });
});

// Job history
app.get('/api/jobs/history', (req, res) => {
  res.json({ history: jobHistory });
});

// Log tail
app.get('/api/logs', (req, res) => {
  const n = Math.min(parseInt(req.query.n || '100'), MAX_LOG_BUFFER);
  res.json({ lines: logBuffer.slice(-n) });
});

// ── Users CRUD ───────────────────────────────────────────────────────────────
const CSV_PATH = path.resolve(__dirname, '..', 'users.csv');

function readUsersCSV() {
  if (!fs.existsSync(CSV_PATH)) return [];
  const csv = fs.readFileSync(CSV_PATH, 'utf8');
  const lines = csv.trim().split('\n');
  const users = [];
  for (let i = 1; i < lines.length; i++) {
    const [sourceEmail, targetEmail, displayName] = lines[i].split(',');
    if (sourceEmail && targetEmail) {
      users.push({ sourceEmail: sourceEmail.trim(), targetEmail: targetEmail.trim(), displayName: (displayName || '').trim() });
    }
  }
  return users;
}

function writeUsersCSV(users) {
  const header = 'sourceEmail,targetEmail,displayName';
  const lines = users.map(u => `${u.sourceEmail},${u.targetEmail},${u.displayName || ''}`);
  fs.writeFileSync(CSV_PATH, [header, ...lines].join('\n') + '\n');
}

app.post('/api/users', (req, res) => {
  const { sourceEmail, targetEmail, displayName } = req.body;
  if (!sourceEmail || !targetEmail) return res.status(400).json({ error: 'sourceEmail and targetEmail required' });
  const users = readUsersCSV();
  if (users.find(u => u.sourceEmail === sourceEmail)) return res.status(409).json({ error: 'User already exists' });
  users.push({ sourceEmail, targetEmail, displayName: displayName || '' });
  writeUsersCSV(users);
  res.json({ ok: true, users });
});

app.delete('/api/users/:email', (req, res) => {
  let users = readUsersCSV();
  const before = users.length;
  users = users.filter(u => u.sourceEmail !== req.params.email);
  if (users.length === before) return res.status(404).json({ error: 'User not found' });
  writeUsersCSV(users);
  res.json({ ok: true, users });
});

app.get('/api/users/:email/log', (req, res) => {
  const logName = req.params.email.replace('@', '_').replace(/\./g, '_') + '.log';
  const logPath = path.resolve(__dirname, '..', 'logs', logName);
  if (!fs.existsSync(logPath)) return res.json({ lines: [] });
  const n = Math.min(parseInt(req.query.n || '200'), 500);
  const content = fs.readFileSync(logPath, 'utf8');
  const lines = content.trim().split('\n').slice(-n);
  res.json({ lines });
});

// ── Start ────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  console.log(`\n  M365 Email Dashboard running on http://localhost:${PORT}\n`);
});
