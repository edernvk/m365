const fs = require('fs');
const { parse } = require('csv-parse/sync');

function loadUsers(csvPath) {
  if (!fs.existsSync(csvPath)) {
    throw new Error(`Users CSV not found: ${csvPath}`);
  }

  const content = fs.readFileSync(csvPath, 'utf8');
  const records = parse(content, {
    columns: true,
    skip_empty_lines: true,
    trim: true
  });

  const users = records.map(r => ({
    source: r.source_email?.toLowerCase().trim(),
    target: r.target_email?.toLowerCase().trim(),
    displayName: r.display_name?.trim() || r.source_email
  })).filter(u => u.source && u.target);

  if (users.length === 0) {
    throw new Error('No valid users found in CSV. Check format: source_email,target_email,display_name');
  }

  return users;
}

module.exports = { loadUsers };
