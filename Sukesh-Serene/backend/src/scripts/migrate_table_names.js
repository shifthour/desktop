import Database from 'better-sqlite3';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const dbPath = path.join(__dirname, '../../database.db');
const db = new Database(dbPath);

console.log('🔄 Starting table migration to add serene_ prefix...\n');

// Disable foreign keys temporarily
db.pragma('foreign_keys = OFF');

try {
  const tables = [
    'users',
    'pg_properties',
    'rooms',
    'tenants',
    'payments',
    'expenses',
    'maintenance_requests',
    'payment_submissions'
  ];

  for (const tableName of tables) {
    const newTableName = `serene_${tableName}`;

    // Check if old table exists
    const tableExists = db.prepare(`
      SELECT name FROM sqlite_master WHERE type='table' AND name=?
    `).get(tableName);

    if (tableExists) {
      console.log(`✓ Renaming ${tableName} to ${newTableName}...`);
      db.exec(`ALTER TABLE ${tableName} RENAME TO ${newTableName}`);
    } else {
      console.log(`⊘ Table ${tableName} doesn't exist, skipping...`);
    }
  }

  console.log('\n✅ Migration completed successfully!');
  console.log('⚠️  Please restart the backend server.\n');

} catch (error) {
  console.error('❌ Migration failed:', error.message);
  process.exit(1);
} finally {
  // Re-enable foreign keys
  db.pragma('foreign_keys = ON');
  db.close();
}
