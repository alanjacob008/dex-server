const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const morgan = require('morgan');
const duckdb = require('duckdb');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(helmet()); // Security headers
app.use(cors()); // Enable CORS
app.use(morgan('combined')); // Logging
app.use(express.json()); // Parse JSON bodies
app.use(express.urlencoded({ extended: true })); // Parse URL-encoded bodies

// MotherDuck (DuckDB) connection
// Expect environment variables:
// - MOTHERDUCK_TOKEN: MotherDuck service token
// - MOTHERDUCK_DATABASE (optional): database path like 'md:mydb' (defaults to md:default)
let motherDuckDb = null;
let motherDuckConnection = null;

function initializeMotherDuckConnection() {
  const motherDuckToken = process.env.MOTHERDUCK_TOKEN;
  const motherDuckDatabase = process.env.MOTHERDUCK_DATABASE || 'md:default';
  if (!motherDuckToken) {
    console.warn('MotherDuck token not set. Set MOTHERDUCK_TOKEN to enable MotherDuck endpoints.');
    return;
  }

  // Configure MotherDuck token for DuckDB. The token is provided via open_config.
  try {
    motherDuckDb = new duckdb.Database(':memory:');
    const conn = motherDuckDb.connect();
    // Ensure motherduck extension is available and loaded, then attach with TOKEN
    const safeToken = String(motherDuckToken).replaceAll("'", "''");
    const safeDb = String(motherDuckDatabase).replaceAll("'", "''");
    conn.run('INSTALL motherduck;');
    conn.run('LOAD motherduck;');
    // Attach and alias as md_db for clarity
    conn.run(`ATTACH '${safeDb}' (TOKEN '${safeToken}') AS md_db;`);
    motherDuckConnection = conn;
    console.log('✅ MotherDuck connection initialized');
  } catch (error) {
    console.error('Failed to initialize MotherDuck connection:', error);
  }
}

initializeMotherDuckConnection();

// Routes
app.get('/', (req, res) => {
  res.json({
    message: 'Welcome to Dex Server API',
    status: 'running',
    timestamp: new Date().toISOString(),
    version: '1.0.0'
  });
});

app.get('/api/hello', (req, res) => {
  res.json({
    message: 'Hello World!',
    status: 'success',
    timestamp: new Date().toISOString()
  });
});

app.get('/api/health', (req, res) => {
  res.json({
    status: 'healthy',
    uptime: process.uptime(),
    timestamp: new Date().toISOString()
  });
});

// MotherDuck routes
app.get('/api/md/tables', async (req, res, next) => {
  try {
    if (!motherDuckConnection) {
      return res.status(503).json({ error: 'MotherDuck not configured. Set MOTHERDUCK_TOKEN.' });
    }
    const schemaFilter = req.query.schema ? `AND table_schema = '${req.query.schema}'` : '';
    // Prefer listing from attached md_db catalog if available
    const sql = `
      SELECT table_catalog, table_schema, table_name
      FROM md_db.information_schema.tables
      WHERE table_type = 'BASE TABLE'
        AND table_schema NOT IN ('information_schema', 'pg_catalog')
        ${schemaFilter}
      ORDER BY table_catalog, table_schema, table_name
    `;
    motherDuckConnection.all(sql, (err, rows) => {
      if (err) {
        // Fallback to default information_schema if md_db prefix fails
        return motherDuckConnection.all(
          `SELECT table_catalog, table_schema, table_name
           FROM information_schema.tables
           WHERE table_type = 'BASE TABLE'
             AND table_schema NOT IN ('information_schema', 'pg_catalog')
           ORDER BY table_catalog, table_schema, table_name`,
          (err2, rows2) => {
            if (err2) return next(err2);
            res.json({ tables: rows2, note: 'Listed from default information_schema (fallback)' });
          }
        );
      }
      res.json({ tables: rows });
    });
  } catch (error) {
    next(error);
  }
});

// Diagnostics to understand what is attached and visible (robust, never throws)
app.get('/api/md/diagnostics', async (req, res) => {
  if (!motherDuckConnection) {
    return res.status(503).json({ error: 'MotherDuck not configured. Set MOTHERDUCK_TOKEN.' });
  }
  const result = { database_list: null, current_database: null, schemas: null, table_count: null, errors: {} };
  // Step 1: PRAGMA database_list
  motherDuckConnection.all('PRAGMA database_list;', (e1, dbList) => {
    if (e1) result.errors.database_list = String(e1.message || e1);
    else result.database_list = dbList;
    // Step 2: current_database()
    motherDuckConnection.all('SELECT current_database() AS current_database;', (e2, currentDb) => {
      if (e2) result.errors.current_database = String(e2.message || e2);
      else result.current_database = currentDb?.[0]?.current_database || null;
      // Step 3: schemata from md_db or fallback
      motherDuckConnection.all('SELECT schema_name FROM md_db.information_schema.schemata ORDER BY schema_name;', (e3, schemas) => {
        if (e3) {
          result.errors.schemata_md_db = String(e3.message || e3);
          return motherDuckConnection.all('SELECT schema_name FROM information_schema.schemata ORDER BY schema_name;', (e3b, schemas2) => {
            if (e3b) result.errors.schemata_default = String(e3b.message || e3b);
            else result.schemas = schemas2;
            // Step 4: table count fallback
            motherDuckConnection.all('SELECT COUNT(*) AS table_count FROM information_schema.tables;', (e4b, count2) => {
              if (e4b) result.errors.table_count_default = String(e4b.message || e4b);
              else result.table_count = count2?.[0]?.table_count ?? null;
              return res.json(result);
            });
          });
        }
        result.schemas = schemas;
        // Step 4: table count in md_db
        motherDuckConnection.all('SELECT COUNT(*) AS table_count FROM md_db.information_schema.tables;', (e4, count) => {
          if (e4) result.errors.table_count_md_db = String(e4.message || e4);
          else result.table_count = count?.[0]?.table_count ?? null;
          return res.json(result);
        });
      });
    });
  });
});

// Simple ping to test connectivity to MotherDuck
app.get('/api/md/ping', (req, res, next) => {
  if (!motherDuckConnection) {
    return res.status(503).json({ error: 'MotherDuck not configured. Set MOTHERDUCK_TOKEN.' });
  }
  motherDuckConnection.all('SELECT 1 AS ok;', (err, rows) => {
    if (err) return next(err);
    res.json({ rows });
  });
});

// Cautious read-only query endpoint. Only allows SELECT statements.
app.post('/api/md/query', async (req, res, next) => {
  try {
    if (!motherDuckConnection) {
      return res.status(503).json({ error: 'MotherDuck not configured. Set MOTHERDUCK_TOKEN.' });
    }
    const { sql } = req.body || {};
    if (!sql || typeof sql !== 'string') {
      return res.status(400).json({ error: 'Missing SQL in body { sql }' });
    }
    const trimmed = sql.trim().toLowerCase();
    if (!trimmed.startsWith('select')) {
      return res.status(400).json({ error: 'Only SELECT queries are allowed.' });
    }
    motherDuckConnection.all(sql, (err, rows) => {
      if (err) return next(err);
      res.json({ rows });
    });
  } catch (error) {
    next(error);
  }
});

// 404 handler
app.use('*', (req, res) => {
  res.status(404).json({
    error: 'Route not found',
    message: `Cannot ${req.method} ${req.originalUrl}`,
    timestamp: new Date().toISOString()
  });
});

// Error handler
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({
    error: 'Something went wrong!',
    message: process.env.NODE_ENV === 'production' ? 'Internal server error' : err.message,
    timestamp: new Date().toISOString()
  });
});

// Start server
app.listen(PORT, () => {
  console.log(`🚀 Server is running on port ${PORT}`);
  console.log(`📍 Local: http://localhost:${PORT}`);
  console.log(`📍 API Health: http://localhost:${PORT}/api/health`);
  console.log(`📍 Hello World: http://localhost:${PORT}/api/hello`);
});

module.exports = app;
