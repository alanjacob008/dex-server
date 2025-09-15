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
    // Set token
    conn.run(`SET motherduck_token='${motherDuckToken}';`);
    // Open the MotherDuck database alias
    conn.run(`ATTACH '${motherDuckDatabase}' AS md_db;`);
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

// Diagnostics to understand what is attached and visible
app.get('/api/md/diagnostics', async (req, res, next) => {
  try {
    if (!motherDuckConnection) {
      return res.status(503).json({ error: 'MotherDuck not configured. Set MOTHERDUCK_TOKEN.' });
    }
    motherDuckConnection.all('PRAGMA database_list;', (e1, dbList) => {
      if (e1) return next(e1);
      motherDuckConnection.all('SELECT current_database() AS current_database;', (e2, currentDb) => {
        if (e2) return next(e2);
        motherDuckConnection.all('SELECT schema_name FROM md_db.information_schema.schemata ORDER BY schema_name;', (e3, schemas) => {
          if (e3) {
            // Try fallback schemata without md_db prefix
            return motherDuckConnection.all('SELECT schema_name FROM information_schema.schemata ORDER BY schema_name;', (e3b, schemas2) => {
              if (e3b) return next(e3b);
              motherDuckConnection.all('SELECT COUNT(*) AS table_count FROM information_schema.tables;', (e4b, count2) => {
                if (e4b) return next(e4b);
                res.json({
                  database_list: dbList,
                  current_database: currentDb?.[0]?.current_database,
                  schemas: schemas2,
                  table_count: count2?.[0]?.table_count
                });
              });
            });
          }
          motherDuckConnection.all('SELECT COUNT(*) AS table_count FROM md_db.information_schema.tables;', (e4, count) => {
            if (e4) return next(e4);
            res.json({
              database_list: dbList,
              current_database: currentDb?.[0]?.current_database,
              schemas,
              table_count: count?.[0]?.table_count
            });
          });
        });
      });
    });
  } catch (error) {
    next(error);
  }
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
