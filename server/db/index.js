// db.js using DuckDB’s prepared statement API
const duckdb = require('duckdb');
let db;
let connection; // global connection variable

function initDB(dbFile = ':memory:') {
    db = new duckdb.Database(dbFile);
    connection = db.connect();
}

function createTables() {
    // Use the global connection to run table creation statements.
    connection.run(`
    CREATE TABLE IF NOT EXISTS repositories (
      id INTEGER PRIMARY KEY,
      name TEXT,
      analysis_key TEXT
    );
  `, (err) => { if (err) console.error(err); });

    connection.run(`
    CREATE TABLE IF NOT EXISTS actions (
      id INTEGER PRIMARY KEY,
      author TEXT,
      action TEXT,
      repository TEXT,
      filename TEXT,
      analysis_key TEXT
    );
  `, (err) => { if (err) console.error(err); });

    connection.run(`
    CREATE TABLE IF NOT EXISTS file_creators (
      filename TEXT,
      repository TEXT,
      creator TEXT,
      analysis_key TEXT,
      PRIMARY KEY (filename, repository, analysis_key)
    );
  `, (err) => { if (err) console.error(err); });

    connection.run(`
    CREATE TABLE IF NOT EXISTS edits_to_creations (
      editor TEXT,
      creator TEXT,
      repository TEXT,
      filename TEXT,
      analysis_key TEXT,
      PRIMARY KEY (editor, creator, repository, filename, analysis_key)
    );
  `, (err) => { if (err) console.error(err); });
}

async function getQuery(query, params = []) {
    return new Promise((resolve, reject) => {
        connection.all(query, ...params, (err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

async function runQuery(query, params = []) {
    return new Promise((resolve, reject) => {
        connection.run(query, ...params, (err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

module.exports = {
    initDB,
    runQuery,
    getQuery,
    db, // raw db object if needed
};
