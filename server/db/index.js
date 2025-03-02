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
    CREATE SEQUENCE IF NOT EXISTS analysis_id_seq;
    CREATE TABLE IF NOT EXISTS analyses (
      id BIGINT DEFAULT nextval('analysis_id_seq') PRIMARY KEY,
      analysis_key TEXT UNIQUE,
      label TEXT,
      local_path TEXT,
      start_date DATE,
      end_date DATE,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `, (err) => { if (err) console.error(err); });
    
    connection.run(`
    CREATE SEQUENCE IF NOT EXISTS repo_id_seq;
    CREATE TABLE IF NOT EXISTS repositories (
      id BIGINT DEFAULT nextval('repo_id_seq') PRIMARY KEY,
      name TEXT,
      analysis_key TEXT
    );
  `, (err) => { if (err) console.error(err); });

    connection.run(`
    CREATE SEQUENCE IF NOT EXISTS action_id_seq;
    CREATE TABLE IF NOT EXISTS actions (
      id BIGINT DEFAULT nextval('action_id_seq') PRIMARY KEY,
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
  
    // Table for storing raw git log data
    connection.run(`
    CREATE SEQUENCE IF NOT EXISTS commit_id_seq;
    CREATE TABLE IF NOT EXISTS commits (
      id BIGINT DEFAULT nextval('commit_id_seq') PRIMARY KEY,
      hash TEXT,
      author TEXT,
      timestamp TIMESTAMP,
      message TEXT,
      repository TEXT,
      analysis_key TEXT
    );
  `, (err) => { if (err) console.error(err); });
  
    // Table for storing file changes in each commit
    connection.run(`
    CREATE SEQUENCE IF NOT EXISTS file_change_id_seq;
    CREATE TABLE IF NOT EXISTS file_changes (
      id BIGINT DEFAULT nextval('file_change_id_seq') PRIMARY KEY,
      commit_hash TEXT,
      status TEXT,
      filename TEXT,
      repository TEXT,
      analysis_key TEXT
    );
  `, (err) => { if (err) console.error(err); });
  
    // Table for storing branch information
    connection.run(`
    CREATE SEQUENCE IF NOT EXISTS branch_id_seq;
    CREATE TABLE IF NOT EXISTS branches (
      id BIGINT DEFAULT nextval('branch_id_seq') PRIMARY KEY,
      name TEXT,
      created_timestamp TIMESTAMP,
      merged_timestamp TIMESTAMP,
      created_by TEXT,
      merged_by TEXT,
      repository TEXT,
      analysis_key TEXT
    );
  `, (err) => { if (err) console.error(err); });
  
    // Table for tracking hot files (files that change frequently)
    connection.run(`
    CREATE SEQUENCE IF NOT EXISTS hot_file_id_seq;
    CREATE TABLE IF NOT EXISTS hot_files (
      id BIGINT DEFAULT nextval('hot_file_id_seq') PRIMARY KEY,
      filename TEXT,
      change_count INTEGER,
      contributor_count INTEGER,
      repository TEXT,
      analysis_key TEXT
    );
  `, (err) => { if (err) console.error(err); });
  
    // Table for classifying commits by type (feature, bug, refactor)
    connection.run(`
    CREATE SEQUENCE IF NOT EXISTS commit_type_id_seq;
    CREATE TABLE IF NOT EXISTS commit_types (
      id BIGINT DEFAULT nextval('commit_type_id_seq') PRIMARY KEY, 
      commit_hash TEXT,
      type TEXT,
      confidence FLOAT,
      analysis_key TEXT
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

async function healthcheck() {
    return new Promise((resolve, reject) => {
        connection.all("SELECT 1 as test", (err, result) => {
            if (err) return reject(err);
            resolve(result);
        });
    });
}

module.exports = {
    initDB,
    runQuery,
    getQuery,
    healthcheck,
    createTables,
    db, // raw db object if needed
};
