// db.js
const sqlite3 = require('sqlite3').verbose();
let db;

function initDB(dbFile = ':memory:') {
    db = new sqlite3.Database(dbFile, (err) => {
        if (err) {
            console.error('Error opening DB:', err);
        } else {
            console.log('Connected to SQLite database.');
            createTables();
        }
    });
}

function createTables() {
    // Create the repositories table
    db.run(`
    CREATE TABLE IF NOT EXISTS repositories (
      id INTEGER PRIMARY KEY,
      name TEXT
    )
  `);
    // Create the actions table
    db.run(`
    CREATE TABLE IF NOT EXISTS actions (
      id INTEGER PRIMARY KEY,
      author TEXT,
      action TEXT,
      repository TEXT,
      filename TEXT
    )
  `);
    // Table for file creators
    db.run(`
    CREATE TABLE IF NOT EXISTS file_creators (
      filename TEXT,
      repository TEXT,
      creator TEXT,
      PRIMARY KEY (filename, repository)
    )
  `);
    // Table for edits to files created by others
    db.run(`
    CREATE TABLE IF NOT EXISTS edits_to_creations (
      editor TEXT,
      creator TEXT,
      repository TEXT,
      filename TEXT,
      PRIMARY KEY (editor, creator, repository, filename)
    )
  `);
}

function runQuery(query, params = []) {
    return new Promise((resolve, reject) => {
        db.run(query, params, function (err) {
            if (err) reject(err);
            else resolve(this);
        });
    });
}

function getQuery(query, params = []) {
    return new Promise((resolve, reject) => {
        db.all(query, params, function (err, rows) {
            if (err) reject(err);
            else resolve(rows);
        });
    });
}
function healthcheck() {
    return getQuery("SELECT 1");
}
module.exports = {
    initDB,
    runQuery,
    getQuery,
    healthcheck,
    db, // export the raw db if needed
};
