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
    // Repositories table with analysis_key column
    db.run(`
        CREATE TABLE IF NOT EXISTS repositories (
                                                    id INTEGER PRIMARY KEY,
                                                    name TEXT,
                                                    analysis_key TEXT
        )
    `);

    // Actions table with analysis_key column
    db.run(`
        CREATE TABLE IF NOT EXISTS actions (
                                               id INTEGER PRIMARY KEY,
                                               author TEXT,
                                               action TEXT,
                                               repository TEXT,
                                               filename TEXT,
                                               analysis_key TEXT
        )
    `);

    // File creators table with analysis_key and composite primary key
    db.run(`
        CREATE TABLE IF NOT EXISTS file_creators (
                                                     filename TEXT,
                                                     repository TEXT,
                                                     creator TEXT,
                                                     analysis_key TEXT,
                                                     PRIMARY KEY (filename, repository, analysis_key)
            )
    `);

    // Edits to creations table with analysis_key and composite primary key
    db.run(`
        CREATE TABLE IF NOT EXISTS edits_to_creations (
                                                          editor TEXT,
                                                          creator TEXT,
                                                          repository TEXT,
                                                          filename TEXT,
                                                          analysis_key TEXT,
                                                          PRIMARY KEY (editor, creator, repository, filename, analysis_key)
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

module.exports = {
    initDB,
    runQuery,
    getQuery,
    db, // export raw db if needed
};
