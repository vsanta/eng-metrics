// service.js
const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');
const db = require('../db');

function runCommand(cmd, cwd) {
    return new Promise((resolve, reject) => {
        exec(cmd, { cwd, shell: true }, (error, stdout, stderr) => {
            if (error) {
                console.error(`Error running command: ${cmd}\n`, stderr);
                return resolve(''); // return empty string on error
            }
            resolve(stdout);
        });
    });
}

function findRepositories(startDir) {
    let repositories = [];
    // Check if startDir itself is a git repo
    if (fs.existsSync(path.join(startDir, '.git'))) {
        repositories.push(startDir);
    }
    // Look for child directories that are git repositories
    try {
        fs.readdirSync(startDir).forEach((item) => {
            const childPath = path.join(startDir, item);
            if (fs.statSync(childPath).isDirectory() && fs.existsSync(path.join(childPath, '.git'))) {
                repositories.push(childPath);
            }
        });
    } catch (err) {
        console.error(`Permission denied when accessing ${startDir}`, err);
    }
    return repositories;
}

async function analyzeRepository(repoPath, sinceDate) {
    const repoName = path.basename(repoPath);
    console.log(`Analyzing repository: ${repoName} at ${repoPath}`);

    // Insert repository record
    await db.runQuery("INSERT INTO repositories (name) VALUES (?)", [repoName]);

    // Check for any commits since the specified date
    const sinceOption = sinceDate ? `--since='${sinceDate}'` : '';
    let output = await runCommand(`git log ${sinceOption} --pretty=format:'%H' -n 1`, repoPath);
    if (!output.trim()) {
        console.log(`No commits found in ${repoName} ${sinceDate ? 'since ' + sinceDate : ''}`);
        return { creates: 0, edits: 0 };
    }

    // Get git log with file changes
    const logCmd = `git log ${sinceOption} --name-status --pretty=format:'COMMIT%n%H %an'`;
    output = await runCommand(logCmd, repoPath);
    if (!output) {
        console.log(`No data found in repository ${repoName}`);
        return { creates: 0, edits: 0 };
    }

    let currentAuthor = null;
    const creatorMap = {};

    const lines = output.split('\n');
    for (const line of lines.map(l => l.trim()).filter(Boolean)) {
        if (line.startsWith("COMMIT")) {
            currentAuthor = null;
        } else if (/^[0-9a-f]{7,}/.test(line)) {
            // commit hash line containing the author
            currentAuthor = line.split(" ", 2)[1];
        } else if (currentAuthor && /^[AMDRT]/.test(line)) {
            // status line
            const parts = line.split("\t");
            if (parts.length < 2) continue;
            const status = parts[0][0];
            const filename = parts[parts.length - 1];
            if (status === "A") {
                await db.runQuery(
                    "INSERT INTO actions (author, action, repository, filename) VALUES (?, ?, ?, ?)",
                    [currentAuthor, "create", repoName, filename]
                );
                creatorMap[filename] = currentAuthor;
                await db.runQuery(
                    "INSERT OR REPLACE INTO file_creators (filename, repository, creator) VALUES (?, ?, ?)",
                    [filename, repoName, currentAuthor]
                );
            } else if (status === "M") {
                await db.runQuery(
                    "INSERT INTO actions (author, action, repository, filename) VALUES (?, ?, ?, ?)",
                    [currentAuthor, "edit", repoName, filename]
                );
                // Record edits to creations from other authors
                if (creatorMap[filename] && creatorMap[filename] !== currentAuthor) {
                    await db.runQuery(
                        "INSERT OR IGNORE INTO edits_to_creations (editor, creator, repository, filename) VALUES (?, ?, ?, ?)",
                        [currentAuthor, creatorMap[filename], repoName, filename]
                    );
                }
            }
        }
    }

    // Query to count actions for reporting
    const rows = await db.getQuery(
        "SELECT action, COUNT(*) as count FROM actions WHERE repository = ? GROUP BY action",
        [repoName]
    );
    const stats = rows.reduce((acc, row) => {
        acc[row.action] = row.count;
        return acc;
    }, {});
    console.log(`Repository ${repoName} - Creates: ${stats.create || 0}, Edits: ${stats.edit || 0}`);
    return { creates: stats.create || 0, edits: stats.edit || 0 };
}

async function analyzeAllRepositories(startDir, years) {
    // Calculate the since date (years ago)
    const sinceDate = new Date(Date.now() - years * 365 * 24 * 3600 * 1000)
        .toISOString()
        .slice(0, 10);
    console.log(`Analyzing repositories in ${startDir} since ${sinceDate}`);
    const repos = findRepositories(startDir);
    let totalCreates = 0, totalEdits = 0;
    for (const repo of repos) {
        const { creates, edits } = await analyzeRepository(repo, sinceDate);
        totalCreates += creates;
        totalEdits += edits;
    }
    console.log(`Total: ${totalCreates} creations and ${totalEdits} edits`);
    return { totalCreates, totalEdits, sinceDate };
}

// In service.js (or a separate module if preferred)

async function getContributors() {
    const query = `
    SELECT author,
           SUM(CASE WHEN action = 'create' THEN 1 ELSE 0 END) AS creates,
           SUM(CASE WHEN action = 'edit' THEN 1 ELSE 0 END) AS edits
    FROM actions
    GROUP BY author
    ORDER BY (creates + edits) DESC
  `;
    try {
        const rows = await db.getQuery(query);
        return rows; // Each row will be an object: { author, creates, edits }
    } catch (err) {
        console.error('Error fetching contributors:', err);
        throw err;
    }
}


module.exports = {
    analyzeAllRepositories,
    getContributors,
    findRepositories,
    analyzeRepository,
    runCommand,
};
