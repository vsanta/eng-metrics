// service.js
const { exec } = require('child_process');
const fs = require('fs');
const crypto = require('crypto');
const path = require('path');
const db = require('../db');

function runCommand(cmd, cwd) {
    return new Promise((resolve, reject) => {
        exec(cmd, { cwd, shell: true,  maxBuffer: 1024 * 1024 * 10  }, (error, stdout, stderr) => {
            if (error) {
                console.error(`Error running command: ${cmd} on ${cwd}\n`, stderr);
                console.log(error);
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

async function analyzeRepository(repoPath, sinceDate, analysisKey) {
    const repoName = path.basename(repoPath);
    console.log(`Analyzing repository: ${repoName} at ${repoPath}`);

    // Insert repository record with analysis_key
    await db.runQuery(
        "INSERT INTO repositories (name, analysis_key) VALUES (?, ?)",
        [repoName, analysisKey]
    );

    // Run git commands as before
    const sinceOption = sinceDate ? `--since='${sinceDate}'` : '';
    let output = await runCommand(`git log ${sinceOption} --pretty=format:'%H' -n 1`, repoPath);

    if (!output.trim()) {
        console.log(`No commits found in ${repoName} ${sinceDate ? 'since ' + sinceDate : ''}`);
        return { creates: 0, edits: 0 };
    }

    output = await runCommand(`git log ${sinceOption} --name-status --pretty=format:'COMMIT%n%H %an'`, repoPath);
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
            currentAuthor = line.split(" ", 2)[1];
        } else if (currentAuthor && /^[AMDRT]/.test(line)) {
            const parts = line.split("\t");
            if (parts.length < 2) continue;
            const status = parts[0][0];
            const filename = parts[parts.length - 1];

            if (status === "A") {
                await db.runQuery(
                    "INSERT INTO actions (author, action, repository, filename, analysis_key) VALUES (?, ?, ?, ?, ?)",
                    [currentAuthor, "create", repoName, filename, analysisKey]
                );
                creatorMap[filename] = currentAuthor;
                await db.runQuery(
                    "INSERT OR REPLACE INTO file_creators (filename, repository, creator, analysis_key) VALUES (?, ?, ?, ?)",
                    [filename, repoName, currentAuthor, analysisKey]
                );
            } else if (status === "M") {
                await db.runQuery(
                    "INSERT INTO actions (author, action, repository, filename, analysis_key) VALUES (?, ?, ?, ?, ?)",
                    [currentAuthor, "edit", repoName, filename, analysisKey]
                );
                if (creatorMap[filename] && creatorMap[filename] !== currentAuthor) {
                    await db.runQuery(
                        "INSERT OR IGNORE INTO edits_to_creations (editor, creator, repository, filename, analysis_key) VALUES (?, ?, ?, ?, ?)",
                        [currentAuthor, creatorMap[filename], repoName, filename, analysisKey]
                    );
                }
            }
        }
    }

    const rows = await db.getQuery(
        "SELECT action, COUNT(*) as count FROM actions WHERE repository = ? AND analysis_key = ? GROUP BY action",
        [repoName, analysisKey]
    );
    const stats = rows.reduce((acc, row) => {
        acc[row.action] = row.count;
        return acc;
    }, {});

    console.log(`Repository ${repoName} - Creates: ${stats.create || 0}, Edits: ${stats.edit || 0}`);
    return { creates: stats.create || 0, edits: stats.edit || 0 };
}


async function getContributorDetails(author, analysisKey) {
    const summaryQuery = `
    SELECT 
      COALESCE(SUM(CASE WHEN action = 'create' THEN 1 ELSE 0 END), 0) AS creates,
      COALESCE(SUM(CASE WHEN action = 'edit' THEN 1 ELSE 0 END), 0) AS edits
    FROM actions
    WHERE author = ? AND analysis_key = ?
  `;
    const summaryRows = await db.getQuery(summaryQuery, [author, analysisKey]);
    const { creates, edits } = summaryRows[0] || { creates: 0, edits: 0 };

    // Query for edits on files the contributor created (by others)
    const editsToCreationQuery = `
    SELECT COUNT(*) AS editsToCreations
    FROM edits_to_creations
    WHERE creator = ? AND analysis_key = ?
  `;
    const editsToCreationRows = await db.getQuery(editsToCreationQuery, [author, analysisKey]);
    const { editsToCreations } = editsToCreationRows[0] || { editsToCreations: 0 };

    // Per-repository breakdown
    const perRepoQuery = `
    SELECT repository,
           COALESCE(SUM(CASE WHEN action = 'create' THEN 1 ELSE 0 END), 0) AS creates,
           COALESCE(SUM(CASE WHEN action = 'edit' THEN 1 ELSE 0 END), 0) AS edits
    FROM actions
    WHERE author = ? AND analysis_key = ?
    GROUP BY repository
  `;
    const perRepoRows = await db.getQuery(perRepoQuery, [author, analysisKey]);

    // New influence calculation:
    // influenceScore = edits + creates * (1 + log(1 + editsToCreations))
    // Using natural logarithm (Math.log) here.
    const multiplier = 1 + Math.log(1 + editsToCreations);
    const influenceScore = edits + (creates * multiplier);

    return {
        author,
        creates,
        edits,
        editsToCreations,
        influenceScore,
        perRepo: perRepoRows,
    };
}

async function analyzeAllRepositories(localPath, years) {
    const analysisKey = generateAnalysisKey(localPath, years);
    const sinceDate = new Date(Date.now() - years * 365 * 24 * 3600 * 1000)
        .toISOString()
        .slice(0, 10);
    console.log(`Analyzing repositories in ${localPath} since ${sinceDate} with key: ${analysisKey}`);

    const repos = findRepositories(localPath);
    let totalCreates = 0, totalEdits = 0;
    for (const repo of repos) {
        const { creates, edits } = await analyzeRepository(repo, sinceDate, analysisKey);
        totalCreates += creates;
        totalEdits += edits;
    }
    return { totalCreates, totalEdits, sinceDate, analysisKey };
}

// In service.js (or a separate module if preferred)


async function getContributors(analysisKey) {
    const query = `
        SELECT author,
               SUM(CASE WHEN action = 'create' THEN 1 ELSE 0 END) AS creates,
               SUM(CASE WHEN action = 'edit' THEN 1 ELSE 0 END) AS edits
        FROM actions
        WHERE analysis_key = ?
        GROUP BY author
        ORDER BY (creates + edits) DESC
    `;
    try {
        const rows = await db.getQuery(query, [analysisKey]);
        return rows;
    } catch (err) {
        console.error('Error fetching contributors:', err);
        throw err;
    }
}
/**
 * Generates a unique key for an analysis run, based on the local path and the number of years being analyzed.
 * The key is a hexadecimal MD5 hash of the string "<localPath>-<years>-<current time in ms>".
 * @param {string} localPath - The local path being analyzed.
 * @param {number} years - The number of years being analyzed.
 * @returns {string} A unique key for the analysis run.
 */
function generateAnalysisKey(localPath, years) {
    return crypto
        .createHash('md5')
        .update(`${localPath}-${years}-${Date.now()}`)
        .digest('hex');
}

module.exports = {
    analyzeAllRepositories,
    getContributors,
    getContributorDetails,
    findRepositories,
    analyzeRepository,
    runCommand,
};
