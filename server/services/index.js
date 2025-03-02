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

async function getContributorDetails(author, analysisKey) {
    const summaryQuery = `
        SELECT
            COALESCE(CAST(SUM(CASE WHEN action = 'create' THEN 1 ELSE 0 END) AS INTEGER), 0) AS creates,
            COALESCE(CAST(SUM(CASE WHEN action = 'edit' THEN 1 ELSE 0 END) AS INTEGER), 0) AS edits
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
        COALESCE(CAST(SUM(CASE WHEN action = 'create' THEN 1 ELSE 0 END) AS INTEGER), 0) AS creates,
        COALESCE(CAST(SUM(CASE WHEN action = 'edit' THEN 1 ELSE 0 END) AS INTEGER), 0) AS edits
    FROM actions
    WHERE author = ? AND analysis_key = ?
    GROUP BY repository
  `;
    const perRepoRows = await db.getQuery(perRepoQuery, [author, analysisKey]);

    // New influence calculation:
    // influenceScore = edits + creates * (1 + log(1 + editsToCreations))
    // Using natural logarithm (Math.log) here.
    const multiplier = 1 + Math.log(1 + Number(editsToCreations));
    const influenceScore = Number(edits) + (Number(creates) * multiplier);

    return {
        author: author,
        creates: Number(creates),
        edits: Number(edits),
        editsToCreations: Number(editsToCreations),
        influenceScore: Number(influenceScore),
        perRepo: perRepoRows,
    };
}

async function analyzeAllRepositories(localPath, startDate, endDate, label) {
    const analysisKey = generateAnalysisKey(localPath, startDate, endDate);
    
    // Format dates consistently
    const formattedStartDate = new Date(startDate).toISOString().slice(0, 10);
    
    // If endDate is not provided, use current date
    const formattedEndDate = endDate ? 
        new Date(endDate).toISOString().slice(0, 10) : 
        new Date().toISOString().slice(0, 10);
    
    console.log(`Analyzing repositories in ${localPath} from ${formattedStartDate} to ${formattedEndDate} with key: ${analysisKey}, label: ${label}`);

    // Store analysis metadata
    try {
        await db.runQuery(
            `INSERT INTO analyses (analysis_key, label, local_path, start_date, end_date) 
             VALUES (?, ?, ?, ?, ?)`,
            [analysisKey, label, localPath, formattedStartDate, formattedEndDate]
        );
    } catch (error) {
        console.error('Error storing analysis metadata:', error);
        // Continue anyway - this shouldn't block the analysis
    }

    const repos = findRepositories(localPath);
    let totalCreates = 0, totalEdits = 0;
    for (const repo of repos) {
        const { creates, edits } = await analyzeRepository(repo, formattedStartDate, analysisKey, formattedEndDate);
        totalCreates += Number(creates);
        totalEdits += Number(edits);
    }
    return { 
        totalCreates, 
        totalEdits, 
        startDate: formattedStartDate, 
        endDate: formattedEndDate, 
        analysisKey, 
        label 
    };
}


async function analyzeRepository(repoPath, sinceDate, analysisKey) {
    const repoName = path.basename(repoPath);
    console.log(`Analyzing repository: ${repoName} at ${repoPath} key ${analysisKey}`);

    // Check if repository record already exists for this analysisKey
    try {
        const existingRepo = await db.getQuery(
            "SELECT 1 FROM repositories WHERE name = ? AND analysis_key = ?",
            [repoName, analysisKey]
        );
        if (existingRepo.length === 0) {
            await db.runQuery(
                "INSERT INTO repositories (name, analysis_key) VALUES (?, ?)",
                [repoName, analysisKey]
            );
            console.log("Repository insert done");
        } else {
            console.log("Repository record already exists; skipping insert.");
        }
    } catch (error) {
        console.error("Error inserting repository record:", error);
        // Continue anyway - we don't want to fail the entire analysis if this fails
    }

    // Run git commands as before
    const sinceOption = sinceDate ? `--since='${sinceDate}'` : '';
    let output = await runCommand(`git log ${sinceOption} --pretty=format:'%H' -n 1`, repoPath);

    if (!output.trim()) {
        console.log(`No commits found in ${repoName} ${sinceDate ? 'since ' + sinceDate : ''}`);
        return { creates: 0, edits: 0 };
    }

    // Get branch information
    await analyzeBranches(repoPath, repoName, sinceDate, analysisKey);

    // Get more detailed git log with all the data we want to store
    // Format: commit hash, author name, timestamp, and commit message
    const detailedLogOutput = await runCommand(
        `git log ${sinceOption} --pretty=format:'COMMIT%n%H|%an|%at|%s'`, 
        repoPath
    );
    
    // Get file changes with commit hash
    const fileChangesOutput = await runCommand(
        `git log ${sinceOption} --name-status --pretty=format:'COMMIT %H'`, 
        repoPath
    );

    // Parse and store the detailed commit data
    if (detailedLogOutput) {
        const detailedLines = detailedLogOutput.split('\n');
        let currentCommit = null;
        
        for (const line of detailedLines.map(l => l.trim()).filter(Boolean)) {
            if (line === "COMMIT") {
                currentCommit = null;
            } else if (line.includes('|')) {
                const [hash, author, timestamp, message] = line.split('|');
                currentCommit = hash;
                
                // Convert Unix timestamp to ISO datetime
                const date = new Date(parseInt(timestamp) * 1000);
                
                // Insert commit data
                try {
                    await db.runQuery(
                        "INSERT INTO commits (hash, author, timestamp, message, repository, analysis_key) VALUES (?, ?, ?, ?, ?, ?)",
                        [hash, author, date.toISOString(), message, repoName, analysisKey]
                    );
                    
                    // Classify commit type based on commit message
                    const commitType = classifyCommitType(message);
                    if (commitType.type) {
                        await db.runQuery(
                            "INSERT INTO commit_types (commit_hash, type, confidence, analysis_key) VALUES (?, ?, ?, ?)",
                            [hash, commitType.type, commitType.confidence, analysisKey]
                        );
                    }
                } catch (error) {
                    console.error("Error storing commit:", error);
                }
            }
        }
    }
    
    // Parse and store file changes
    if (fileChangesOutput) {
        const fileLines = fileChangesOutput.split('\n');
        let currentCommitHash = null;
        
        // Track file change counts to identify hot files
        const fileChangeCounts = {};
        const fileContributors = {};
        
        for (const line of fileLines.map(l => l.trim()).filter(Boolean)) {
            if (line.startsWith("COMMIT ")) {
                currentCommitHash = line.substring(7);
            } else if (currentCommitHash && /^[AMDRT]/.test(line)) {
                const parts = line.split("\t");
                if (parts.length < 2) continue;
                const status = parts[0][0];
                const filename = parts[parts.length - 1];
                
                // Update hot file tracking
                if (!fileChangeCounts[filename]) {
                    fileChangeCounts[filename] = 0;
                    fileContributors[filename] = new Set();
                }
                fileChangeCounts[filename]++;
                
                // Store the file change
                try {
                    await db.runQuery(
                        "INSERT INTO file_changes (commit_hash, status, filename, repository, analysis_key) VALUES (?, ?, ?, ?, ?)",
                        [currentCommitHash, status, filename, repoName, analysisKey]
                    );
                    
                    // Get the author of this commit for contributor tracking
                    const commitInfo = await db.getQuery(
                        "SELECT author FROM commits WHERE hash = ?",
                        [currentCommitHash]
                    );
                    
                    if (commitInfo.length > 0) {
                        fileContributors[filename].add(commitInfo[0].author);
                    }
                } catch (error) {
                    console.error("Error storing file change:", error);
                }
            }
        }
        
        // Store hot files information
        await updateHotFiles(fileChangeCounts, fileContributors, repoName, analysisKey);
    }

    // Original analysis code for the existing metrics
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
                // Always insert the creation action.
                await db.runQuery(
                    "INSERT INTO actions (author, action, repository, filename, analysis_key) VALUES (?, ?, ?, ?, ?)",
                    [currentAuthor, "create", repoName, filename, analysisKey]
                );
                creatorMap[filename] = currentAuthor;

                // Check for an existing file_creators record.
                const existingFC = await db.getQuery(
                    "SELECT 1 FROM file_creators WHERE filename = ? AND repository = ? AND analysis_key = ?",
                    [filename, repoName, analysisKey]
                );
                if (existingFC.length === 0) {
                    await db.runQuery(
                        "INSERT INTO file_creators (filename, repository, creator, analysis_key) VALUES (?, ?, ?, ?)",
                        [filename, repoName, currentAuthor, analysisKey]
                    );
                }
            } else if (status === "M") {
                await db.runQuery(
                    "INSERT INTO actions (author, action, repository, filename, analysis_key) VALUES (?, ?, ?, ?, ?)",
                    [currentAuthor, "edit", repoName, filename, analysisKey]
                );
                if (creatorMap[filename] && creatorMap[filename] !== currentAuthor) {
                    const existingETC = await db.getQuery(
                        "SELECT 1 FROM edits_to_creations WHERE editor = ? AND creator = ? AND repository = ? AND filename = ? AND analysis_key = ?",
                        [currentAuthor, creatorMap[filename], repoName, filename, analysisKey]
                    );
                    if (existingETC.length === 0) {
                        await db.runQuery(
                            "INSERT INTO edits_to_creations (editor, creator, repository, filename, analysis_key) VALUES (?, ?, ?, ?, ?)",
                            [currentAuthor, creatorMap[filename], repoName, filename, analysisKey]
                        );
                    }
                }
            }
        }
    }

    // Aggregate stats for actions in this repository for the current analysis
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

async function getTopInfluencers(analysisKey) {
    const query = `
    SELECT author,
           total_edits,
           total_creates,
           times_edited_on_created_files,
           total_edits + (total_creates * (CASE WHEN times_edited_on_created_files > 0 THEN times_edited_on_created_files ELSE 1 END)) AS influenceScore
    FROM (
      SELECT a.author,
             COALESCE(CAST(SUM(CASE WHEN a.action = 'create' THEN 1 ELSE 0 END) AS INTEGER)) AS total_creates,
             COALESCE(CAST(SUM(CASE WHEN a.action = 'edit' THEN 1 ELSE 0 END) AS INTEGER)) AS total_edits,
             COALESCE((
               SELECT COUNT(*)
               FROM edits_to_creations etc
               WHERE etc.creator = a.author AND etc.analysis_key = ?
             ), 0) AS times_edited_on_created_files
      FROM actions a
      WHERE a.analysis_key = ?
      GROUP BY a.author
    ) sub
    ORDER BY influenceScore DESC
    LIMIT 20;
  `;
    return await db.getQuery(query, [analysisKey, analysisKey]);
}


async function getContributors(analysisKey) {
    const query = `
        SELECT author,
               COALESCE(CAST(SUM(CASE WHEN action = 'create' THEN 1 ELSE 0 END) AS INTEGER), 0) AS creates,
               COALESCE(CAST(SUM(CASE WHEN action = 'edit' THEN 1 ELSE 0 END) AS INTEGER), 0) AS edits
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
async function getTopCreators(analysisKey) {
    const query = `
    SELECT author,
           COALESCE(CAST(SUM(CASE WHEN action = 'create' THEN 1 ELSE 0 END) AS INTEGER))AS creates
    FROM actions
    WHERE analysis_key = ?
    GROUP BY author
    ORDER BY creates DESC
    LIMIT 20;
  `;
    return await db.getQuery(query, [analysisKey]);
}

async function getTopEditors(analysisKey) {
    const query = `
    SELECT author,
           COALESCE(CAST(SUM(CASE WHEN action = 'edit' THEN 1 ELSE 0 END) AS INTEGER) )AS edits
    FROM actions
    WHERE analysis_key = ?
    GROUP BY author
    ORDER BY edits DESC
    LIMIT 20;
  `;
    return await db.getQuery(query, [analysisKey]);
}
async function getTopEditedCreators(analysisKey) {
    const query = `
    SELECT fc.creator AS author,
           COUNT(*) AS times_edited
    FROM file_creators fc
    JOIN edits_to_creations etc
      ON fc.filename = etc.filename
      AND fc.repository = etc.repository
      AND fc.analysis_key = etc.analysis_key
    WHERE fc.analysis_key = ?
    GROUP BY fc.creator
    ORDER BY times_edited DESC
    LIMIT 20;
  `;
    return await db.getQuery(query, [analysisKey]);
}
async function getInfluenceRank(author, analysisKey) {
    const query = `
    WITH influencer_scores AS (
      SELECT 
        a.author,
        COALESCE(CAST(SUM(CASE WHEN a.action = 'create' THEN 1 ELSE 0 END) AS INTEGER)) AS creates,
        COALESCE(CAST(SUM(CASE WHEN a.action = 'edit' THEN 1 ELSE 0 END) AS INTEGER)) AS edits,
        COALESCE((
          SELECT COUNT(*) 
          FROM edits_to_creations etc 
          WHERE etc.creator = a.author AND etc.analysis_key = ?
        ), 0) AS times_edited,
        RANK() OVER (
          ORDER BY 
            COALESCE(CAST(SUM(CASE WHEN a.action = 'edit' THEN 1 ELSE 0 END) AS INTEGER))
            + COALESCE(CAST(SUM(CASE WHEN a.action = 'create' THEN 1 ELSE 0 END) AS INTEGER)) *
              (CASE WHEN COALESCE((
                 SELECT COUNT(*) 
                 FROM edits_to_creations etc 
                 WHERE etc.creator = a.author AND etc.analysis_key = ?
               ), 0) > 0 THEN COALESCE((
                 SELECT COUNT(*) 
                 FROM edits_to_creations etc 
                 WHERE etc.creator = a.author AND etc.analysis_key = ?
               ), 0) ELSE 1 END)
          DESC
        ) AS influence_rank
      FROM actions a
      WHERE a.analysis_key = ?
      GROUP BY a.author
    )
    SELECT influence_rank 
    FROM influencer_scores 
    WHERE author = ?;
  `;
    // Pass analysisKey three times for the subquery and once for the outer query, then the author.
    const params = [analysisKey, analysisKey, analysisKey, analysisKey, author];
    const rows = await db.getQuery(query, params);
    if (rows.length > 0) return rows[0].influence_rank;
    return null;
}
// Get collaboration network data - who worked on files created by others
async function getCollaborationNetwork(analysisKey) {
    const query = `
    SELECT 
        creator, 
        editor,
        CAST(COUNT(*) AS INTEGER) AS collaboration_count
    FROM edits_to_creations
    WHERE analysis_key = ?
    GROUP BY creator, editor
    ORDER BY collaboration_count DESC
    `;
    
    return await db.getQuery(query, [analysisKey]);
}

// Get commit count per author per day
async function getCommitsByDay(analysisKey) {
    const query = `
    SELECT 
        author,
        strftime('%Y-%m-%d', timestamp) as date,
        CAST(COUNT(*) AS INTEGER) as commit_count
    FROM commits
    WHERE analysis_key = ?
    GROUP BY author, strftime('%Y-%m-%d', timestamp)
    ORDER BY date
    `;

    return await db.getQuery(query, [analysisKey]);
}

// Get file changes by type
async function getFileChangesByType(analysisKey) {
    const query = `
    SELECT 
        status,
        CAST(COUNT(*) AS INTEGER) as change_count
    FROM file_changes
    WHERE analysis_key = ?
    GROUP BY status
    `;

    return await db.getQuery(query, [analysisKey]);
}

// Get file extensions being modified
async function getFileExtensions(analysisKey) {
    const query = `
    WITH extensions AS (
        SELECT 
            CASE 
                WHEN filename LIKE '%.%' THEN 
                    SUBSTR(filename, LENGTH(filename) - POSITION('.' IN REVERSE(filename)) + 2) 
                ELSE 'no-extension'
            END as extension
        FROM file_changes
        WHERE analysis_key = ?
    )
    SELECT 
        extension,
        CAST(COUNT(*) AS INTEGER) as count
    FROM extensions
    GROUP BY extension
    ORDER BY count DESC
    `;

    return await db.getQuery(query, [analysisKey]);
}

// Analyze branch information (PRs, branch lifecycle)
async function analyzeBranches(repoPath, repoName, sinceDate, analysisKey) {
    try {
        console.log(`Analyzing branches for ${repoName}...`);
        
        // Detect the main branch (main or master)
        let mainBranch = "main";
        const checkMaster = await runCommand(`git rev-parse --verify master 2>/dev/null || echo "not found"`, repoPath);
        if (checkMaster.trim() !== "not found") {
            mainBranch = "master";
        }
        console.log(`Detected main branch: ${mainBranch}`);
        
        // Find all merge commits to the main branch
        const sinceOption = sinceDate ? `--since='${sinceDate}'` : '';
        const mergeCommits = await runCommand(
            `git log ${sinceOption} --merges --first-parent ${mainBranch} --pretty=format:"%H|%P|%an|%aI|%s"`, 
            repoPath
        );
        
        if (!mergeCommits.trim()) {
            console.log(`No merge commits found in ${repoName}`);
            return;
        }
        
        const merges = mergeCommits.split('\n').filter(Boolean);
        console.log(`Found ${merges.length} merge commits in ${repoName}`);
        
        for (const merge of merges) {
            // Parse merge commit data
            const [mergeHash, parents, merger, mergeDate, message] = merge.split('|');
            
            // Extract branch name from merge message
            // Usually in format "Merge branch 'feature/x' into main" or "Merge pull request #X from branch"
            const branchNameMatch = message.match(/branch ['"]([^'"]+)['"]/i) || 
                                    message.match(/from ([^/\s]+\/[^\s]+)/i) ||
                                    message.match(/pull request #\d+ from ([^\s]+)/i);
            
            let branchName = branchNameMatch ? branchNameMatch[1] : `unknown-${mergeHash.substring(0, 7)}`;
            
            // Get the commit hash of the branch head (second parent in the merge commit)
            const parentHashes = parents.split(' ');
            if (parentHashes.length < 2) continue; // Not a real merge
            
            const branchHead = parentHashes[1];
            
            // Find the first commit of the branch
            // This is an approximation - we find the first commit reachable from branch head
            // but not from the main branch parent
            const branchFirstCommit = await runCommand(
                `git rev-list ${branchHead} --not ${parentHashes[0]} --topo-order --reverse | head -1`, 
                repoPath
            );
            
            if (!branchFirstCommit.trim()) continue;
            
            // Get info about the first commit
            const firstCommitInfo = await runCommand(
                `git show --no-patch --format="%an|%aI" ${branchFirstCommit.trim()}`,
                repoPath
            );
            
            if (!firstCommitInfo.trim()) continue;
            
            const [creator, creationDate] = firstCommitInfo.trim().split('|');
            
            console.log(`Found PR: ${branchName} created by ${creator} on ${creationDate}, merged by ${merger} on ${mergeDate}`);
            
            // Store branch information
            await db.runQuery(
                `INSERT INTO branches (name, created_timestamp, merged_timestamp, created_by, merged_by, repository, analysis_key)
                 VALUES (?, ?, ?, ?, ?, ?, ?)`,
                [branchName, new Date(creationDate).toISOString(), new Date(mergeDate).toISOString(), 
                 creator, merger, repoName, analysisKey]
            );
        }
    } catch (error) {
        console.error("Error analyzing branches:", error);
    }
}

// Update hot files tracking
async function updateHotFiles(fileChangeCounts, fileContributors, repoName, analysisKey) {
    try {
        // Get top files by change count
        const topFiles = Object.entries(fileChangeCounts)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 50); // Top 50 files
            
        for (const [filename, changeCount] of topFiles) {
            const contributorCount = fileContributors[filename] ? fileContributors[filename].size : 0;
            
            // Store hot file data
            await db.runQuery(
                `INSERT INTO hot_files (filename, change_count, contributor_count, repository, analysis_key)
                 VALUES (?, ?, ?, ?, ?)`,
                [filename, changeCount, contributorCount, repoName, analysisKey]
            );
        }
    } catch (error) {
        console.error("Error updating hot files:", error);
    }
}

// Classify commit type using heuristics
function classifyCommitType(message) {
    message = message.toLowerCase();
    
    // Keywords for classification
    const patterns = {
        feature: /\b(feature|feat|add|new|implement|support)\b/i,
        bug: /\b(fix|bug|issue|problem|resolve|crash|error)\b/i,
        refactor: /\b(refactor|clean|improve|enhance|update|optimiz|reorganiz|restructur)\b/i,
        docs: /\b(doc|comment|readme|changelog)\b/i,
        test: /\b(test|spec|assert|validate)\b/i,
        chore: /\b(chore|bump|version|release|merge|config)\b/i
    };
    
    // Count matches for each category
    let bestMatch = null;
    let highestConfidence = 0;
    
    for (const [type, regex] of Object.entries(patterns)) {
        const matches = (message.match(regex) || []).length;
        
        if (matches > 0) {
            const confidence = matches / message.split(' ').length;
            if (confidence > highestConfidence) {
                highestConfidence = confidence;
                bestMatch = type;
            }
        }
    }
    
    return {
        type: bestMatch,
        confidence: highestConfidence
    };
}

// Get PR lifecycle metrics
async function getPRLifecycle(analysisKey) {
    console.log(`Getting PR lifecycle data for analysis key: ${analysisKey}`);
    const query = `
    SELECT 
        name as branch_name,
        created_by,
        merged_by,
        created_timestamp,
        merged_timestamp,
        CAST((JULIAN(merged_timestamp) - JULIAN(created_timestamp)) * 24 * 60 AS INTEGER) as lifetime_minutes
    FROM branches
    WHERE analysis_key = ?
      AND merged_timestamp IS NOT NULL
    ORDER BY lifetime_minutes DESC
    `;
    
    return await db.getQuery(query, [analysisKey]);
}

// Get average time to merge PRs
async function getAverageTimeToMerge(analysisKey) {
    console.log(`Getting average time to merge for analysis key: ${analysisKey}`);
    const query = `
    WITH monthly_stats AS (
        SELECT 
            strftime('%Y-%m', merged_timestamp) as month,
            AVG(JULIAN(merged_timestamp) - JULIAN(created_timestamp)) * 24 as avg_hours,
            COUNT(*) as pr_count
        FROM branches
        WHERE analysis_key = ?
          AND merged_timestamp IS NOT NULL
        GROUP BY strftime('%Y-%m', merged_timestamp)
        ORDER BY month
    )
    SELECT
        month,
        CAST(avg_hours AS FLOAT) as avg_hours,
        CAST(pr_count AS INTEGER) as pr_count
    FROM monthly_stats
    `;
    
    try {
        return await db.getQuery(query, [analysisKey]);
    } catch (error) {
        console.error("Error getting average time to merge:", error);
        return [];
    }
}

// Get per-contributor time to merge stats
async function getTimeToMergeByContributor(analysisKey) {
    console.log(`Getting time to merge by contributor for analysis key: ${analysisKey}`);
    const query = `
    WITH contributor_stats AS (
        SELECT 
            created_by as author,
            AVG(JULIAN(merged_timestamp) - JULIAN(created_timestamp)) * 24 as avg_hours,
            COUNT(*) as pr_count
        FROM branches
        WHERE analysis_key = ?
          AND merged_timestamp IS NOT NULL
        GROUP BY created_by
    ),
    team_avg AS (
        SELECT AVG(JULIAN(merged_timestamp) - JULIAN(created_timestamp)) * 24 as team_avg_hours
        FROM branches
        WHERE analysis_key = ?
          AND merged_timestamp IS NOT NULL
    )
    SELECT
        cs.author,
        CAST(cs.avg_hours AS FLOAT) as avg_hours,
        CAST(cs.pr_count AS INTEGER) as pr_count,
        CAST(ta.team_avg_hours AS FLOAT) as team_avg_hours,
        CAST((cs.avg_hours - ta.team_avg_hours) AS FLOAT) as difference
    FROM contributor_stats cs, team_avg ta
    WHERE cs.pr_count >= 2
    ORDER BY cs.avg_hours
    `;
    
    try {
        return await db.getQuery(query, [analysisKey, analysisKey]);
    } catch (error) {
        console.error("Error getting time to merge by contributor:", error);
        return [];
    }
}

// Get hot files (most frequently changed)
async function getHotFiles(analysisKey) {
    const query = `
    SELECT 
        filename,
        CAST(change_count AS INTEGER) as change_count,
        CAST(contributor_count AS INTEGER) as contributor_count
    FROM hot_files
    WHERE analysis_key = ?
    ORDER BY change_count DESC
    LIMIT 20
    `;
    
    return await db.getQuery(query, [analysisKey]);
}

// Get commit types breakdown by author
async function getCommitTypesByAuthor(analysisKey) {
    const query = `
    SELECT 
        c.author,
        ct.type,
        CAST(COUNT(*) AS INTEGER) as count
    FROM commits c
    JOIN commit_types ct ON c.hash = ct.commit_hash
    WHERE c.analysis_key = ?
    GROUP BY c.author, ct.type
    ORDER BY c.author, count DESC
    `;
    
    return await db.getQuery(query, [analysisKey]);
}

// Get list of previous analyses
async function getPreviousAnalyses() {
    console.log("Getting previous analyses list");
    const query = `
    SELECT 
        analysis_key,
        label,
        local_path,
        years,
        created_at,
        since_date
    FROM analyses
    ORDER BY created_at DESC
    `;
    
    try {
        return await db.getQuery(query);
    } catch (error) {
        console.error("Error getting previous analyses:", error);
        return [];
    }
}

// Get analysis by key
async function getAnalysisByKey(analysisKey) {
    console.log(`Getting analysis details for key: ${analysisKey}`);
    const query = `
    SELECT 
        analysis_key,
        label,
        local_path,
        years,
        created_at,
        since_date
    FROM analyses
    WHERE analysis_key = ?
    `;
    
    try {
        const results = await db.getQuery(query, [analysisKey]);
        return results.length > 0 ? results[0] : null;
    } catch (error) {
        console.error(`Error getting analysis details for ${analysisKey}:`, error);
        return null;
    }
}

module.exports = {
    analyzeAllRepositories,
    getContributorDetails,
    analyzeRepository,
    getTopInfluencers,
    getContributors,
    getTopCreators,
    getTopEditors,
    getTopEditedCreators,
    getInfluenceRank,
    getCollaborationNetwork,
    getCommitsByDay,
    getFileChangesByType,
    getFileExtensions,
    getPRLifecycle,
    getAverageTimeToMerge,
    getTimeToMergeByContributor,
    getHotFiles,
    getCommitTypesByAuthor,
    getPreviousAnalyses,
    getAnalysisByKey,
};
