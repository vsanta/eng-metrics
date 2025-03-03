// api.js
const express = require('express');
const service = require('../services');
const db = require('../db');

const router = express.Router();

// Endpoint to get collaboration network data
router.get('/network/:analysisKey', async (req, res) => {
    const { analysisKey } = req.params;
    try {
        const networkData = await service.getCollaborationNetwork(analysisKey);
        
        // Convert any BigInt values to numbers
        const cleanNetworkData = networkData.map(item => {
            const cleanItem = {};
            for (const key in item) {
                cleanItem[key] = typeof item[key] === 'bigint' ? 
                    Number(item[key]) : item[key];
            }
            return cleanItem;
        });
        
        res.json(cleanNetworkData);
    } catch (error) {
        console.error('Error fetching collaboration network:', error);
        res.status(500).json({ error: error.toString() });
    }
});

// Endpoint to get raw commit data
router.get('/commits/:analysisKey', async (req, res) => {
    const { analysisKey } = req.params;
    try {
        const query = `
            SELECT 
                CAST(id AS INTEGER) as id,
                hash,
                author,
                timestamp,
                message,
                repository,
                analysis_key
            FROM commits
            WHERE analysis_key = ?
            ORDER BY timestamp DESC
        `;
        const commits = await db.getQuery(query, [analysisKey]);
        
        // Convert any potential BigInt values to regular numbers
        const cleanCommits = commits.map(commit => {
            const cleanCommit = {};
            for (const key in commit) {
                if (typeof commit[key] === 'bigint') {
                    cleanCommit[key] = Number(commit[key]);
                } else {
                    cleanCommit[key] = commit[key];
                }
            }
            return cleanCommit;
        });
        
        res.json(cleanCommits);
    } catch (error) {
        console.error('Error fetching commits:', error);
        res.status(500).json({ error: error.toString() });
    }
});

// Endpoint to get file changes for a specific commit
router.get('/file-changes/:commitHash', async (req, res) => {
    const { commitHash } = req.params;
    try {
        const query = `
            SELECT 
                CAST(id AS INTEGER) as id,
                commit_hash,
                status,
                filename,
                repository,
                analysis_key
            FROM file_changes
            WHERE commit_hash = ?
        `;
        const fileChanges = await db.getQuery(query, [commitHash]);
        
        // Convert any potential BigInt values to regular numbers
        const cleanFileChanges = fileChanges.map(change => {
            const cleanChange = {};
            for (const key in change) {
                if (typeof change[key] === 'bigint') {
                    cleanChange[key] = Number(change[key]);
                } else {
                    cleanChange[key] = change[key];
                }
            }
            return cleanChange;
        });
        
        // Check if there are any BigInt values left before stringify
        const stringified = JSON.stringify(cleanFileChanges, (key, value) => 
            typeof value === 'bigint' ? value.toString() : value
        );
        
        res.send(stringified);
    } catch (error) {
        console.error('Error fetching file changes:', error);
        res.status(500).json({ error: error.toString() });
    }
});

// New analytics endpoints
router.get('/analytics/commits-by-day/:analysisKey', async (req, res) => {
    const { analysisKey } = req.params;
    try {
        const data = await service.getCommitsByDay(analysisKey);
        
        // Convert any BigInt values to numbers
        const cleanData = data.map(item => {
            const cleanItem = {};
            for (const key in item) {
                if (typeof item[key] === 'bigint') {
                    cleanItem[key] = Number(item[key]);
                } else {
                    cleanItem[key] = item[key];
                }
            }
            return cleanItem;
        });
        
        // Check if there are any BigInt values left in cleanData before stringify
        const stringified = JSON.stringify(cleanData, (key, value) => 
            typeof value === 'bigint' ? value.toString() : value
        );
        
        res.send(stringified);
    } catch (error) {
        console.error('Error fetching commit analytics:', error);
        res.status(500).json({ error: error.toString() });
    }
});

router.get('/analytics/changes-by-type/:analysisKey', async (req, res) => {
    const { analysisKey } = req.params;
    try {
        const data = await service.getFileChangesByType(analysisKey);
        
        // Convert any BigInt values to numbers
        const cleanData = data.map(item => {
            const cleanItem = {};
            for (const key in item) {
                if (typeof item[key] === 'bigint') {
                    cleanItem[key] = Number(item[key]);
                } else {
                    cleanItem[key] = item[key];
                }
            }
            return cleanItem;
        });
        
        // Check if there are any BigInt values left in cleanData before stringify
        const stringified = JSON.stringify(cleanData, (key, value) => 
            typeof value === 'bigint' ? value.toString() : value
        );
        
        res.send(stringified);
    } catch (error) {
        console.error('Error fetching file change analytics:', error);
        res.status(500).json({ error: error.toString() });
    }
});

router.get('/analytics/file-extensions/:analysisKey', async (req, res) => {
    const { analysisKey } = req.params;
    try {
        const data = await service.getFileExtensions(analysisKey);
        
        // Convert any BigInt values to numbers
        const cleanData = data.map(item => {
            const cleanItem = {};
            for (const key in item) {
                if (typeof item[key] === 'bigint') {
                    cleanItem[key] = Number(item[key]);
                } else {
                    cleanItem[key] = item[key];
                }
            }
            return cleanItem;
        });
        
        // Check if there are any BigInt values left in cleanData before stringify
        const stringified = JSON.stringify(cleanData, (key, value) => 
            typeof value === 'bigint' ? value.toString() : value
        );
        
        res.send(stringified);
    } catch (error) {
        console.error('Error fetching file extension analytics:', error);
        res.status(500).json({ error: error.toString() });
    }
});

// PR Lifecycle endpoints
router.get('/analytics/pr-lifecycle/:analysisKey', async (req, res) => {
    const { analysisKey } = req.params;
    try {
        const data = await service.getPRLifecycle(analysisKey);
        
        // Convert any BigInt values to numbers
        const cleanData = data.map(item => {
            const cleanItem = {};
            for (const key in item) {
                if (typeof item[key] === 'bigint') {
                    cleanItem[key] = Number(item[key]);
                } else {
                    cleanItem[key] = item[key];
                }
            }
            return cleanItem;
        });
        
        // Check if there are any BigInt values left before stringify
        const stringified = JSON.stringify(cleanData, (key, value) => 
            typeof value === 'bigint' ? value.toString() : value
        );
        
        res.send(stringified);
    } catch (error) {
        console.error('Error fetching PR lifecycle analytics:', error);
        res.status(500).json({ error: error.toString() });
    }
});

// PR Time to Merge by Month endpoint
router.get('/analytics/pr-time-by-month/:analysisKey', async (req, res) => {
    const { analysisKey } = req.params;
    try {
        console.log(`API: Getting PR time by month for analysis key: ${analysisKey}`);
        const data = await service.getAverageTimeToMerge(analysisKey);
        
        // Convert any BigInt values to numbers
        const cleanData = data.map(item => {
            const cleanItem = {};
            for (const key in item) {
                if (typeof item[key] === 'bigint') {
                    cleanItem[key] = Number(item[key]);
                } else {
                    cleanItem[key] = item[key];
                }
            }
            return cleanItem;
        });
        
        // Check if there are any BigInt values left before stringify
        const stringified = JSON.stringify(cleanData, (key, value) => 
            typeof value === 'bigint' ? value.toString() : value
        );
        
        res.send(stringified);
    } catch (error) {
        console.error('Error fetching PR time by month:', error);
        res.status(500).json({ error: error.toString() });
    }
});

// PR Time to Merge by Contributor endpoint
router.get('/analytics/pr-time-by-contributor/:analysisKey', async (req, res) => {
    const { analysisKey } = req.params;
    try {
        console.log(`API: Getting PR time by contributor for analysis key: ${analysisKey}`);
        const data = await service.getTimeToMergeByContributor(analysisKey);
        
        // Convert any BigInt values to numbers
        const cleanData = data.map(item => {
            const cleanItem = {};
            for (const key in item) {
                if (typeof item[key] === 'bigint') {
                    cleanItem[key] = Number(item[key]);
                } else {
                    cleanItem[key] = item[key];
                }
            }
            return cleanItem;
        });
        
        // Check if there are any BigInt values left before stringify
        const stringified = JSON.stringify(cleanData, (key, value) => 
            typeof value === 'bigint' ? value.toString() : value
        );
        
        res.send(stringified);
    } catch (error) {
        console.error('Error fetching PR time by contributor:', error);
        res.status(500).json({ error: error.toString() });
    }
});

// Hot files (most frequently changed files)
router.get('/analytics/hot-files/:analysisKey', async (req, res) => {
    const { analysisKey } = req.params;
    try {
        const data = await service.getHotFiles(analysisKey);
        
        // Convert any BigInt values to numbers
        const cleanData = data.map(item => {
            const cleanItem = {};
            for (const key in item) {
                if (typeof item[key] === 'bigint') {
                    cleanItem[key] = Number(item[key]);
                } else {
                    cleanItem[key] = item[key];
                }
            }
            return cleanItem;
        });
        
        // Check if there are any BigInt values left before stringify
        const stringified = JSON.stringify(cleanData, (key, value) => 
            typeof value === 'bigint' ? value.toString() : value
        );
        
        res.send(stringified);
    } catch (error) {
        console.error('Error fetching hot files analytics:', error);
        res.status(500).json({ error: error.toString() });
    }
});

// Commit types by author (feature, bug, etc)
router.get('/analytics/commit-types/:analysisKey', async (req, res) => {
    const { analysisKey } = req.params;
    try {
        const data = await service.getCommitTypesByAuthor(analysisKey);
        
        // Convert any BigInt values to numbers
        const cleanData = data.map(item => {
            const cleanItem = {};
            for (const key in item) {
                if (typeof item[key] === 'bigint') {
                    cleanItem[key] = Number(item[key]);
                } else {
                    cleanItem[key] = item[key];
                }
            }
            return cleanItem;
        });
        
        // Check if there are any BigInt values left before stringify
        const stringified = JSON.stringify(cleanData, (key, value) => 
            typeof value === 'bigint' ? value.toString() : value
        );
        
        res.send(stringified);
    } catch (error) {
        console.error('Error fetching commit types analytics:', error);
        res.status(500).json({ error: error.toString() });
    }
});

// New endpoint to fetch detailed contributor info
router.get('/contributor/:author', async (req, res) => {
    const { author } = req.params;
    // Get analysisKey from query parameters
    const { analysisKey } = req.query;
    if (!analysisKey) {
        return res.status(400).json({ error: 'Missing analysisKey' });
    }
    try {
        const details = await service.getContributorDetails(author, analysisKey);
        const influenceRank = await service.getInfluenceRank(author, analysisKey);
        details['influenceRank'] = Number(influenceRank);
        
        // Get PR metrics for this contributor
        try {
            console.log(`Getting PR metrics for contributor: ${author}`);
            const prMetrics = await service.getTimeToMergeByContributor(analysisKey);
            const authorMetrics = prMetrics.find(m => m.author === author);
            if (authorMetrics) {
                details['prMetrics'] = {
                    avgHours: authorMetrics.avg_hours,
                    prCount: authorMetrics.pr_count,
                    teamAvgHours: authorMetrics.team_avg_hours,
                    difference: authorMetrics.difference
                };
            }
        } catch (error) {
            console.error(`Error getting PR metrics for ${author}:`, error);
            // Continue without PR metrics
        }
        
        res.json(details);
    } catch (error) {
        console.error('Error fetching contributor details:', error);
        res.status(500).json({ error: error.toString() });
    }
});
// Endpoint to trigger analysis
router.post('/analyze', async (req, res) => {
    const { localPath, startDate, endDate, label } = req.body;
    try {
        if (!label) {
            return res.status(400).send('Team/Group label is required');
        }
        
        if (!startDate) {
            return res.status(400).send('Start date is required');
        }
        
        console.log(`Starting analysis with: path=${localPath}, startDate=${startDate}, endDate=${endDate}, label=${label}`);
        
        // Verify the path exists before running analysis
        const fs = require('fs');
        const path = require('path');
        const resolvedPath = path.resolve(localPath);
        
        if (!fs.existsSync(resolvedPath)) {
            console.error(`Path does not exist: ${resolvedPath}`);
            return res.status(400).send(`
                <h1>Invalid Path</h1>
                <p>The path "${localPath}" (resolved to "${resolvedPath}") does not exist or is not accessible.</p>
                <p>Please check that the path exists and you have permission to access it.</p>
                <p><a href="/">Return to the dashboard</a> to try again.</p>
            `);
        }
        
        console.log(`Verified path exists: ${resolvedPath}`);
        
        // Run analysis with the date range parameters
        const analysis = await service.analyzeAllRepositories(resolvedPath, startDate, endDate, label);
        
        // Log the analysis key for debugging
        console.log(`Analysis completed successfully. Analysis key: ${analysis.analysisKey}`);
        
        // Redirect to the results page
        res.redirect(`/results/${analysis.analysisKey}`);
    } catch (error) {
        console.error('Error during analysis:', error);
        res.status(500).send(`
            <h1>Analysis Error</h1>
            <p>An error occurred while analyzing the repository:</p>
            <pre style="background-color: #f8f8f8; padding: 10px; border-radius: 5px; overflow-x: auto;">${error.toString()}</pre>
            <p>Please check:</p>
            <ul>
                <li>The path points to a valid Git repository or a directory containing Git repositories</li>
                <li>You have permission to access the repository</li>
                <li>Git is installed and accessible from the command line</li>
            </ul>
            <p><a href="/">Return to the dashboard</a> to try again.</p>
        `);
    }
});
// Healthcheck endpoint
router.get('/health', async (req, res) => {
    try {
        // Simple check: run a test query on the DB
        await db.healthcheck()
        res.json({ status: "OK", db: "Connected" });
    } catch (err) {
        res.status(500).json({ status: "ERROR", error: err.toString() });
    }
});
function convertBigInts(obj) {
    if (typeof obj === 'bigint') {
        return Number(obj);
    } else if (Array.isArray(obj)) {
        return obj.map(convertBigInts);
    } else if (obj !== null && typeof obj === 'object') {
        const newObj = {};
        for (const key in obj) {
            newObj[key] = convertBigInts(obj[key]);
        }
        return newObj;
    }
    return obj;
}

module.exports = router;
