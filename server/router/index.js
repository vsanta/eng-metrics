// api.js
const express = require('express');
const service = require('../services');
const db = require('../db');

const router = express.Router();

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
        res.json(details);
    } catch (error) {
        console.error('Error fetching contributor details:', error);
        res.status(500).json({ error: error.toString() });
    }
});
// Endpoint to trigger analysis
router.post('/analyze', async (req, res) => {
    const { localPath, years } = req.body;
    try {
        // Run analysis and get the analysisKey
        const analysis = await service.analyzeAllRepositories(localPath, years || 10);
        // Pass the analysisKey into the contributor query
        const contributors = await service.getContributors(analysis.analysisKey);

        // Convert BigInts to Numbers in your contributors array and other numeric fields
        const cleanContributors = convertBigInts(contributors);

        const result = {
            totalCreates: Number(analysis.totalCreates),
            totalEdits: Number(analysis.totalEdits),
            sinceDate: analysis.sinceDate,
            analysisKey: analysis.analysisKey,
            contributors: cleanContributors,
        };
        res.render('results', { result });
    } catch (error) {
        console.error('Error during analysis:', error);
        res.status(500).send('Error performing analysis: ' + error.toString());
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
