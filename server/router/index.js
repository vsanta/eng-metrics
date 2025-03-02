// api.js
const express = require('express');
const router = express.Router();
const service = require('../services');
const db = require('../db');

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
        const analysis = await service.analyzeAllRepositories(localPath, years || 10);
        const contributors = await service.getContributors(analysis.analysisKey);

        // Build the result context expected by your EJS template.
        const result = {
            totalCreates: analysis.totalCreates,
            totalEdits: analysis.totalEdits,
            sinceDate: analysis.sinceDate,
            analysisKey: analysis.analysisKey,
            contributors, // an array of objects with {author, creates, edits}
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

module.exports = router;
