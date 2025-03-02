// api.js
const express = require('express');
const router = express.Router();
const service = require('../services');
const db = require('../db');

// Endpoint to trigger analysis
router.post('/analyze', async (req, res) => {
    const { localPath, years } = req.body;
    try {
        const analysis = await service.analyzeAllRepositories(localPath, years || 10);
        const contributors = await service.getContributors();

        // Build the result context expected by your EJS template.
        const result = {
            totalCreates: analysis.totalCreates,
            totalEdits: analysis.totalEdits,
            sinceDate: analysis.sinceDate,
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
