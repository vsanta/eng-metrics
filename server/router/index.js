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
        res.json(networkData);
    } catch (error) {
        console.error('Error fetching collaboration network:', error);
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

        const topInfluencers = await service.getTopInfluencers(analysis.analysisKey);

        const topCreators = await service.getTopCreators(analysis.analysisKey);
        const topEditors = await service.getTopEditors(analysis.analysisKey);
        const topEditedCreators = await service.getTopEditedCreators(analysis.analysisKey);
        
        // Get collaboration network data
        const networkData = await service.getCollaborationNetwork(analysis.analysisKey);
        const cleanNetworkData = convertBigInts(networkData);

        res.render('results', {
            totalCreates: analysis.totalCreates,
            totalEdits: analysis.totalEdits,
            sinceDate: analysis.sinceDate,
            analysisKey: analysis.analysisKey,
            contributors: cleanContributors,
            topInfluencers: topInfluencers,
            topCreators: topCreators,
            topEditors: topEditors,
            topEditedCreators: topEditedCreators,
            networkData: cleanNetworkData
        });
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
