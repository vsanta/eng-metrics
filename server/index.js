// server.js
const express = require('express');
const bodyParser = require('body-parser');
const path = require('path');

const apiRoutes = require('./router');
const db = require('./db');
const service = require('./services');
const app = express();
const PORT = 3000;

// Initialize the DuckDB database
db.initDB('git_influence.db');
// Create database tables if they don't exist
db.createTables();

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
// Middleware to parse POST data
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

app.get('/', async (req, res) => {
    try {
        const previousAnalyses = await service.getPreviousAnalyses();
        res.render('index', { previousAnalyses });
    } catch (error) {
        console.error('Error getting previous analyses:', error);
        res.render('index', { previousAnalyses: [] });
    }
});

app.get('/results/:analysisKey', async (req, res) => {
    try {
        const { analysisKey } = req.params;
        const analysisDetails = await service.getAnalysisByKey(analysisKey);
        
        if (!analysisDetails) {
            return res.status(404).send('Analysis not found');
        }
        
        const contributors = await service.getContributors(analysisKey);
        const topInfluencers = await service.getTopInfluencers(analysisKey);
        const topCreators = await service.getTopCreators(analysisKey);
        const topEditors = await service.getTopEditors(analysisKey);
        const topEditedCreators = await service.getTopEditedCreators(analysisKey);
        const networkData = await service.getCollaborationNetwork(analysisKey);
        
        // Convert any BigInt values to Numbers
        const cleanContributors = convertBigInts(contributors);
        const cleanNetworkData = convertBigInts(networkData);
        
        res.render('results', {
            analysisDetails,
            analysisKey,
            contributors: cleanContributors,
            topInfluencers,
            topCreators,
            topEditors,
            topEditedCreators,
            networkData: cleanNetworkData
        });
    } catch (error) {
        console.error('Error rendering results:', error);
        res.status(500).send('Error displaying results: ' + error.toString());
    }
});

// Helper function to convert BigInt values to Numbers
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

app.use('/api', apiRoutes);

// Serve static assets (the front-end)
app.use(express.static(path.join(__dirname, 'public')));

// Healthcheck endpoint at the root (or add additional endpoints as needed)
app.get('/health', (req, res) => {
    res.json({ status: "OK" });
});

app.listen(PORT, () => {
    console.log(`Server running at http://localhost:${PORT}`);
});
