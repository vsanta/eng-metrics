// server.js
const express = require('express');
const bodyParser = require('body-parser');
const path = require('path');

const apiRoutes = require('./router');
const db = require('./db');
const service = require('./services');
const app = express();
const PORT = 3000;

// Initialize the DuckDB database with an absolute path to ensure consistency
// Use the file in the project root as it appears to be where data is currently stored
const dbPath = path.join(path.dirname(__dirname), 'git_influence.db');
console.log(`Initializing database at: ${dbPath}`);
db.initDB(dbPath);
// Create database tables if they don't exist
db.createTables();

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
// Middleware to parse POST data
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

app.get('/', async (req, res) => {
    try {
        console.log("Loading dashboard with previous analyses");
        const previousAnalyses = await service.getPreviousAnalyses();
        
        // Normalize field names for the UI - support both old and new schema
        const normalizedAnalyses = previousAnalyses.map(analysis => {
            // Handle different date field names
            if (!analysis.start_date && analysis.since_date) {
                analysis.start_date = analysis.since_date;
            }
            
            // Calculate an end date from years if needed
            if (!analysis.end_date && analysis.years) {
                const startDate = new Date(analysis.start_date || analysis.since_date);
                const yearsToAdd = parseFloat(analysis.years) || 0;
                const endDate = new Date(startDate);
                endDate.setFullYear(endDate.getFullYear() + Math.floor(yearsToAdd));
                endDate.setMonth(endDate.getMonth() + Math.floor((yearsToAdd % 1) * 12));
                analysis.end_date = endDate.toISOString().slice(0, 10);
            }
            
            return analysis;
        });
        
        console.log(`Found ${normalizedAnalyses.length} previous analyses`);
        res.render('index', { previousAnalyses: normalizedAnalyses });
    } catch (error) {
        console.error('Error getting previous analyses:', error);
        res.render('index', { previousAnalyses: [] });
    }
});

app.get('/results/:analysisKey', async (req, res) => {
    try {
        const { analysisKey } = req.params;
        console.log(`Fetching results for analysis key: ${analysisKey}`);
        
        const analysisDetails = await service.getAnalysisByKey(analysisKey);
        console.log(`Analysis details lookup result:`, analysisDetails);
        
        if (!analysisDetails) {
            console.error(`Analysis not found for key: ${analysisKey}`);
            return res.status(404).send(`
                <h1>Analysis Not Found</h1>
                <p>The analysis with key "${analysisKey}" was not found in the database.</p>
                <p>This could be due to:</p>
                <ul>
                    <li>The analysis was never completed</li>
                    <li>The database file location has changed</li>
                    <li>The application was restarted with a new database</li>
                </ul>
                <p><a href="/">Return to the dashboard</a> to start a new analysis.</p>
            `);
        }
        
        console.log(`Fetching contributor data for analysis key: ${analysisKey}`);
        const contributors = await service.getContributors(analysisKey);
        const topInfluencers = await service.getTopInfluencers(analysisKey) || [];
        const networkData = await service.getCollaborationNetwork(analysisKey) || [];
        
        // Calculate some basic stats for the header
        const totalCreates = contributors.reduce((sum, contributor) => sum + Number(contributor.creates || 0), 0);
        const totalEdits = contributors.reduce((sum, contributor) => sum + Number(contributor.edits || 0), 0);
        
        // Convert any BigInt values to Numbers
        const cleanContributors = convertBigInts(contributors);
        const cleanTopInfluencers = convertBigInts(topInfluencers);
        const cleanNetworkData = convertBigInts(networkData);
        
        console.log(`Rendering results with ${contributors.length} contributors and ${topInfluencers.length} influencers`);
        
        res.render('results', {
            analysisDetails,
            analysisKey,
            totalCreates,
            totalEdits,
            contributors: cleanContributors,
            topInfluencers: cleanTopInfluencers,
            networkData: cleanNetworkData
        });
    } catch (error) {
        console.error('Error rendering results:', error);
        res.status(500).send(`
            <h1>Error Displaying Results</h1>
            <p>An error occurred while trying to display the results:</p>
            <pre>${error.toString()}</pre>
            <p><a href="/">Return to the dashboard</a> to try again.</p>
        `);
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
