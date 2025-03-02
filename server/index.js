// server.js
const express = require('express');
const bodyParser = require('body-parser');
const path = require('path');

const apiRoutes = require('./router');
const db = require('./db');
const app = express();
const PORT = 3000;

// Initialize the SQLite database (change ':memory:' to a file path if needed)
db.initDB('git_influence.db');

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));
// Middleware to parse POST data
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

app.get('/', (req, res) => {
    res.render('index');
});

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
