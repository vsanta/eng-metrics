# Engineering Metrics

A tool to analyze Git repositories and generate engineering metrics including:

- **Contributor influence metrics** - Who creates and modifies code
- **File activity metrics** - Which files change most frequently
- **PR lifecycle metrics** - How long PRs take to be merged
- **Collaboration patterns** - How developers work together

## Features

- Analyze multiple repositories in a directory
- Specify date ranges for analysis (custom periods, months, quarters, etc.)
- Label analyses with team/group names
- View previous analyses
- Interactive visualizations with zoom support
- Contributor detail view

## Usage

1. Clone this repository
2. Install dependencies:
   ```bash
   npm install
   ```
3. Run the server:
   ```bash
   node server/index.js
   ```
4. Open your browser to http://localhost:3000

## How it works

The tool analyzes Git repositories by:
1. Finding all repositories in a directory
2. Analyzing commit history within a specified date range
3. Tracking file creation and modification patterns
4. Detecting branch merges to analyze PR lifecycle
5. Building influence scores based on code creation and modification
6. Generating interactive visualizations

## License

MIT
