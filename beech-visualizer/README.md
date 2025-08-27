# Beech Visualizer

A web-based visualizer for Beech prolly trees using React Flow.

## Features

- Visual tree structure representation
- Interactive node exploration
- Real-time tree statistics
- JSON API endpoints for tree data

## Usage

```bash
# Start the server with a data directory
npm start [data-directory]

# Example:
npm start /tmp
```

The server will start on http://localhost:3000

## API Endpoints

- `GET /api/info` - Get transaction and table information
- `GET /api/page/:pageId` - Get details for a specific page
- `GET /api/tree-structure` - Get tree structure formatted for React Flow

## Development

```bash
# Install dependencies
npm install

# Start development server with auto-reload
npm run dev [data-directory]
```

## Requirements

- Node.js
- Built beech-cli binary in ../target/debug/beech-cli
- Beech prolly tree data in the specified directory