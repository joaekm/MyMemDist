# MyMemDist

Distribution repo for **MyMemory** - a personal AI-powered knowledge management system.

This repo contains the runtime files needed to run MyMemory. Development tools, documentation, and tests are in the private main repo.

## Quick Install

```bash
curl -sL https://raw.githubusercontent.com/joaekm/MyMemDist/main/setup_mymemory.py -o setup_mymemory.py
python3 setup_mymemory.py
```

The setup script will:
1. Download the runtime files from this repo
2. Create a Python virtual environment and install dependencies
3. Set up the data directory structure (`~/MyMemory/`)
4. Guide you through API key configuration (Anthropic + Google Gemini)
5. Optionally configure Slack, Gmail, and Calendar integrations
6. Generate your config file and Claude Desktop MCP snippet

## Requirements

- Python 3.12+
- macOS or Linux
- API keys: Anthropic (required), Google Gemini (required)
- Optional: ffmpeg (for audio transcription)

## After Installation

Start the system:
```bash
cd <install-dir>
source venv/bin/activate
python start_services.py
```

## What is MyMemory?

MyMemory is a personal knowledge management system that:
- Collects data from documents, Slack, email, calendar, and audio recordings
- Processes and indexes content using AI (entity extraction, semantic search)
- Stores knowledge in a graph database (DuckDB) and vector database (ChromaDB)
- Exposes everything via MCP (Model Context Protocol) for use with Claude Desktop, Cursor, and other AI tools
