# Mongo CSV Importer

This project automates the import of FTC DNC complaint CSV files into a MongoDB database. It is designed to run continuously in a Docker container, handle large CSV files in batches, and send alerts via Bitrix24 for monitoring.

---

## Features

- Automatically imports `_session.csv` files from the `csv_files` directory.
- Processes files in batches for efficient MongoDB insertion.
- Tracks offsets to resume interrupted imports.
- Sends alerts to Bitrix24 for success or failure reports.
- Logs progress and maintains rotating logs.
- Can run inside Docker with `docker-compose` for easy deployment.
- Supports environment-based configuration for MongoDB connection and batch size.

---

## File Overview

| File | Description |
|------|-------------|
| `main.py` | Main Python script that handles importing CSV data into MongoDB. |
| `entrypoint.sh` | Bash script to run the importer in a loop, sleeping 24 hours between runs. |
| `docker-compose.yml` | Docker Compose setup for running the importer container. |
| `Dockerfile` | Builds a Python environment for the importer. |
| `csv_files/` | Directory to place CSV files to be imported. |
| `logs/` | Directory for logs, offset tracking, and health files. |
| `.env` | Environment variables for configuring MongoDB and importer settings. |

---

## Setup

### 1. Docker (recommended)

```bash
docker-compose build
docker-compose up -d
