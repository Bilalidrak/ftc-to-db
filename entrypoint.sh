#!/bin/bash

echo "Starting CSV importer loop..."

while true; do
    echo "Starting import at $(date)..."
    
    # Run the Python importer
    python /app/main.py
    
    echo "Import finished at $(date). Sleeping for 24 hours..."
    
    # Sleep for 24 hours (24*60*60 = 86400 seconds)
    sleep 86400
done
