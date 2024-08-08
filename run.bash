# run.bash

#!/bin/bash

# Define the subfolder
SUBFOLDER="data"

# Check if the subfolder exists
if [ -d "$SUBFOLDER" ]; then
    echo "Deleting everything in $SUBFOLDER..."
    rm -rf "$SUBFOLDER"/*
else
    echo "$SUBFOLDER does not exist. Creating it..."
    mkdir "$SUBFOLDER"
fi

# Call the rpsa_client command
echo "Running rpsa_client..."
./rpsa_client -s -h 131.225.130.81 -f csv -l 200000 -d "$SUBFOLDER"

# Find the most recent CSV file in the subfolder
CSV_FILE=$(find "$SUBFOLDER" -maxdepth 1 -type f -name "*.csv" -print0 | xargs -0 ls -t | head -n 1)

# Check if the CSV file was found
if [ -n "$CSV_FILE" ]; then
    echo "CSV file $CSV_FILE found. Running Producer.py..."
    # Pass the CSV file as an argument to Producer.py
    python bcmProducer.py "$CSV_FILE"
else
    echo "Error: No CSV file found in $SUBFOLDER. Please check rpsa_client output."
    exit 1
fi

echo "Done."
