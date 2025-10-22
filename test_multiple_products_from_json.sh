#!/bin/bash

# Path to the YAML file
GLOBAL_ATTRIBUTES="config/global_attributes.yaml"
PLATFORM_METADATA="config/platforms.yaml"
PRODUCT_METADATA="config/product_types.csv"

# Directory containing JSON files
JSON_DIR="/home/lukem/Documents/MET/Projects/ESA_NBS/Git_repos/cdse_synchroniser"
# Product filepath directory
PRODUCT_DIR="/home/lukem/Documents/MET/Projects/ESA_NBS/Git_repos/NBS_MMD_creator/source_files"

# Loop through all JSON files in the JSON_DIR
for JSON_FILE in "$JSON_DIR"/S*.json; do
    # Extract the filename without the directory and extension
    PRODUCT_NAME=$(basename "$JSON_FILE" .json)
    
    # Check if a matching product file exists in PRODUCT_DIR
    PRODUCT_FILE="$PRODUCT_DIR/$PRODUCT_NAME.zip"
    echo "$PRODUCT_FILE"
    
    if [[ -f "$PRODUCT_FILE" ]]; then
        # Construct the output XML file name
        XML_FILE="${PRODUCT_NAME}.xml"
        PRODUCT="${PRODUCT_NAME}.zip"

        # Run the Python script
        echo "Processing $PRODUCT..."
        python3 create_mmd.py -p "$PRODUCT" -g "$GLOBAL_ATTRIBUTES" -pr "$PRODUCT_METADATA" -pl "$PLATFORM_METADATA" -m "$XML_FILE" -f "$PRODUCT_FILE" -j "$JSON_FILE"
    else
        echo "No match for: $PRODUCT_NAME"
    fi
done

