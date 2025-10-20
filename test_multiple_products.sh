#!/bin/bash

# Path to the YAML file
GLOBAL_ATTRIBUTES="config/global_attributes.yaml"
PLATFORM_METADATA="config/platforms.yaml"
PRODUCT_METADATA="config/product_types.csv"

# List of Sentinel product names
for file in source_files/*; do
    if [[ -f "$file" ]]; then
        PRODUCTS+=("$(basename "$file")")
    fi
done

# Loop through each product
for PRODUCT in "${PRODUCTS[@]}"
do

    PRODUCT_FILEPATH="source_files/${PRODUCT}"
    PRODUCT_NAME="${PRODUCT%%.*}"

    # Construct the output XML file name
    XML_FILE="${PRODUCT_NAME}.xml"

    # Run the Python script
    echo "Processing $PRODUCT..."
    python3 create_mmd.py -p "$PRODUCT" -g "$GLOBAL_ATTRIBUTES" -pr "$PRODUCT_METADATA" -pl "$PLATFORM_METADATA" -m "$XML_FILE" -f "$PRODUCT_FILEPATH"
done
