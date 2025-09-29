#!/bin/bash

# Path to the YAML file
GLOBAL_ATTRIBUTES="config/global_attributes.yaml"
PLATFORM_METADATA="config/platforms.yaml"
PRODUCT_METADATA="config/product_types.csv"

# List of Sentinel product names
PRODUCTS=(
    "S2B_MSIL1C_20250922T125259_N0511_R138_T27WVM_20250922T144414.zip"
    "S5P_OFFL_L2__SO2____20250922T005651_20250922T023821_41153_03_020701_20250924T221311.nc"
)

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
