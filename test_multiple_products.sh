#!/bin/bash

# Path to the YAML file
GLOBAL_ATTRIBUTES="config/global_attributes.yaml"
PLATFORM_METADATA="config/platforms.yaml"
PRODUCT_METADATA="config/product_types.csv"

# List of Sentinel product names
PRODUCTS=(
    "S1C_EW_GRDM_1SDH_20251013T100855_20251013T100951_004542_008FC9_ACA7.zip"
    #"S2A_MSIL2A_20250930T104041_N0511_R008_T33WWR_20250930T124013.zip"
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
    python3 create_mmd.py -p "$PRODUCT" -g "$GLOBAL_ATTRIBUTES" -pr "$PRODUCT_METADATA" -pl "$PLATFORM_METADATA" -m "$XML_FILE" -f "$PRODUCT_FILEPATH" -id
done
