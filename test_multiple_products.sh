#!/bin/bash

# Path to the YAML file
GLOBAL_ATTRIBUTES="config/global_attributes.yaml"
PLATFORM_METADATA="config/platforms.yaml"
PRODUCT_METADATA="config/product_types.csv"

# List of Sentinel product names
PRODUCTS=(
    "S3B_OL_2_WFR____20250930T090908_20250930T091208_20250930T110637_0179_111_321_1980_MAR_O_NR_003.zip"
    "S1C_EW_GRDM_1SDH_20250930T110834_20250930T110938_004353_008A1C_C6A6.zip"
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
