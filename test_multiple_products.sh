#!/bin/bash

# Path to the YAML file
GLOBAL_ATTRIBUTES="config/global_attributes.yaml"
PLATFORM_METADATA="config/platforms.yaml"
PRODUCT_METADATA="config/product_types.csv"

# List of Sentinel product names
PRODUCTS=(
    #"S1A_S2_GRDH_1SDV_20150302T062820_20150302T062839_004849_006096_D34F.zip"
    #"S1B_IW_SLC__1SDV_20210928T070156_20210928T070226_028895_0372C9_DAA1.zip"
    #"S1A_EW_GRDM_1SDH_20250207T071546_20250207T071653_057787_07201D_BA16.SAFE.zip"
    #"S1A_EW_GRDH_1SDH_20250207T071444_20250207T071652_057787_07201D_BBE3.SAFE.zip"
    #"S2A_MSIL2A_20240806T175911_N0511_R041_T27XVK_20240806T234100.zip"
    #"S2B_MSIL1C_20240317T130759_N0510_R081_T35XMH_20240317T133928.zip"
    #"S3A_SR_1_SRA____20221122T102040_20221122T103040_20221122T123949_0599_092_222______PS1_O_NR_004.zip"
    #"S3A_SL_2_LST____20250208T204259_20250208T204559_20250210T082211_0179_122_214_0900_PS1_O_NT_004.zip" # opensearch wrong way coordinates
    #"S5P_OFFL_L1B_RA_BD4_20210926T025310_20210926T043439_20483_02_020000_20210927T072924.nc"
    #"S5P_NRTI_L2__SO2____20250206T123119_20250206T123619_37925_03_020700_20250206T134123.nc"
    #"S5P_OFFL_L2__NP_BD7_20250204T125202_20250204T143332_37897_03_020003_20250206T120908.nc"
    #"S5P_OFFL_L2__SO2____20210926T061608_20210926T075738_20485_02_020201_20210928T083653.nc"
    #"S3A_SL_2_LST____20180301T213419_20180301T213719_20180302T000007_0179_028_243_1080_SVL_O_NR_002"
    #"S3A_SR_1_SRA____20171111T104632_20171111T105632_20171111T130410_0599_024_208______SVL_O_NR_002.zip"
    "S3A_SL_2_FRP____20250311T203612_20250311T203912_20250313T080915_0179_123_271_0720_PS1_O_NT_004.zip"
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
