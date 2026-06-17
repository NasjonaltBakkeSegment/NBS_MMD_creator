# NBS MMD Creator

## Overview

This script generates a Metadata Markup Document (MMD) XML file for Sentinel satellite data by extracting metadata from the input product file, optional JSON metadata, and supporting configuration tables. It then formats the result into the MMD schema used by the Norwegian Meteorological Institute (MET Norway).

## Features

- Extracts metadata from Sentinel product files and optional JSON metadata, with a fallback metadata query when needed.
- Extracts orbit information from Sentinel data files.
- Computes file checksums (MD5) for integrity verification.
- Detects if a dataset falls within the SIOS (Svalbard Integrated Arctic Earth Observing System) region.
- Supports different Sentinel missions (S1, S2, S3, S5P).
- Generates MMD-compliant XML output.

## Usage

Run the script with the following command-line arguments:

python create_mmd.py --product <FILENAME> --global_attributes_config <YAML_FILE> --platform_metadata_config <YAML_FILE> --product_metadata_csv <CSV_FILE> --mmd_path <OUTPUT_XML> [--filepath <DATAFILE>] [--json_metadata <JSON_FILE>] [--create_id]

### Arguments:

- --product, -p : The Sentinel product filename.
- --global_attributes_config, -g : Path to the YAML file containing global attributes.
- --platform_metadata_config, -pl : Path to the YAML file containing platform metadata.
- --product_metadata_csv, -pr : Path to the CSV file with product metadata.
- --mmd_path, -m : Path to save the generated MMD XML file.
- --filepath, -f : Path to the data file for extracting orbit information (optional).
- --json_metadata, -j : Optional JSON file with expanded OData metadata.
- --create_id, -id : Generate a new NBS metadata identifier instead of using the ESA tracking ID.

### Example:

```
python create_mmd.py -p S1B_IW_SLC__1SDV_20210928T070156_20210928T070226_028895_0372C9_DAA1 -g config/global_attributes.yaml -pl config/platforms.yaml -pr config/product_types.csv -m S1B_IW_SLC__1SDV_20210928T070156_20210928T070226_028895_0372C9_DAA1.xml -f source_files/S1B_IW_SLC__1SDV_20210928T070156_20210928T070226_028895_0372C9_DAA1.zip
```

### Output

The script generates an MMD XML file containing metadata structured according to the MET Norway schema. If an MMD file already exists, it is written to the requested output path again.
