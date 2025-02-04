# NBS MMD Creator

## Overview

This script generates a Metadata Markup Document (MMD) XML file for Sentinel satellite data by retrieving metadata from the Copernicus OpenSearch API. It processes metadata, extracts relevant attributes, and formats them into the MMD schema used by the Norwegian Meteorological Institute (MET Norway).

## Features

- Retrieves metadata from the Copernicus OpenSearch API.
- Extracts orbit information from Sentinel data files.
- Computes file checksums (MD5) for integrity verification.
- Detects if a dataset falls within the SIOS (Svalbard Integrated Arctic Earth Observing System) region.
- Supports different Sentinel missions (S1, S2, S3, S5P). Functionality can be added for S6.
- Generates MMD-compliant XML output.

## Usage

Run the script with the following command-line arguments:

python create_mmd.py --product <FILENAME> --yaml_path <YAML_FILE> --mmd_path <OUTPUT_XML> [--overwrite] [--filepath <DATAFILE>]

### Arguments:

- --product, -p : The Sentinel product filename.
- --yaml_path, -y : Path to the YAML file containing global attributes.
- --mmd_path, -m : Path to save the generated MMD XML file.
- --overwrite, -o : Overwrite existing elements if they exist (optional).
- --filepath, -f : Path to the data file for extracting orbit information (optional).

### Example:

```
python create_mmd.py -p S1B_IW_SLC__1SDV_20210928T070156_20210928T070226_028895_0372C9_DAA1 -y global_attributes.yaml -m S1B_IW_SLC__1SDV_20210928T070156_20210928T070226_028895_0372C9_DAA1.xml -o
```

### Output

The script generates an MMD XML file containing metadata structured according to the MET Norway schema. If an MMD file already exists, it can be updated or left unchanged based on the --overwrite flag.
