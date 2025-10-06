import argparse
import os
import sys
import pandas as pd
from mmd_utils.metadata_extraction import (
    get_metadata_from_netcdf,
    get_metadata_from_opensearch,
    get_metadata_from_odata,
    get_metadata_from_safe,
    get_metadata_from_sen3,
    check_metadata,
)
from mmd_utils.config_handling import load_config,save_xml_to_file
from mmd_utils.mmd_helpers import create_xml, get_id_from_mapping_file

# Get the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

def generate_mmd(filename, global_attributes_config, platform_metadata_config, product_metadata_csv, output_path, overwrite, filepath):
    basename = filename.split('.')[0]
    try:
        # Pass script_dir to get_id_from_mapping_file
        id = get_id_from_mapping_file(script_dir, filename)

        if filename.startswith("S5"):
            print("Extracting metadata from NetCDF file")
            metadata = get_metadata_from_netcdf(filepath)
        elif filename.startswith("S3"):
            print("Extracting metadata from SEN3 file")
            metadata = get_metadata_from_sen3(filepath)
        elif filename[:2] in ["S1", "S2"]:
            print("Extracting metadata from SAFE file")
            metadata = get_metadata_from_safe(filepath)
        else:
            metadata = {}
            id = None
    except Exception as e:
        print(f"Error: Couldn't extract metadata from source file. Reason: {e}")
        metadata = {}
        id = None

    if not check_metadata(metadata, id):
        print("Insufficient metadata, so querying")
        metadata, id = get_metadata_from_odata(basename)
        if not check_metadata(metadata,id):
            metadata, id = get_metadata_from_opensearch(basename)
        else:
            pass

    # Load configurations
    global_attributes = load_config(global_attributes_config)
    platform_metadata = load_config(platform_metadata_config)
    product_metadata_df = pd.read_csv(product_metadata_csv)

    # Create XML
    mmd_xml = create_xml(script_dir, metadata, id, global_attributes, platform_metadata, product_metadata_df, filename, filepath)

    # Save XML to the output path
    save_xml_to_file(mmd_xml, output_path)
    print(f"MMD XML file saved to {output_path}")

def main():
    """
    Main function to parse arguments and call the generate_mmd function.
    """
    parser = argparse.ArgumentParser(description="Generate an MMD file from Copernicus metadata.")

    parser.add_argument(
        "--product", "-p", type=str, required=True,
        help="The product filename to fetch metadata for."
    )
    parser.add_argument(
        "--global_attributes_config", "-g", type=str, required=True,
        help="Path to the YAML global attributes configuration file."
    )
    parser.add_argument(
        "--platform_metadata_config", "-pl", type=str, required=True,
        help="Path to the YAML platform metadata configuration file."
    )
    parser.add_argument(
        "--product_metadata_csv", "-pr", type=str, required=True,
        help="Path to the CSV file with metadata related to each product type."
    )
    parser.add_argument(
        "--mmd_path", "-m", type=str, required=True,
        help="Path to save the generated MMD file."
    )
    parser.add_argument(
        "-o", "--overwrite", action="store_true",
        help="Overwrite existing elements if they exist."
    )
    parser.add_argument(
        "-f", "--filepath", type=str, required=False,
        help="Path to the data file (e.g., .zip, .SAFE, .nc) for metadata extraction."
    )

    # Parse the command-line arguments
    args = parser.parse_args()

    if os.path.isdir(args.mmd_path):
        print(f"Error: Output path is a directory, not a file: {args.mmd_path}")
        sys.exit(1)

    # Call the generate_mmd function
    generate_mmd(
        filename=args.product,
        global_attributes_config=args.global_attributes_config,
        platform_metadata_config=args.platform_metadata_config,
        product_metadata_csv=args.product_metadata_csv,
        output_path=args.mmd_path,
        overwrite=args.overwrite,
        filepath=args.filepath
    )

if __name__ == "__main__":
    main()
