import argparse
import os
import sys
from utils.mmd_helpers import generate_mmd  # Import the generate_mmd function

# Get the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

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
    parser.add_argument(
        "-j", "--json_metadata_filepath", type=str, required=False,
        help="Path to a JSON metadata file from an earlier OpenSearch query."
    )

    # Parse the command-line arguments
    args = parser.parse_args()

    if os.path.isdir(args.mmd_path):
        print(f"Error: Output path is a directory, not a file: {args.mmd_path}")
        sys.exit(1)

    # Call the generate_mmd function
    generate_mmd(
        script_dir=script_dir,  # Pass the script directory
        filename=args.product,
        global_attributes_config=args.global_attributes_config,
        platform_metadata_config=args.platform_metadata_config,
        product_metadata_csv=args.product_metadata_csv,
        output_path=args.mmd_path,
        overwrite=args.overwrite,
        filepath=args.filepath,
        json_file=args.json_metadata_filepath
    )

if __name__ == "__main__":
    main()
