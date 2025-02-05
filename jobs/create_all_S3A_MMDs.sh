#!/bin/bash

script_dir=$(dirname "$0")
ini_file="$script_dir/filepaths.ini"

# Read filepaths from the config file
input_root_dir=$(grep 'input_root_dir' "$ini_file" | sed 's/input_root_dir="//;s/"$//')
output_root_dir=$(grep 'output_root_dir' "$ini_file" | sed 's/output_root_dir="//;s/"$//')
python_script=$(grep 'python_script' "$ini_file" | sed 's/python_script="//;s/"$//')
yaml_file=$(grep 'yaml_file' "$ini_file" | sed 's/yaml_file="//;s/"$//')

# Ensure all required paths are read successfully
if [[ -z "$input_root_dir" || -z "$output_root_dir" || -z "$python_script" || -z "$yaml_file" ]]; then
    echo "Error: One or more configuration values are missing!"
    exit 1
fi

# Store the full file paths in an array
mapfile -t files < <(find "$input_root_dir" -type f -name 'S3A*.zip')

# Loop through the array
for filepath in "${files[@]}"; do
    filename=$(basename -- "$filepath")
    name_without_suffix="${filename%.zip}"
    echo "Processing file: $name_without_suffix"

    mmd_filename="${name_without_suffix}.xml"

    product_type=${filename:4:12}
    date_part=${filename:16:8}
    year=${date_part:0:4}
    month=${date_part:4:2}
    day=${date_part:6:2}

    target_dir="$output_root_dir/$year/$month/$day/$product_type/metadata"
    mkdir -p "$target_dir"

    mmd_filepath="$target_dir/$mmd_filename"

    # Check if the MMD file already exists
    if [ ! -e "$mmd_filepath" ]; then
        echo "Writing MMD to: $mmd_filepath"
        python3 "$python_script" -p "$name_without_suffix" -y "$yaml_file" -m "$mmd_filepath" -f "$filepath"
    else
        # If the file already exists, skip the Python script
        echo "MMD file $mmd_filepath already exists. Skipping."
    fi

done