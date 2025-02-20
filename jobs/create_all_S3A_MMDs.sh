#!/bin/bash

script_dir=$(dirname "$0")
ini_file="$script_dir/filepaths.ini"

# Read filepaths from the config file
input_root_dir=$(awk -F '=' '/input_root_dir/ {gsub(/"/, "", $2); print $2}' "$ini_file")
output_root_dir=$(awk -F '=' '/output_root_dir/ {gsub(/"/, "", $2); print $2}' "$ini_file")
python_script=$(awk -F '=' '/python_script/ {gsub(/"/, "", $2); print $2}' "$ini_file")
global_attributes=$(awk -F '=' '/global_attributes/ {gsub(/"/, "", $2); print $2}' "$ini_file")
product_metadata=$(awk -F '=' '/product_metadata/ {gsub(/"/, "", $2); print $2}' "$ini_file")
platform_metadata=$(awk -F '=' '/platform_metadata/ {gsub(/"/, "", $2); print $2}' "$ini_file")

# Ensure all required paths are read successfully
if [[ -z "$input_root_dir" || -z "$output_root_dir" || -z "$python_script" || -z "$global_attributes" || -z "$product_metadata" || -z "$platform_metadata" ]]; then
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
        python3 "$python_script" -p "$name_without_suffix" -g "$global_attributes" -pr "$product_metadata" -pl "$platform_metadata" -m "$mmd_filepath" -f "$filepath"
    else
        # If the file already exists, skip the Python script
        echo "MMD file $mmd_filepath already exists. Skipping."
    fi

done