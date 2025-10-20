import requests
import zipfile
import json
import h5py
from lxml import etree as ET
import os
import numpy as np
import pandas as pd
import uuid
import random
import time
from shapely.geometry import Polygon
from mmd_utils.mmd_utils import extract_polygon, get_bounding_box


def generate_http_url(filepath, product_type):

    filename = os.path.basename(filepath)

    root_path = "https://nbstds.met.no/thredds/fileServer/nbsArchive/"
    platform = filename.split('_')[0]
    mission = filename[0:2]

    if mission == 'S1':
        date = filename[17:25]
        mode = filename[4:6]
    elif mission == 'S2':
        date = filename[11:19]
    elif mission == 'S3':
        date = filename[16:24]
    elif mission == 'S5':
        date = filename[20:28]

    year = date[:4]
    month = date[4:6]
    day = date[6:]

    if mission in ['S3', 'S5']:
        url = f'{root_path}{platform}/{year}/{month}/{day}/{product_type}/{filename}'
    elif mission == 'S1':
        url = f'{root_path}{platform}/{year}/{month}/{day}/{mode}/{filename}'
    elif mission == 'S2':
        url = f'{root_path}{platform}/{year}/{month}/{day}/{filename}'

    return url

def is_valid_id(identifier: str) -> bool:
    """
    Check if the provided identifier is valid. It is valid if:
    - It is a valid UUID, or
    - It starts with 'no.met.nbs:' followed by a valid UUID.
    """
    def is_uuid(value: str) -> bool:
        try:
            uuid.UUID(value)
            return True
        except ValueError:
            return False

    if is_uuid(identifier):
        return True

    prefix = "no.met.nbs:"
    if identifier.startswith(prefix):
        return is_uuid(identifier[len(prefix):])

    return False

def check_metadata(metadata: dict, id: str) -> bool:
    """
    Checks if the metadata dictionary contains the required keys
    and if the id variable is a valid UUID.

    Parameters:
    metadata (dict): The metadata dictionary to check.
    id (str): The identifier to validate as a UUID.

    Returns:
    bool: True if all checks pass, False otherwise.
    """
    if not id:
        print("Missing ID")
        return False
    if not metadata:
        print("No metadata found")
        return False

    required_keys = {"north", "south", "east", "west", "orbitNumber", "completionDate", "startDate"}

    missing_keys = required_keys - metadata.keys()  # Find keys that are in required_keys but not in metadata

    if missing_keys:
        print("Missing keys:", missing_keys)
        return False

    # Check if id is a valid UUID
    try:
        if is_valid_id(id):
            return True
        else:
            print("Invalid ID")
            return False
    except ValueError:
        print("Invalid ID")
        return False

def get_product_metadata(product_metadata_df, esa_product_type):
    """
    Extracts all non-empty values from a single row where 'Alias (ESA product type)'
    matches the given esa_product_type.

    Parameters:
        product_metadata_df (pd.DataFrame): The input DataFrame.
        esa_product_type (str): The value to match in 'Alias (ESA product type)'.

    Returns:
        dict: A dictionary of column names and their non-empty values.
    """
    # Filter the DataFrame for the matching row
    row = product_metadata_df[product_metadata_df['Alias (ESA product type)'] == esa_product_type]

    # Extract non-empty values
    if not row.empty:
        return {col: row.iloc[0][col] for col in row.columns if pd.notna(row.iloc[0][col]) and row.iloc[0][col] != ''}
    else:
        return {}

def get_collection_from_filename(filename):
    if filename.startswith('S1'):
        return 'Sentinel1'
    elif filename.startswith('S2'):
        return 'Sentinel2'
    elif filename.startswith('S3'):
        return 'Sentinel3'
    elif filename.startswith('S5'):
        return 'Sentinel5P'
    elif filename.startswith('S6'):
        return 'Sentinel6'
    else:
        raise ValueError('Unknown filename prefix; unable to determine collection')

def get_metadata_from_safe(zip_file):

    base = os.path.basename(zip_file)
    source_file = base.split('.')[0] + '.SAFE'

    xml_file = 'manifest.safe'
    xml_file_path = os.path.join(source_file, xml_file)

    metadata = {}

    # Open the ZIP file and read the manifest.safe file
    with zipfile.ZipFile(zip_file, 'r') as z:
        if xml_file_path in z.namelist():
            with z.open(xml_file_path) as f:

                tree = ET.parse(f)
                root = tree.getroot()

                # Extract namespaces
                namespaces = root.nsmap

                orbit_number_element = root.xpath("//safe:orbitNumber", namespaces=namespaces)
                if orbit_number_element:
                    metadata['orbitNumber'] = orbit_number_element[0].text
                    orbitDirection = orbit_number_element[0].get('groundTrackDirection')
                    if orbitDirection:
                        if orbitDirection[0].lower() in ['d', 'D', 'descending', 'DESCENDING']:
                            metadata['orbitDirection'] = 'descending'
                        elif orbitDirection[0].lower() in ['a', 'A', 'ascending', 'ASCENDING']:
                            metadata['orbitDirection'] = 'ascending'
                    else:
                        orbit_direction_element = root.xpath("//s1:pass", namespaces=namespaces)
                        if orbit_direction_element:
                            metadata['orbitDirection'] = orbit_direction_element[0].text.lower()

                relative_orbit_number_element = root.xpath("//safe:relativeOrbitNumber", namespaces=namespaces)
                if relative_orbit_number_element:
                    metadata['relativeOrbitNumber'] = relative_orbit_number_element[0].text

                start_time_element = root.xpath("//safe:startTime", namespaces=namespaces)
                if start_time_element:
                    metadata['startDate'] = start_time_element[0].text

                stop_time_element = root.xpath("//safe:stopTime", namespaces=namespaces)
                if stop_time_element:
                    metadata['completionDate'] = stop_time_element[0].text
                elif start_time_element:
                    metadata['completionDate'] = start_time_element[0].text

                gml_element = root.xpath("//gml:coordinates", namespaces=namespaces)
                if gml_element:
                    coords_str = gml_element[0].text
                    if base.startswith('S2'):
                        # coordinates in S2 are space separated, e.g. lat lon lat lon lat lon
                        # Split into floats
                        numbers = list(map(float, coords_str.split()))

                        # Group into (lon, lat) tuples
                        coords = [(numbers[i+1], numbers[i]) for i in range(0, len(numbers), 2)]

                    elif base.startswith('S1'):
                        pairs = coords_str.split()

                        # Convert to (lon, lat) tuples
                        coords = [(float(lon), float(lat)) for lat, lon in (pair.split(",") for pair in pairs)]

                    # Create Shapely Polygon
                    metadata['polygon'] = Polygon(coords)

                    try:
                        (
                            metadata['north'],
                            metadata['south'],
                            metadata['east'],
                            metadata['west']
                        ) = get_bounding_box(metadata['polygon'])
                    except:
                        print('Failed to compute bounding box from GML')

                if base.startswith('S1'):

                    mode_element = root.xpath("//s1sarl1:mode", namespaces=namespaces)
                    if mode_element:
                        metadata['sensorMode'] = mode_element[0].text

                    polarisation_element = root.xpath("//s1sarl1:transmitterReceiverPolarisation", namespaces=namespaces)
                    if polarisation_element:
                        polarisation0 = polarisation_element[0].text
                        if len(polarisation_element) == 2:
                            metadata['polarisation'] = polarisation0 + '+' + polarisation_element[1].text
                        elif len(polarisation_element) == 1:
                            metadata['polarisation'] = polarisation0
                        else:
                            pass

        mtd_files = [f for f in z.namelist() if f.startswith(f"{source_file}/MTD_") and f.endswith(".xml")]

        if not mtd_files:
            pass
        else:
            xml_file_path = mtd_files[0]

        with z.open(xml_file_path) as f:
            tree = ET.parse(f)
            root = tree.getroot()

            cloud_cover_element = root.xpath("//*[local-name()='Cloud_Coverage_Assessment']")
            if cloud_cover_element:
                metadata['cloudCover'] = cloud_cover_element[0].text

    return metadata

def get_metadata_from_sen3(sen3_file):

    zip_file = sen3_file.split('.')[0] + '.zip'

    base = os.path.basename(sen3_file)
    source_file = base.split('.')[0] + '.SEN3'

    xml_file = 'xfdumanifest.xml'
    xml_file_path = os.path.join(source_file, xml_file)

    metadata = {}

    # Open the ZIP file and read the manifest.safe file
    with zipfile.ZipFile(zip_file, 'r') as z:
        if xml_file_path in z.namelist():
            with z.open(xml_file_path) as f:
                tree = ET.parse(f)
                root = tree.getroot()

                # Extract namespaces
                namespaces = root.nsmap

                orbit_number_element = root.xpath("//sentinel-safe:orbitNumber", namespaces=namespaces)
                if orbit_number_element:
                    metadata['orbitNumber'] = orbit_number_element[0].text
                    direction = orbit_number_element[0].get('groundTrackDirection').lower()
                    metadata['orbitDirection'] = direction

                relative_orbit_number_element = root.xpath("//sentinel-safe:relativeOrbitNumber", namespaces=namespaces)
                if relative_orbit_number_element:
                    metadata['relativeOrbitNumber'] = relative_orbit_number_element[0].text

                start_time_element = root.xpath("//sentinel-safe:startTime", namespaces=namespaces)
                if start_time_element:
                    metadata['startDate'] = start_time_element[0].text

                stop_time_element = root.xpath("//sentinel-safe:stopTime", namespaces=namespaces)
                if stop_time_element:
                    metadata['completionDate'] = stop_time_element[0].text

                gml_element = root.xpath("//gml:posList", namespaces=namespaces)
                if gml_element:
                    coords_str = gml_element[0].text
                    # Split into floats
                    numbers = list(map(float, coords_str.split()))

                    # Group into (lon, lat) tuples
                    coords = [(numbers[i+1], numbers[i]) for i in range(0, len(numbers), 2)]

                    # Create Shapely Polygon
                    metadata['polygon'] = Polygon(coords)
                    try:
                        (
                            metadata['north'],
                            metadata['south'],
                            metadata['east'],
                            metadata['west']
                        ) = get_bounding_box(metadata['polygon'])
                    except:
                        print('Failed to compute bounding box from GML')

                cloud_cover_element = root.xpath("//sentinel3:cloudyPixels", namespaces=namespaces)
                if cloud_cover_element:
                    metadata['cloudCover'] = cloud_cover_element[0].get('percentage').lower()

    return metadata

def get_metadata_from_netcdf(netcdf_file):

    with h5py.File(netcdf_file, "r") as f:
        global_attrs = dict(f.attrs)

    mapping = {
        'startDate': 'time_coverage_start',
        'completionDate': 'time_coverage_end',
        'north': 'geospatial_lat_max',
        'south': 'geospatial_lat_min',
        'east': 'geospatial_lon_max',
        'west': 'geospatial_lon_min',
        'orbitNumber': 'orbit'
    }

    metadata = {}

    for key, val in mapping.items():
        if val in global_attrs.keys():
            if isinstance(global_attrs[val], (list, np.ndarray)):
                metadata[key] = global_attrs[val][0]
            elif isinstance(global_attrs[val], np.bytes_):
                metadata[key] = global_attrs[val].decode("utf-8")
            else:
                metadata[key] = global_attrs[val]
        else:
            pass

    return metadata

def get_metadata_from_json(json_file):
    '''
    #! DEPRECATED
    NOT CURRENTLY IN USE, DO NOT USE IN PRODUCTION
    '''
    with open(json_file, encoding="utf-8") as file:
        metadata = json.load(file)

    id = metadata.get('uuid')

    for key in [
        'orbitNumber',
        'orbitDirection',
        'productLevel',
        'relativeOrbitNumber',
        'productType',
        'platformName'
    ]:
        lower_key = key.lower()
        if lower_key in metadata:
            metadata[key] = metadata[lower_key]

    if 'beginposition' in metadata:
        metadata['startDate'] = metadata['beginposition']
    if 'endposition' in metadata:
        metadata['completionDate'] = metadata['endposition']

    metadata['polygon'] = extract_polygon(metadata['gmlfootprint'])

    (
        metadata['north'],
        metadata['south'],
        metadata['east'],
        metadata['west']
    ) = get_bounding_box(metadata['polygon'])

    return id, metadata

def query_api(url, params, access_token=None, max_retries=5, base_delay=5):
    """
    Helper function to query the API with exponential backoff and jitter.
    """

    headers = {}
    if access_token:
        headers['Authorization'] = f'Bearer {access_token}'
    for attempt in range(1, max_retries + 1):
        try:
            print(f'Attempt {attempt} of {max_retries}: Querying API...')
            response = requests.get(url, params=params, headers=headers, timeout=15)
            response.raise_for_status()  # Raise HTTPError for bad responses
            return response.json()
        except requests.exceptions.RequestException as e:
            wait = min(base_delay * (2 ** (attempt - 1)), 300)  # cap at 5 min
            wait = wait * (0.5 + random.random())  # add jitter
            print(f'API request failed (attempt {attempt}): {e}')
            if attempt < max_retries:
                print(f'Retrying in {wait:.1f} seconds...')
                time.sleep(wait)
            else:
                print('All retry attempts failed.')
                return None

def get_metadata_from_odata(basename):

    base_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"

    if basename.startswith('S1') or basename.startswith('S2'):
        filename = basename + '.SAFE'
    elif basename.startswith('S3'):
        filename = basename + '.SEN3'
    else:
        filename = basename + '.nc'

    params = {
        "$filter": f"Name eq '{filename}'",
        "$expand": "Attributes",
        "$top": 1
    }

    data = query_api(base_url, params)
    if 'value' in data and len(data['value']) > 0:
        attributes = data['value'][0].get('Attributes', [])
    else:
        attributes = []
    attr_dict = {attr['Name']: attr['Value'] for attr in attributes}

    metadata = {}

    # Extract orbit numbers
    for attr in ['orbitNumber','relativeOrbitNumber']:
        if attr in attr_dict and attr_dict[attr] is not None:
            metadata[attr] = attr_dict[attr]

    if "value" in data and data["value"]:
        item = data["value"][0]
        tracking_id = item["Id"]
        metadata['startDate'] = item['ContentDate']['Start']
        metadata['completionDate'] = item['ContentDate']['End']
        metadata['gmlgeometry'] = item['Footprint']

        metadata['polygon'] = extract_polygon(metadata['gmlgeometry'])
        (
            metadata['north'],
            metadata['south'],
            metadata['east'],
            metadata['west']
        ) = get_bounding_box(metadata['polygon'])

        print('Found required metadata using OData')
        return metadata, tracking_id

    # Graceful failure instead of raising
    print(f"Warning: Issue querying OData for metadata for {filename}.")
    return None, None

def get_metadata_from_opensearch(filename):
    collection = get_collection_from_filename(filename)
    base_url = f'https://catalogue.dataspace.copernicus.eu/resto/api/collections/{collection}/search.json'
    params = {
        'productIdentifier': filename,
        'maxRecords': 1
    }
    print(f"Querying API with URL: {base_url} and params: {params}")
    data = query_api(base_url, params)
    if data and 'features' in data and data['features']:
        metadata = data['features'][0]['properties']
        id = data['features'][0]['id']
        metadata["polygon"] = extract_polygon(metadata["gmlgeometry"])
        (
            metadata["north"],
            metadata["south"],
            metadata["east"],
            metadata["west"]
        ) = get_bounding_box(metadata["polygon"])
        return metadata, id
    else:
        print("No exact match found, trying broader search...")
        parts = filename.split('_')
        if len(parts) > 1:
            params.pop('productIdentifier', None)
            params['q'] = parts[1]  # Using a part of the filename for broader search
            data = query_api(base_url, params)
            if data and 'features' in data and data['features']:
                metadata = data['features'][0]['properties']
                id = data['features'][0]['id']
                metadata["polygon"] = extract_polygon(metadata["gmlgeometry"])
                (
                    metadata["north"],
                    metadata["south"],
                    metadata["east"],
                    metadata["west"]
                ) = get_bounding_box(metadata["polygon"])
                return metadata, id
    # Graceful failure instead of raising
    print(f"Warning: Issue querying OpenSearch for metadata for {filename}")
    return None, None