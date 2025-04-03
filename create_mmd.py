import requests
from lxml import etree as ET
import yaml
import sys
import os
import argparse
import hashlib
from datetime import datetime
from shapely.geometry import Polygon, LinearRing, box
import zipfile
import re
import h5py
import uuid
import pandas as pd
import json
import glob
import numpy as np

# Get the script's directory
script_dir = os.path.dirname(os.path.abspath(__file__))

# # Construct the relative path to the config file
# config_path = os.path.join(script_dir, "config", "config.yaml")

# # Load the filepaths from the YAML file
# with open(config_path, 'r') as file:
#     config = yaml.safe_load(file)

# # Add the repository paths to sys.path
# repos = config.get('repos', {})
# for repo_name, repo_path in repos.items():
#     if os.path.exists(repo_path):
#         sys.path.append(repo_path)
#     else:
#         print(f"Warning: The path {repo_path} does not exist.")

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

def get_parent_id(platform, product_type):
    mapping_file = os.path.join(script_dir, "config", "parent_id_mapping.yaml")
    with open(mapping_file, 'r') as file:
        mapping = yaml.safe_load(file)
    parent_id = mapping[platform][product_type]
    return parent_id

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
        return False
    if not metadata:
        return False

    required_keys = {"north", "south", "east", "west", "orbitNumber", "completionDate", "startDate"}

    missing_keys = required_keys - metadata.keys()  # Find keys that are in required_keys but not in metadata

    if missing_keys:
        print("Missing keys:", missing_keys)
        return False

    # Check if id is a valid UUID
    try:
        uuid.UUID(id)
    except ValueError:
        return False

    return True

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

def get_size_mb(path):
    """Returns the size of a file or the uncompressed size of a ZIP in MB."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"The path '{path}' does not exist.")

    if os.path.isfile(path):
        # Check if it's a zip file
        if zipfile.is_zipfile(path):
            with zipfile.ZipFile(path, 'r') as zip_ref:
                size_bytes = sum(file.file_size for file in zip_ref.infolist())
        else:
            size_bytes = os.path.getsize(path)  # Regular file size
    else:
        raise ValueError(f"'{path}' is not a file.")

    return size_bytes / (1024 * 1024)  # Convert to MB

def within_sios(coord_strings=None,north=None,south=None,east=None,west=None):
    # Parse the SIOS polygon
    sios_polygon = Polygon([
        (-20, 70),
        (-20, 90),
        (40, 90),
        (40, 70),
        (-20, 70)
    ])

    if coord_strings:
        # Try to convert directly, and fall back to regex extraction if needed
        try:
            # Attempt to parse coordinates directly from coord_strings
            linear_ring_coords = [tuple(map(float, coord.split())) for coord in coord_strings]

            # Create a Shapely LinearRing from the coordinates
            linear_ring = LinearRing(linear_ring_coords)

            # Check if the LinearRing intersects the SIOS polygon
            return linear_ring.intersects(sios_polygon)

        except ValueError:
            # If a ValueError occurs (e.g., invalid format like XML/GML), fallback to regex method
            linear_ring_coords = []

            # Regular expression to match coordinates (e.g., -8.766348 57.313187)
            coord_pattern = re.compile(r"(-?\d+\.\d+) (-?\d+\.\d+)")

            # Loop through each coordinate string
            for coord_string in coord_strings:
                # Find all coordinate pairs in the string using regex
                matches = coord_pattern.findall(coord_string)

                # Convert matched coordinates to tuples of floats and add to the list
                for match in matches:
                    linear_ring_coords.append((float(match[0]), float(match[1])))

            # Create a Shapely LinearRing from the coordinates
            linear_ring = LinearRing(linear_ring_coords)

            # Check if the LinearRing intersects the SIOS polygon
            return linear_ring.intersects(sios_polygon)

    elif north is not None and south is not None and east is not None and west is not None:
        # Create a bounding box Polygon
        bbox = box(west, south, east, north)

        # Check if the bounding box intersects the SIOS polygon
        return bbox.intersects(sios_polygon)

    return False  # Default return if neither condition is met

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
                    metadata['polygon'] = gml_element[0].text
                    # coordinates in S1 are comma separated, e.g. lat,lon lat,lon lat,lon
                    # coordinates in S2 are space separated, e.g. lat lon lat lon lat lon
                    metadata['polygon'] = metadata['polygon'].replace(',', ' ')

                    try:
                        (
                            metadata['north'],
                            metadata['south'],
                            metadata['east'],
                            metadata['west'],
                            metadata['coords']
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
                    metadata['polygon'] = gml_element[0].text
                    # Polygon in SAFE file is lat,lon lat,lon etc...
                    # But polygon through OpenSearch is lon,lat lon,lat etc...
                    # So need to flip the coordinates round here to be consistent since this program supports retrieves pulling metadata from either OpenSearch or the file.
                    coords = list(map(float, metadata['polygon'].split()))
                    flipped_coords = [f"{lat} {lon}" for lon, lat in zip(coords[0::2], coords[1::2])]
                    coords = flipped_coords = [f"{lon} {lat}" for lon, lat in zip(coords[0::2], coords[1::2])]
                    #metadata['polygon'] = " ".join(flipped_coords)
                    metadata['polygon'] = " ".join(coords)
                    try:
                        (
                            metadata['north'],
                            metadata['south'],
                            metadata['east'],
                            metadata['west'],
                            metadata['coords']
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

    metadata['polygon'] = extract_coordinates(metadata['gmlfootprint'])

    (
        metadata['north'],
        metadata['south'],
        metadata['east'],
        metadata['west'],
        metadata['coords']
    ) = get_bounding_box(metadata['polygon'])

    return id, metadata

def get_metadata_from_opensearch(filename):

    collection = get_collection_from_filename(filename)
    base_url = f'https://catalogue.dataspace.copernicus.eu/resto/api/collections/{collection}/search.json'

    params = {
        'productIdentifier': filename,
        'maxRecords': 1
    }

    print(f"Querying API with URL: {base_url} and params: {params}")  # Debug statement for query
    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        data = response.json()

        #print(f"API Response: {data}")  # Debug statement to inspect API response
        if 'features' in data and data['features']:
            return data['features'][0]['properties'], data['features'][0]['id']
        else:
            # Try a broader search if exact match fails
            print("No exact match found, trying broader search...")
            # Extracting parts of the filename for broader search
            parts = filename.split('_')
            if len(parts) > 1:
                params.pop('productIdentifier')
                params['q'] = parts[1]  # Using a part of the filename for search
                response = requests.get(base_url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    print(f"Broader API Response: {data}")  # Debug statement for broader search response
                    if 'features' in data and data['features']:
                        return data['features'][0]['properties'], data['features'][0]['id']
            raise ValueError('No metadata found for the given filename.')
    else:
        print(f"API Request failed with status code {response.status_code}")  # Debug statement for failed request
        raise ValueError('No metadata found for the given filename.')

def load_config(yaml_path):
    with open(yaml_path, 'r') as file:
        return yaml.safe_load(file)

def prepend_mmd(tag: str) -> str:
        return f'{{http://www.met.no/schema/mmd}}{tag}'

def prepend_gml(tag: str) -> str:
        return f'{{http://www.opengis.net/gml}}{tag}'

def prepend_xml(tag: str) -> str:
        return f'{{http://www.w3.org/XML/1998/namespace}}{tag}'

def create_root_with_namespaces(tag: str, namespaces: dict) -> ET.Element:
    nsmap = {f'xmlns:{prefix}': uri for prefix, uri in namespaces.items()}
    return ET.Element(f'{{{namespaces["mmd"]}}}{tag}', nsmap)

def extract_coordinates(xml_string):
    match = re.search(r"<gml:coordinates>(.*?)</gml:coordinates>", xml_string)
    if match:
        return match.group(1).strip()
    return ""

def get_bounding_box(polygon):
    """
    Given the GML geometry,
    returns the bounding box as (north, south, east, west) and coords to write to the polygon element.
    """
    polygon = polygon.replace(',', ' ')
    coords = re.findall(r'\S+\s+\S+', polygon)

    lat_lon_pairs = [tuple(map(float, coord.split())) for coord in coords]

    lats, lons = zip(*lat_lon_pairs)

    north = max(lats)
    south = min(lats)
    east = max(lons)
    west = min(lons)

    return north, south, east, west, coords

def get_zip_checksum(zip_filepath):
    md5_check = hashlib.md5()
    try:
        with open(zip_filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_check.update(chunk)
        return md5_check.hexdigest()
    except FileNotFoundError:
        return 'File not found'
    except Exception as e:
        return str(e)

def get_netcdf_checksum(netcdf_filepath):
    md5_check = hashlib.md5()
    try:
        with open(netcdf_filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_check.update(chunk)
        return md5_check.hexdigest()
    except FileNotFoundError:
        return 'File not found'
    except Exception as e:
        return str(e)

def create_xml(metadata, id, global_attributes, platform_metadata, product_metadata_df, filename, filepath=None):

    filename_platform = filename.split('_')[0]
    filename_mission = filename[0:2]
    if filename.startswith('S1'):
        filename_product_type = filename.split('_')[1] + '_' + filename.split('_')[2]
        if filename_product_type.startswith('S'):
            filename_product_type = filename[4:14]
    elif filename.startswith('S2'):
        filename_product_type = filename.split('_')[1]
    elif filename.startswith('S3'):
        filename_product_type = filename[4:15]
    elif filename.startswith('S5'):
        filename_product_type = filename[9:19]
    else:
        raise ValueError(f'Could not identify product type from filename')
    product_metadata = get_product_metadata(product_metadata_df,filename_product_type)

    # TODO: The SAFE filepath will later be predictable so use this predictable filepath instead of passing an argument
    namespaces = {
        'mmd': 'http://www.met.no/schema/mmd',
        'gml': 'http://www.opengis.net/gml'
    }
    for prefix, uri in namespaces.items():
        ET.register_namespace(prefix, uri)

    root = ET.Element(f'{{{namespaces["mmd"]}}}mmd', nsmap=namespaces)

    metadata_identifier = ET.SubElement(root, prepend_mmd('metadata_identifier'))
    metadata_identifier.text = id

    title = ET.SubElement(root, prepend_mmd('title'))
    title.attrib[prepend_xml('lang')] = 'en'
    title.text = filename.split('.')[0]

    abstract = ET.SubElement(root, prepend_mmd('abstract'))
    abstract.attrib[prepend_xml('lang')] = 'en'
    abstract.text = product_metadata['description']

    metadata_status = ET.SubElement(root, prepend_mmd('metadata_status'))
    metadata_status.text = global_attributes['metadata_status']

    dataset_production_status = ET.SubElement(root, prepend_mmd('dataset_production_status'))
    dataset_production_status.text = global_attributes['dataset_production_status']

    collection = ET.SubElement(root, prepend_mmd('collection'))
    collection.text = 'NBS'

    if 'coords' in metadata.keys():
        if within_sios(coord_strings=metadata['coords']):
            collection = ET.SubElement(root, prepend_mmd('collection'))
            collection.text = 'SIOS'
        else:
            pass
    elif {"north", "south", "east", "west"}.issubset(metadata):
        if within_sios(
                north=metadata['north'],
                south=metadata['south'],
                east=metadata['east'],
                west=metadata['west']
            ):
            collection = ET.SubElement(root, prepend_mmd('collection'))
            collection.text = 'SIOS'
        else:
            pass
    else:
        print("Coordinates not present so could not compute whether data fall within SIOS AOI")
        pass

    last_metadata_update = ET.SubElement(root, prepend_mmd('last_metadata_update'))
    update = ET.SubElement(last_metadata_update, prepend_mmd('update'))
    creation_timestamp = ET.SubElement(update, prepend_mmd('datetime'))
    creation_timestamp.text = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    update_type = ET.SubElement(update, prepend_mmd('type'))
    update_type.text = 'Created'
    note = ET.SubElement(update, prepend_mmd('note'))

    temporal_extent = ET.SubElement(root, prepend_mmd('temporal_extent'))
    start_date = ET.SubElement(temporal_extent, prepend_mmd('start_date'))
    start_date.text = metadata['startDate']
    end_date = ET.SubElement(temporal_extent, prepend_mmd('end_date'))
    end_date.text = metadata['completionDate']

    iso_topics = product_metadata['iso_topic_category'].split(',')
    keywords = product_metadata['keywords'].split(',')

    for topic in iso_topics:
        iso_topic_category = ET.SubElement(root, prepend_mmd('iso_topic_category'))
        iso_topic_category.text = topic.strip()

    # Separate and process the keywords
    gcmdsk_keywords = []
    gemet_keywords = []

    for keyword in keywords:
        if keyword.startswith('GCMDSK:'):
            gcmdsk_keywords.append(keyword[len('GCMDSK:'):].strip())
        elif keyword.startswith('GEMET:'):
            gemet_keywords.append(keyword[len('GEMET:'):].strip())

    # Create separate XML elements for GCMDSK and GEMET keywords
    if gcmdsk_keywords:
        gcmdsk_elem = ET.SubElement(root, prepend_mmd('keywords'))
        gcmdsk_elem.attrib['vocabulary'] = 'GCMDSK'
        for keyword in gcmdsk_keywords:
            keyword_elem = ET.SubElement(gcmdsk_elem, prepend_mmd('keyword'))
            keyword_elem.text = keyword

        gcmdsk_resource = ET.SubElement(gcmdsk_elem, prepend_mmd('resource'))
        gcmdsk_resource.text = 'https://gcmd.earthdata.nasa.gov/kms/concepts/concept_scheme/sciencekeywords'
        gcmdsk_sep = ET.SubElement(gcmdsk_elem, prepend_mmd('separator'))
        gcmdsk_sep.text = '>'

    if gemet_keywords:
        gemet_elem = ET.SubElement(root, prepend_mmd('keywords'))
        gemet_elem.attrib['vocabulary'] = 'GEMET'
        for keyword in gemet_keywords:
            keyword_elem = ET.SubElement(gemet_elem, prepend_mmd('keyword'))
            keyword_elem.text = keyword

        gemet_resource = ET.SubElement(gemet_elem, prepend_mmd('resource'))
        gemet_resource.text = 'http://inspire.ec.europa.eu/theme'

    if {"north", "south", "east", "west"}.issubset(metadata):
        geographic_extent = ET.SubElement(root, prepend_mmd('geographic_extent'))
        rectangle = ET.SubElement(geographic_extent, prepend_mmd('rectangle'))
        rectangle.attrib['srsName'] = 'EPSG:4326'
        north = ET.SubElement(rectangle, prepend_mmd('north'))
        south = ET.SubElement(rectangle, prepend_mmd('south'))
        east = ET.SubElement(rectangle, prepend_mmd('east'))
        west = ET.SubElement(rectangle, prepend_mmd('west'))
        north.text = str(metadata['north'])
        south.text = str(metadata['south'])
        east.text = str(metadata['east'])
        west.text = str(metadata['west'])
    if 'coords' in metadata.keys():
        polygon = ET.SubElement(geographic_extent, prepend_mmd('polygon'))
        sub_poly = ET.SubElement(polygon, prepend_gml('Polygon'))
        sub_poly.attrib['id'] = 'polygon'
        sub_poly.attrib['srsName'] = 'EPSG:4326'
        exterior = ET.SubElement(sub_poly, prepend_gml('exterior'))
        linear_ring = ET.SubElement(exterior, prepend_gml('LinearRing'))
        for elem in metadata['coords']:
            pos = ET.SubElement(linear_ring, prepend_gml('pos'))
            pos.text = elem
    else:
        print('Warning: polygon is None. Geographic extent will not be included in the XML.')

    dataset_lang = ET.SubElement(root, prepend_mmd('dataset_language'))
    dataset_lang.text = global_attributes['dataset_language']

    operational_status = ET.SubElement(root, prepend_mmd('operational_status'))
    operational_status.text = global_attributes['processing_level']

    access_cons = ET.SubElement(root, prepend_mmd('access_constraint'))
    access_cons.text = global_attributes['access_constraint']

    personnel_1 = ET.SubElement(root, prepend_mmd('personnel'))
    role = ET.SubElement(personnel_1, prepend_mmd('role'))
    role.text = global_attributes['creator_role']
    name = ET.SubElement(personnel_1, prepend_mmd('name'))
    name.text = global_attributes['creator_name']
    email = ET.SubElement(personnel_1, prepend_mmd('email'))
    email.text = global_attributes['creator_email']
    organisation = ET.SubElement(personnel_1, prepend_mmd('organisation'))
    organisation.text = global_attributes['creator_institution']

    personnel_2 = ET.SubElement(root, prepend_mmd('personnel'))
    role_2 = ET.SubElement(personnel_2, prepend_mmd('role'))
    role_2.text = global_attributes['contributor_role']
    name_2 = ET.SubElement(personnel_2, prepend_mmd('name'))
    name_2.text = global_attributes['contributor_name']
    email_2 = ET.SubElement(personnel_2, prepend_mmd('email'))
    email_2.text = global_attributes['contributor_email']
    organisation_2 = ET.SubElement(personnel_2, prepend_mmd('organisation'))
    organisation_2.text = global_attributes['contributor_institution']

    data_center = ET.SubElement(root, prepend_mmd('data_center'))
    data_center_name = ET.SubElement(data_center, prepend_mmd('data_center_name'))
    data_center_short = ET.SubElement(data_center_name, prepend_mmd('short_name'))
    data_center_short.text = 'METNO'
    data_center_long = ET.SubElement(data_center_name, prepend_mmd('long_name'))
    data_center_long.text = 'Norwegian Meteorological Institute'
    data_center_url = ET.SubElement(data_center, prepend_mmd('data_center_url'))
    data_center_url.text = global_attributes['creator_url']

    storage_information = ET.SubElement(root, prepend_mmd('storage_information'))
    file_extension = os.path.splitext(filepath)[1].lower()  # Get the file extension (e.g., '.zip' or '.nc')
    file_name = ET.SubElement(storage_information, prepend_mmd('file_name'))
    file_name.text = filename
    file_format = ET.SubElement(storage_information, prepend_mmd('file_format'))
    if file_extension in ['.zip','SAFE','SEN3']:
        if filename.startswith('S3'):
            file_format.text = 'SEN3'
        elif filename.startswith('S1') or filename.startswith('S2'):
            file_format.text = 'SAFE'
    elif file_extension == '.nc':
        file_format.text = 'NetCDF'
    file_size = ET.SubElement(storage_information, prepend_mmd('file_size'))
    file_size.attrib['unit'] = 'MB'
    if 'size' not in metadata:
        file_size_conv = get_size_mb(filepath)
    else:
        file_size_conv = float(metadata['size'].split(' ')[0])
    file_size.text = f'{file_size_conv:.2f}'

    # Compute checksum for file
    checksum = ET.SubElement(storage_information, prepend_mmd('checksum'))
    checksum.attrib['type'] = 'md5sum'

    if filepath:
        try:
            if file_extension == '.zip':
                checksum.text = get_zip_checksum(filepath)
            elif file_extension == '.nc':
                checksum.text = get_netcdf_checksum(filepath)
            else:
                checksum.text = 'Unsupported file type'
        except Exception as e:
            checksum.text = str(e)
    else:
        checksum.text = 'File not found'

    project = ET.SubElement(root, prepend_mmd('project'))
    project_s_name = ET.SubElement(project, prepend_mmd('short_name'))
    project_s_name.text = global_attributes['project_short_name']
    project_l_name = ET.SubElement(project, prepend_mmd('long_name'))
    project_l_name.text = global_attributes['project']

    platform = ET.SubElement(root, prepend_mmd('platform'))
    platform_short_name = ET.SubElement(platform, prepend_mmd('short_name'))
    platform_long_name = ET.SubElement(platform, prepend_mmd('long_name'))
    platform_short_name.text = filename_platform.replace('S','Sentinel-')
    if filename.startswith('S5'):
        platform_long_name.text = 'Sentinel-5 precursor'
    else:
        platform_long_name.text = platform_short_name.text
    platform_resource = ET.SubElement(platform, prepend_mmd('resource'))

    if 'relativeOrbitNumber' in metadata.keys():
        orbit_relative = ET.SubElement(platform, prepend_mmd('orbit_relative'))
        orbit_relative.text = str(metadata['relativeOrbitNumber'] )
    if 'orbitNumber' in metadata.keys():
        orbit_absolute = ET.SubElement(platform, prepend_mmd('orbit_absolute'))
        orbit_absolute.text = str(metadata['orbitNumber'])
    if 'orbitDirection' in metadata.keys():
        orbit_direction = ET.SubElement(platform, prepend_mmd('orbit_direction'))
        if metadata['orbitDirection'] != None:
            orbit_direction.text = metadata['orbitDirection'].lower()

    # We need to include 2 instruments for the same platform for SYN products.
    instrument_short_names = product_metadata['instrument_short_name'].split(', ')
    num_instruments = len(instrument_short_names)
    for ii in range(num_instruments):

        instrument = ET.SubElement(platform, prepend_mmd('instrument'))
        s_name = ET.SubElement(instrument, prepend_mmd('short_name'))
        l_name = ET.SubElement(instrument, prepend_mmd('long_name'))
        instrument_resource = ET.SubElement(instrument, prepend_mmd('resource'))

        s_name.text = product_metadata['instrument_short_name'].split(', ')[ii]
        l_name.text = product_metadata['instrument_long_name'].split(', ')[ii]
        instrument_resource.text = product_metadata['instrument_vocabulary'].split(', ')[ii]

        if filename_mission == 'S1' and 'sensorMode' in metadata.keys():
            mode = ET.SubElement(instrument, prepend_mmd('mode'))
            mode.text = metadata['sensorMode']
            if 'polarisation' in metadata.keys():
                polarisation = ET.SubElement(instrument, prepend_mmd('polarisation'))
                polarisation.text = metadata['polarisation'].replace('&','+')

        product_type = ET.SubElement(instrument, prepend_mmd('product_type'))
        product_type.text = product_metadata['product_type']

    ancillary = ET.SubElement(platform, prepend_mmd('ancillary'))
    if 'cloudCovered' in metadata.keys():
        cloud_coverage = ET.SubElement(ancillary, prepend_mmd('cloud_coverage'))
        cloud_coverage.text =  str(metadata['cloudCover'])

    spatial_representation = ET.SubElement(root, prepend_mmd('spatial_representation'))
    spatial_representation.text = global_attributes['spatial_representation']

    activity_type = ET.SubElement(root, prepend_mmd('activity_type'))
    activity_type.text = global_attributes['source']

    dataset_citation = ET.SubElement(root, prepend_mmd('dataset_citation'))
    dataset_citation_author = ET.SubElement(dataset_citation, prepend_mmd('author'))
    dataset_citation_author.text = global_attributes['creator_name']
    dataset_citation_title = ET.SubElement(dataset_citation, prepend_mmd('title'))
    dataset_citation_title.text = filename.split('.')[0]

    related_information = ET.SubElement(root, prepend_mmd('related_information'))
    related_type = ET.SubElement(related_information, prepend_mmd('type'))
    related_desc = ET.SubElement(related_information, prepend_mmd('description'))
    related_res = ET.SubElement(related_information, prepend_mmd('resource'))

    platform_resource.text = platform_metadata[filename_platform]['platform_vocabulary']
    related_type.text = platform_metadata[filename_platform]['related_information_type']
    related_desc.text = platform_metadata[filename_platform]['related_information_description']
    related_res.text = platform_metadata[filename_platform]['related_information_resource']

    use_constraint = ET.SubElement(root, prepend_mmd('use_constraint'))
    license_text = ET.SubElement(use_constraint, prepend_mmd('license_text'))
    license_text.text = global_attributes['license_text']

    data_access = ET.SubElement(root,prepend_mmd('data_access'))
    da_type = ET.SubElement(data_access, prepend_mmd('type'))
    da_type.text = 'HTTP'
    da_description = ET.SubElement(data_access,prepend_mmd('description'))
    da_description.text = 'Direct access to the full data file.'
    da_resource = ET.SubElement(data_access,prepend_mmd('resource'))
    da_resource.text = generate_http_url(filepath,product_metadata['product_type'])

    parent_ID = get_parent_id(filename_platform, product_metadata['product_type'])
    related_dataset = ET.SubElement(root,prepend_mmd('related_dataset'))
    related_dataset.attrib['relation_type'] = "parent"
    related_dataset.text = str(parent_ID)
    return root

def save_xml_to_file(xml_element, output_path):

    tree = ET.ElementTree(xml_element)
    output_path = output_path.split('.')[0]+'.xml'
    tree.write(output_path, encoding='utf-8', xml_declaration=True, pretty_print=True)

def get_id_from_mapping_file(filename):
    try:
        # Regex pattern to match the first occurrence of a 4-digit year
        pattern = re.compile(r"(\d{4})")
        match = pattern.search(filename)
        year = str(match.group(1))
        mission = get_collection_from_filename(filename).replace('Sentinel','Sentinel-').replace('Sentinel-5P','Sentinel-5')
        filename_pattern = os.path.join(script_dir, f"mapping/{mission}_{year}0101-{year}*mapping*")
        matching_files = sorted(glob.glob(filename_pattern))
        mapping_file = matching_files[-1]

        with open(mapping_file, 'r', encoding="utf-8") as file:
            dic = json.load(file)
        id = dic[filename.split('.')[0]]
        return id
    except:
        return None

def generate_mmd(filename, global_attributes_config, platform_metadata_config, product_metadata_csv, output_path, overwrite, filepath, json_file=None):
    basename = filename.split('.')[0]
    try:
        if os.path.exists(json_file):
            print('Extracting metadata from JSON file')
            id, metadata = get_metadata_from_json(json_file)
        else:
            raise FileNotFoundError  # Forces fallback logic
    except Exception as e:  # Catch specific exceptions if needed
        print(f"Warning: Couldn't extract metadata from JSON file. Reason: {e}")
        try:
            id = get_id_from_mapping_file(filename)

            if filename.startswith('S5'):
                print('Extracting metadata from netCDF file')
                metadata = get_metadata_from_netcdf(filepath)
            elif filename.startswith('S3'):
                print('Extracting metadata from SEN3 file')
                metadata = get_metadata_from_sen3(filepath)
            elif filename[:2] in ['S1', 'S2']:
                print('Extracting metadata from SAFE file')
                metadata = get_metadata_from_safe(filepath)
            else:
                metadata = {}
                id = None
        except Exception as e:
            print(f"Error: Couldn't extract metadata from source file. Reason: {e}")
            metadata = {}
            id = None
    if check_metadata(metadata,id) == False:
        print('Insufficient metadata, so querying')
        # In production, query for now in some cases, but later synchroniser should store this so this code should become redundant.
        metadata, id = get_metadata_from_opensearch(basename)
        metadata['polygon'] = extract_coordinates(metadata['gmlgeometry'])
        s = metadata['polygon']
        metadata['polygon']  = " ".join(",".join(pair.split(",")[::-1]) for pair in s.split())
        (
            metadata['north'],
            metadata['south'],
            metadata['east'],
            metadata['west'],
            metadata['coords']
        ) = get_bounding_box(metadata['polygon'])
    else:
        pass

    global_attributes = load_config(global_attributes_config)
    platform_metadata = load_config(platform_metadata_config)
    product_metadata_df = pd.read_csv(product_metadata_csv)

    mmd_xml = create_xml(metadata, id, global_attributes, platform_metadata, product_metadata_df, filename, filepath)
    save_xml_to_file(mmd_xml, output_path)
    print(f'Metadata XML file saved to {output_path}')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate an MMD file from Copernicus metadata.')
    parser.add_argument(
        '--product', '-p', type=str, required=True,
        help='The product filename to fetch metadata for.'
    )
    parser.add_argument(
        '--global_attributes_config', '-g', type=str, required=True,
        help='Path to the YAML configuration file with global attributes.'
    )
    parser.add_argument(
        '--platform_metadata_config', '-pl', type=str, required=True,
        help='Path to the YAML configuration file with metadata related to each platform.'
    )
    parser.add_argument(
        '--product_metadata_csv', '-pr', type=str, required=True,
        help='Path to the CSV file with metadata related to each product type.'
    )
    parser.add_argument(
        '--mmd_path', '-m', type=str, required=True,
        help='Path to save the generated MMD file.'
    )
    parser.add_argument(
        '-o', '--overwrite', action='store_true',
        help='Overwrite existing elements if they exist.'
    )
    parser.add_argument(
        '-f', '--filepath', type=str, required=False,
        help='Filepath to the data file, used to obtain orbit direction. Will become deprecated once predictable.'
    )
    parser.add_argument(
        '-j', '--json_metadata_filepath', type=str, required=False,
        help='Filepath to a json file include metadata from an early opensearch query'
    )

    args = parser.parse_args()

    if os.path.isdir(args.mmd_path):
        print(f'Output path is a directory, not a file: {args.mmd_path}')
        sys.exit(1)

    generate_mmd(args.product, args.global_attributes_config, args.platform_metadata_config, args.product_metadata_csv, args.mmd_path, args.overwrite, args.filepath, args.json_metadata_filepath)