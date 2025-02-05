import requests
from lxml import etree as ET
import yaml
import sys
import os
import argparse
import hashlib
from datetime import datetime
from shapely.geometry import Polygon, LinearRing
import zipfile
import re

def within_sios(coord_strings):
    # Parse the SIOS polygon
    sios_polygon = Polygon([
        (-20, 70),
        (-20, 90),
        (40, 90),
        (40, 70),
        (-20, 70)
    ])

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

def get_collection_from_filename(filename):
    if filename.startswith('S1'):
        return 'Sentinel1'
    elif filename.startswith('S2'):
        return 'Sentinel2'
    elif filename.startswith('S3'):
        return 'Sentinel3'
    elif filename.startswith('S5P'):
        return 'Sentinel5P'
    elif filename.startswith('S6'):
        return 'Sentinel6'
    else:
        raise ValueError('Unknown filename prefix; unable to determine collection')

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
        response.raise_for_status()

def load_global_attributes(yaml_path):
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

def orbit_direction_from_data(filepath=None):
    '''
    Try to extract the orbit direction from the data file
    '''
    if filepath:
        filename = os.path.basename(filepath)
        if filename.endswith(".zip"):

            # File to extract from within the ZIP archive
            if filename.startswith('S1') or filename.startswith('S2'):
                source_file = filename.split('.')[0]+'.SAFE'
                xml_file = source_file + "/manifest.safe"
            elif filename.startswith('S3'):
                source_file = filename.split('.')[0]+'.SEN3'
                xml_file = 'xfdumanifest.xml'

            # Open the ZIP file and read the manifest.safe file
            with zipfile.ZipFile(filepath, 'r') as z:
                if xml_file in z.namelist():
                    with z.open(xml_file) as f:
                        tree = ET.parse(f)
                        root = tree.getroot()

                        # Extract namespaces
                        namespaces = root.nsmap

                        if filename.startswith('S1'):
                            pass_element = root.findall(".//s1:pass", namespaces=namespaces)
                            direction = pass_element[0].text
                            return direction.lower()
                        elif filename.startswith('S2') or filename.startswith('S3'):
                            if filename.startswith('S2'):
                                orbit_number_element = root.xpath("//safe:orbitNumber", namespaces=namespaces)
                            elif filename.startswith('S3'):
                                orbit_number_element = root.xpath("//sentinel-safe:orbitNumber", namespaces=namespaces)
                            if orbit_number_element:
                                direction = orbit_number_element[0].get('groundTrackDirection')
                                return direction.lower()
                            else:
                                return ''
                        else:
                            return ''
                else:
                    return ''
        elif filename.startswith('S5'):
            return None
        else:
            return 'UNKNOWN'
    else:
        return 'UNKNOWN'

def extract_coordinates(xml_string):
    match = re.search(r"<gml:coordinates>(.*?)</gml:coordinates>", xml_string)
    if match:
        return match.group(1).strip()
    return ""

def get_bounding_box(coords):
    """
    Given a list of coordinate strings in the format 'longitude latitude',
    returns the bounding box as (north, south, east, west).
    """
    lon_lat_pairs = [tuple(map(float, coord.split())) for coord in coords]

    lons, lats = zip(*lon_lat_pairs)

    north = max(lats)
    south = min(lats)
    east = max(lons)
    west = min(lons)

    return north, south, east, west

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

def create_xml(metadata, id, global_data, filename, filepath=None):

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
    title.text = metadata['title'].split('.')[0]

    abstract = ET.SubElement(root, prepend_mmd('abstract'))
    abstract.attrib[prepend_xml('lang')] = 'en'
    abstract.text = metadata['description']

    metadata_status = ET.SubElement(root, prepend_mmd('metadata_status'))
    metadata_status.text = global_data['global']['metadata_status']

    dataset_production_status = ET.SubElement(root, prepend_mmd('dataset_production_status'))
    dataset_production_status.text = global_data['global']['dataset_production_status']

    collection = ET.SubElement(root, prepend_mmd('collection'))
    collection.text = 'NBS'

    if metadata['gmlgeometry'] is not None:
        ttt = extract_coordinates(metadata['gmlgeometry'])
        ttt = ttt.split(' ')
        for i,elem in enumerate(ttt):
            ttt[i] = elem.replace(',',' ')

        northmost, southmost, eastmost, westmost = get_bounding_box(ttt)

        if within_sios(ttt):
            collection = ET.SubElement(root, prepend_mmd('collection'))
            collection.text = 'SIOS'
        else:
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

    # TODO: Add keywords and iso topics for S6
    if metadata['platform'].startswith('S1'):
        satellites = ['A', 'B', 'C', 'D']
        for sat in satellites:
            if filename.startswith(f'S1{sat}'):
                iso_topics = global_data[f'S1{sat}']['iso_topic_category'].split(',')
                keywords = global_data[f'S1{sat}']['keywords'].split(',')
                break
    elif metadata['platform'].startswith('S2'):
        satellites = ['A', 'B', 'C', 'D']
        for sat in satellites:
            if filename.startswith(f'S2{sat}'):
                iso_topics = global_data[f'S2{sat}']['iso_topic_category'].split(',')
                keywords = global_data[f'S2{sat}']['keywords'].split(',')
                break
    elif metadata['platform'].startswith('S3'):
        products = ['OL', 'SL', 'SY', 'SR']
        satellites = ['A', 'B', 'C', 'D']
        for sat in satellites:
            for product in products:
                if filename.startswith(f'S3{sat}_{product}'):
                    iso_topics = global_data[f'S3_{product}']['iso_topic_category'].split(',')
                    keywords = global_data[f'S3_{product}']['keywords'].split(',')
                    break
    elif metadata['platform'].startswith('S5'):
        iso_topics = global_data['S5P']['iso_topic_category'].split(',')
        keywords = global_data['S5P']['keywords'].split(',')
    else:
        iso_topics = []
        keywords = []

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

    if metadata['gmlgeometry'] is not None:
        geographic_extent = ET.SubElement(root, prepend_mmd('geographic_extent'))
        rectangle = ET.SubElement(geographic_extent, prepend_mmd('rectangle'))
        rectangle.attrib['srsName'] = 'EPSG:4326'
        north = ET.SubElement(rectangle, prepend_mmd('north'))
        south = ET.SubElement(rectangle, prepend_mmd('south'))
        east = ET.SubElement(rectangle, prepend_mmd('east'))
        west = ET.SubElement(rectangle, prepend_mmd('west'))
        north.text = str(northmost)
        south.text = str(southmost)
        east.text = str(eastmost)
        west.text = str(westmost)
        polygon = ET.SubElement(geographic_extent, prepend_mmd('polygon'))
        sub_poly = ET.SubElement(polygon, prepend_gml('Polygon'))
        sub_poly.attrib['id'] = 'polygon'
        sub_poly.attrib['srsName'] = 'EPSG:4326'
        exterior = ET.SubElement(sub_poly, prepend_gml('exterior'))
        linear_ring = ET.SubElement(exterior, prepend_gml('LinearRing'))
        for elem in ttt:
            pos = ET.SubElement(linear_ring, prepend_gml('pos'))
            pos.text = elem
    else:
        print('Warning: gmlgeometry is None. Geographic extent will not be included in the XML.')

    dataset_lang = ET.SubElement(root, prepend_mmd('dataset_language'))
    dataset_lang.text = global_data['global']['dataset_language']

    operational_status = ET.SubElement(root, prepend_mmd('operational_status'))
    operational_status.text = global_data['global']['processing_level']

    access_cons = ET.SubElement(root, prepend_mmd('access_constraint'))
    access_cons.text = global_data['global']['access_constraint']

    personnel_1 = ET.SubElement(root, prepend_mmd('personnel'))
    role = ET.SubElement(personnel_1, prepend_mmd('role'))
    role.text = global_data['global']['creator_role']
    name = ET.SubElement(personnel_1, prepend_mmd('name'))
    name.text = global_data['global']['creator_name']
    email = ET.SubElement(personnel_1, prepend_mmd('email'))
    email.text = global_data['global']['creator_email']
    organisation = ET.SubElement(personnel_1, prepend_mmd('organisation'))
    organisation.text = global_data['global']['creator_institution']

    personnel_2 = ET.SubElement(root, prepend_mmd('personnel'))
    role_2 = ET.SubElement(personnel_2, prepend_mmd('role'))
    role_2.text = global_data['global']['contributor_role']
    name_2 = ET.SubElement(personnel_2, prepend_mmd('name'))
    name_2.text = global_data['global']['contributor_name']
    email_2 = ET.SubElement(personnel_2, prepend_mmd('email'))
    email_2.text = global_data['global']['contributor_email']
    organisation_2 = ET.SubElement(personnel_2, prepend_mmd('organisation'))
    organisation_2.text = global_data['global']['contributor_institution']

    data_center = ET.SubElement(root, prepend_mmd('data_center'))
    data_center_name = ET.SubElement(data_center, prepend_mmd('data_center_name'))
    data_center_short = ET.SubElement(data_center_name, prepend_mmd('short_name'))
    data_center_short.text = 'METNO'
    data_center_long = ET.SubElement(data_center_name, prepend_mmd('long_name'))
    data_center_long.text = 'Norwegian Meteorological Institute'
    data_center_url = ET.SubElement(data_center, prepend_mmd('data_center_url'))
    data_center_url.text = global_data['global']['creator_url']

    storage_information = ET.SubElement(root, prepend_mmd('storage_information'))
    file_extension = os.path.splitext(filepath)[1].lower()  # Get the file extension (e.g., '.zip' or '.nc')
    file_name = ET.SubElement(storage_information, prepend_mmd('file_name'))
    file_name.text = metadata['title']
    file_format = ET.SubElement(storage_information, prepend_mmd('file_format'))
    if file_extension in ['.zip','SAFE']:
        file_format.text = 'SAFE'
    elif file_extension == '.nc':
        file_format.text = 'NetCDF'
    file_size = ET.SubElement(storage_information, prepend_mmd('file_size'))
    file_size.attrib['unit'] = 'MB'
    file_size_conv = metadata['services']['download']['size'] / 1048576
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
    project_s_name.text = global_data['global']['project_short_name']
    project_l_name = ET.SubElement(project, prepend_mmd('long_name'))
    project_l_name.text = global_data['global']['project']

    platform = ET.SubElement(root, prepend_mmd('platform'))
    platform_short_name = ET.SubElement(platform, prepend_mmd('short_name'))
    platform_long_name = ET.SubElement(platform, prepend_mmd('long_name'))
    platform_short_name.text = metadata['platform'].replace('S','Sentinel-')
    platform_long_name.text = metadata['platform'].replace('S','Sentinel-')
    platform_resource = ET.SubElement(platform, prepend_mmd('resource'))
    orbit_relative = ET.SubElement(platform, prepend_mmd('orbit_relative'))
    orbit_relative.text = str(metadata['relativeOrbitNumber'] )
    orbit_absolute = ET.SubElement(platform, prepend_mmd('orbit_absolute'))
    orbit_absolute.text = str(metadata['orbitNumber'])
    if metadata['orbitDirection'] is None:
        direction = orbit_direction_from_data(filepath)
        if direction:
            orbit_direction = ET.SubElement(platform, prepend_mmd('orbit_direction'))
            orbit_direction.text = direction.lower()
        else:
            # Can't currently find the orbit direction for S5 products
            pass
    else:
        orbit_direction = ET.SubElement(platform, prepend_mmd('orbit_direction'))
        orbit_direction.text = metadata['orbitDirection'].lower()
    instrument = ET.SubElement(platform, prepend_mmd('instrument'))
    s_name = ET.SubElement(instrument, prepend_mmd('short_name'))
    s_name.text = metadata['instrument']
    l_name = ET.SubElement(instrument, prepend_mmd('long_name'))
    instrument_resource = ET.SubElement(instrument, prepend_mmd('resource'))

    if filename.startswith('S1'):
        mode = ET.SubElement(instrument, prepend_mmd('mode'))
        mode.text = metadata['sensorMode']
        if 'polarisation' in metadata:
            polarisation = ET.SubElement(instrument, prepend_mmd('polarisation'))
            polarisation.text = metadata['polarisation'].replace('&','+')

    product_type = ET.SubElement(instrument, prepend_mmd('product_type'))
    if metadata['platform'].startswith('S1'):
        product_types = ['SLC','GRD','OCN']
        for p in product_types:
            if p in metadata['productType']:
                product_type.text = p
                break
    elif metadata['platform'].startswith('S2'):
        satellites = ['A', 'B', 'C', 'D']
        products = ['MSIL1C','MSIL2A']
        for sat in satellites:
            for p in products:
                if filename.startswith(f'S2{sat}_{p}'):
                    product_type.text = 'S2' + p.replace('L','')
                    break
    elif metadata['platform'].startswith('S3'):
        product_type.text = filename[4:15]
    elif metadata['platform'].startswith('S5'):
        product_type.text = filename[9:19]
    else:
        product_types = []

    ancillary = ET.SubElement(platform, prepend_mmd('ancillary'))
    cloud_coverage = ET.SubElement(ancillary, prepend_mmd('cloud_coverage'))
    cloud_coverage.text =  str(metadata['cloudCover'])

    spatial_representation = ET.SubElement(root, prepend_mmd('spatial_representation'))
    spatial_representation.text = global_data['global']['spatial_representation']

    activity_type = ET.SubElement(root, prepend_mmd('activity_type'))
    activity_type.text = global_data['global']['source']

    dataset_citation = ET.SubElement(root, prepend_mmd('dataset_citation'))
    dataset_citation_author = ET.SubElement(dataset_citation, prepend_mmd('author'))
    dataset_citation_author.text = global_data['global']['creator_name']
    dataset_citation_title = ET.SubElement(dataset_citation, prepend_mmd('title'))
    dataset_citation_title.text = metadata['title'].split('.')[0]

    related_information = ET.SubElement(root, prepend_mmd('related_information'))
    related_type = ET.SubElement(related_information, prepend_mmd('type'))
    related_desc = ET.SubElement(related_information, prepend_mmd('description'))
    related_res = ET.SubElement(related_information, prepend_mmd('resource'))

    # TODO: Add for S6
    if metadata['platform'].startswith('S1'):
        satellites = ['A', 'B', 'C', 'D']
        for sat in satellites:
            if filename.startswith(f'S1{sat}'):
                l_name.text = global_data[f'S1{sat}']['instrument']
                instrument_resource.text = global_data[f'S1{sat}']['instrument_vocabulary']
                platform_resource.text = global_data[f'S1{sat}']['platform_vocabulary']
                related_type.text = global_data[f'S1{sat}']['related_information_type']
                related_desc.text = global_data[f'S1{sat}']['related_information_description']
                related_res.text = global_data[f'S1{sat}']['related_information_resource']
                break
    elif metadata['platform'].startswith('S2'):
        satellites = ['A', 'B', 'C', 'D']
        for sat in satellites:
            if filename.startswith(f'S2{sat}'):
                l_name.text = global_data[f'S2{sat}']['instrument']
                instrument_resource.text = global_data[f'S2{sat}']['instrument_vocabulary']
                platform_resource.text = global_data[f'S2{sat}']['platform_vocabulary']
                related_type.text = global_data[f'S2{sat}']['related_information_type']
                related_desc.text = global_data[f'S2{sat}']['related_information_description']
                related_res.text = global_data[f'S2{sat}']['related_information_resource']
                break
    elif metadata['platform'].startswith('S3'):
        products = ['OL', 'SL', 'SY', 'SR']
        satellites = ['A', 'B', 'C', 'D']
        for sat in satellites:
            for product in products:
                if filename.startswith(f'S3{sat}_{product}'):
                    l_name.text = global_data[f'S3_{product}']['instrument']
                    instrument_resource.text = global_data[f'S3_{product}']['instrument_vocabulary']
                    related_type.text = global_data[f'S3_{product}']['related_information_type']
                    related_desc.text = global_data[f'S3_{product}']['related_information_description']
                    related_res.text = global_data[f'S3_{product}']['related_information_resource']
                    platform_resource.text = global_data[f'S3{sat}']['platform_vocabulary']
                    break
    elif metadata['platform'].startswith('S5'):
        l_name.text = global_data['S5P']['instrument']
        instrument_resource.text = global_data['S5P']['instrument_vocabulary']
        platform_resource.text = global_data[f'S5P']['platform_vocabulary']
        related_type.text = global_data[f'S5P']['related_information_type']
        related_desc.text = global_data[f'S5P']['related_information_description']
        related_res.text = global_data[f'S5P']['related_information_resource']
    elif metadata['platform'].startswith('S6'):
        satellites = ['A', 'B', 'C']
        for sat in satellites:
            if filename.startswith(f'S6{sat}'):
                platform_resource.text = global_data[f'S6{sat}']['platform_vocabulary']
                break
    else:
        l_name.text = 'Unknown Instrument'
        instrument_resource.text = 'Unknown instrument_resource'
        platform_resource.text = 'Unknown platform resource'
        related_type.text = 'Unknown related information type'
        related_desc.text = 'Unknown related information description'
        related_res.text =  'Unknown related information resource'

    use_constraint = ET.SubElement(root, prepend_mmd('use_constraint'))
    license_text = ET.SubElement(use_constraint, prepend_mmd('license_text'))
    license_text.text = global_data['global']['license_text']

    data_access = ET.SubElement(root,prepend_mmd('data_access'))
    da_type = ET.SubElement(data_access, prepend_mmd('type'))
    da_type.text = 'ODATA'
    da_description = ET.SubElement(data_access,prepend_mmd('description'))
    da_description.text = 'Open Data Protocol.'
    da_resource = ET.SubElement(data_access,prepend_mmd('resource'))
    da_resource.text = f"https://colhub-archive.met.no/odata/v1/Products('{id}')/$value"
    return root

def save_xml_to_file(xml_element, output_path):

    tree = ET.ElementTree(xml_element)
    tree.write(output_path, encoding='utf-8', xml_declaration=True, pretty_print=True)

def main(filename, yaml_path, output_path, overwrite, filepath):
    try:
        metadata, id = get_metadata_from_opensearch(filename)
        global_attributes = load_global_attributes(yaml_path)

        if os.path.exists(output_path):
            try:
                myxml = ET.parse(output_path)
                myroot = myxml.getroot()
                existing_elem = myroot.find("related_dataset")
                if existing_elem is not None and not overwrite:
                    print(f'Already specified, not changing anything in {output_path}')
                    sys.exit()
            except ET.ParseError:
                print(f"Couldn't parse existing file: {output_path}")

        mmd_xml = create_xml(metadata, id, global_attributes, filename, filepath)
        save_xml_to_file(mmd_xml, output_path)
        print(f'Metadata XML file saved to {output_path}')
    except requests.exceptions.RequestException as e:
        print(f'Network error occurred: {e}')
    except Exception as e:
        print(f'An error occurred: {e}')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate an MMD file from Copernicus metadata.')
    parser.add_argument(
        '--product', '-p', type=str, required=True,
        help='The product filename to fetch metadata for.'
    )
    parser.add_argument(
        '--yaml_path', '-y', type=str, required=True,
        help='Path to the YAML file with global attributes.'
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

    args = parser.parse_args()

    if os.path.isdir(args.mmd_path):
        print(f'Output path is a directory, not a file: {args.mmd_path}')
        sys.exit(1)

    main(args.product, args.yaml_path, args.mmd_path, args.overwrite, args.filepath)
