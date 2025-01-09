import requests
from lxml import etree as ET
import yaml
import sys
import os
import argparse
import hashlib
from datetime import datetime
from shapely.geometry import Polygon, box, LinearRing

def within_sios(coord_strings):
    # Parse the SIOS polygon
    sios_polygon = Polygon([
        (-20, 70),
        (-20, 90),
        (40, 90),
        (40, 70),
        (-20, 70)
    ])

    linear_ring_coords = [tuple(map(float, coord.split())) for coord in coord_strings]

    # Create a Shapely LinearRing from the coordinates
    linear_ring = LinearRing(linear_ring_coords)

    # Check if the LinearRing intersects the SIOS polygon
    in_polygon = linear_ring.intersects(sios_polygon)
    return in_polygon

def get_collection_from_filename(filename):
    if filename.startswith('S1'):
        return 'Sentinel1'
    elif filename.startswith('S2A') or filename.startswith('S2B'):
        return 'Sentinel2'
    elif filename.startswith('S3'):
        return 'Sentinel3'
    elif filename.startswith('S5P'):
        return 'Sentinel5P'
    elif filename.startswith('S6'):
        return 'Sentinel6'
    elif filename.startswith('S1A') or filename.startswith('S1B'):
        return 'Sentinel1RTC'
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
def infer_orbit_direction(metadata):
    start_time = metadata['startDate']
    if start_time:
        # Convert string to datetime object
        start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))

        # Assuming Sentinel-2 descending passes over equator around 10:30 AM local time
        if start_time.hour < 10 or (start_time.hour == 10 and start_time.minute < 30):
            return 'DESCENDING'
        else:
            return 'ASCENDING'

    return 'UNKNOWN'

def create_xml(metadata, id, global_data):

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
        ttt = metadata['gmlgeometry'].lstrip('<gml:Polygon srsName="EPSG:4326"><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>')
        ttt = ttt.rstrip('</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>')
        ttt = ttt.split(' ')
        for i,elem in enumerate(ttt):
            ttt[i] = elem.replace(',',' ')

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

    if metadata['platform'] in ['S1A', 'S1B']:
        iso_topics = global_data['S1']['iso_topic_category'].split(',')
    elif metadata['platform'] in ['S2A', 'S2B']:
        iso_topics = global_data['S2']['iso_topic_category'].split(',')
    else:
        iso_topics = []

    for topic in iso_topics:
        iso_topic_category = ET.SubElement(root, prepend_mmd('iso_topic_category'))
        iso_topic_category.text = topic.strip()

    # Retrieve the keywords for the specific platform
    if metadata['platform'] in ['S1A', 'S1B']:
        keywords = global_data['S1']['keywords'].split(',')
    elif metadata['platform'] in ['S2A', 'S2B']:
        keywords = global_data['S2']['keywords'].split(',')
    else:
        keywords = []

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
        gemet_sep = ET.SubElement(gemet_elem, prepend_mmd('separator'))


    if metadata['gmlgeometry'] is not None:
        ttt = metadata['gmlgeometry'].lstrip('<gml:Polygon srsName="EPSG:4326"><gml:outerBoundaryIs><gml:LinearRing><gml:coordinates>')
        ttt = ttt.rstrip('</gml:coordinates></gml:LinearRing></gml:outerBoundaryIs></gml:Polygon>')
        ttt = ttt.split(' ')
        for i,elem in enumerate(ttt):
            ttt[i] = elem.replace(',',' ')

        geographic_extent = ET.SubElement(root, prepend_mmd('geographic_extent'))
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
    file_name = ET.SubElement(storage_information, prepend_mmd('file_name'))
    file_name.text = metadata['title']
    file_format = ET.SubElement(storage_information, prepend_mmd('file_format'))
    file_format.text = 'SAFE'
    file_size = ET.SubElement(storage_information, prepend_mmd('file_size'))
    file_size.attrib['unit'] = 'MB'
    file_size_conv = metadata['services']['download']['size'] / 1048576
    file_size.text = f'{file_size_conv:.2f}'

    # Compute checksum for a specific file inside the .SAFE directory
    checksum = ET.SubElement(storage_information, prepend_mmd('checksum'))
    checksum.attrib['type'] = 'md5sum'

    safe_dir_path = metadata['title']
    specific_file_name = 'manifest.safe'
    file_path = os.path.join(safe_dir_path, specific_file_name)

    md5_check = hashlib.md5()
    try:
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_check.update(chunk)
        checksum.text = md5_check.hexdigest()
    except FileNotFoundError:
        checksum.text = 'File not found'
    except IsADirectoryError:
        checksum.text = 'Expected a file but found a directory'
    except Exception as e:
        checksum.text = str(e)

    project = ET.SubElement(root, prepend_mmd('project'))
    project_s_name = ET.SubElement(project, prepend_mmd('short_name'))
    project_s_name.text = global_data['global']['project_short_name']
    project_l_name = ET.SubElement(project, prepend_xml('long_name'))
    project_l_name.text = global_data['global']['project']

    platform = ET.SubElement(root, prepend_mmd('platform'))
    short_name = ET.SubElement(platform, prepend_mmd('short_name'))
    short_name.text = metadata['platform']
    long_name = ET.SubElement(platform, prepend_mmd('long_name'))
    long_name.text = metadata['platform']
    orbit_relative = ET.SubElement(platform, prepend_mmd('orbit_relative'))
    orbit_relative.text = str(metadata['relativeOrbitNumber'] )
    orbit_absolute = ET.SubElement(platform, prepend_mmd('orbit_absolute'))
    orbit_absolute.text = str(metadata['orbitNumber'])
    orbit_direction = ET.SubElement(platform, prepend_mmd('orbit_direction'))
    if metadata['orbitDirection'] is None:
        orbit_direction.text = infer_orbit_direction(metadata)
    else:
        orbit_direction.text = metadata['orbitDirection']

    instrument = ET.SubElement(platform, prepend_mmd('instrument'))
    s_name = ET.SubElement(instrument, prepend_mmd('short_name'))
    s_name.text = metadata['instrument']
    l_name = ET.SubElement(instrument, prepend_mmd('long_name'))
    if metadata['platform'] in ['S1A', 'S1B']:
        l_name.text = 'Synthetic Aperture Radar (C-band)'
    elif metadata['platform'] in ['S2A', 'S2B']:
        l_name.text = 'Multi-Spectral Imager for Sentinel-2'
    elif metadata['platform'] in ['S3A', 'S3B']:
        l_name.text = 'Sea and Land Surface Temperature Radiometer'
    elif metadata['platform'] in ['S5P']:
        l_name.text = 'Tropospheric Monitoring Instrument'
    else:
        l_name.text = 'Unknown Platform'

    instrument_resource = ET.SubElement(instrument, prepend_mmd('resource'))
    if metadata['platform'] in ['S1A','S1B']:
        instrument_resource.text = global_data['S1A']['instrument_vocabulary']
    elif metadata['platform'] == 'S2A':
        instrument_resource.text = global_data['S2A']['instrument_vocabulary']
    elif metadata['platform'] == 'S2B':
        instrument_resource.text = global_data['S2B']['instrument_vocabulary']
    elif metadata['platform'] == 'S3A':
        instrument_resource.text = 'https://space.oscar.wmo.int/satellites/view/sentinel_3a'
    elif metadata['platform'] == 'S3B':
        instrument_resource.text = 'https://space.oscar.wmo.int/satellites/view/sentinel_3b'
    elif metadata['platform'] == 'S5P':
        instrument_resource.text = 'https://space.oscar.wmo.int/satellites/view/sentinel_5p'
    else:
        instrument_resource.text = 'Unknown instrument_resource'

    mode = ET.SubElement(instrument, prepend_mmd('mode'))
    mode.text = metadata['sensorMode']

    if 'polarisation' in metadata:
        polarisation = ET.SubElement(instrument, prepend_mmd('polarisation'))
        polarisation.text = metadata['polarisation'].replace('&','+')

    product_type = ET.SubElement(instrument, prepend_mmd('product_type'))
    product_type.text = metadata['productType']

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

    if metadata['platform'] in ['S1A', 'S1B']:
        related_type.text = global_data['S1']['related_information_type']
        related_desc.text = global_data['S1']['related_information_description']
        related_res.text = global_data['S1']['related_information_resource']
    elif metadata['platform'] in ['S2A', 'S2B']:
        related_type.text = global_data['S2']['related_information_type']
        related_desc.text = global_data['S2']['related_information_description']
        related_res.text = global_data['S2']['related_information_resource']
    elif metadata['platform'] in ['S3A', 'S3B']:
        related_type.text = global_data['S2']['related_information_type']
        related_desc.text = global_data['S2']['related_information_description']
        related_res.text = 'https://sentiwiki.copernicus.eu/web/s3-mission'
    elif metadata['platform'] == 'S5P':
        related_type.text = global_data['S2']['related_information_type']
        related_desc.text = global_data['S2']['related_information_description']
        related_res.text = 'https://sentiwiki.copernicus.eu/web/s5p-mission'
    else:
        related_type.text = 'Unknown related information type'
        related_desc.text = 'Unknown related information description'
        related_res.text =  'Unknown related information resource'

    use_constraint = ET.SubElement(root, prepend_mmd('use_constraint'))
    license_text = ET.SubElement(use_constraint, prepend_mmd('license_text'))
    license_text.text = global_data['global']['license_text']

    # TODO: Add parent ID
    related_dataset = ET.SubElement(root, prepend_mmd('related_dataset'))
    related_dataset.attrib['relation_type'] = 'parent'
    related_dataset.text = 'Pending parent_id'

    return root

def save_xml_to_file(xml_element, output_path):

    tree = ET.ElementTree(xml_element)
    tree.write(output_path, encoding='utf-8', xml_declaration=True, pretty_print=True)

def main(filename, yaml_path, output_path, overwrite):
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

        mmd_xml = create_xml(metadata, id, global_attributes)
        save_xml_to_file(mmd_xml, output_path)
        print(f'Metadata XML file saved to {output_path}')
    except requests.exceptions.RequestException as e:
        print(f'Network error occurred: {e}')
    except Exception as e:
        print(f'An error occurred: {e}')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate an MMD file from Copernicus metadata.')
    parser.add_argument(
        '--filename', '-f', type=str, required=True,
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

    args = parser.parse_args()

    if os.path.isdir(args.mmd_path):
        print(f'Output path is a directory, not a file: {args.mmd_path}')
        sys.exit(1)

    main(args.filename, args.yaml_path, args.mmd_path, args.overwrite)
