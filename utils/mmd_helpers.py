import os 
import re 
import json 
import yaml
import glob
from lxml import etree as ET
from datetime import datetime 
from utils.metadata_extraction import (
    get_collection_from_filename,
    get_product_metadata,
    generate_http_url
)
from utils.xml_creation import prepend_mmd,prepend_xml,prepend_gml
from utils.utils import (
    within_sios,get_size_mb,
    get_netcdf_checksum,
    get_zip_checksum
)


def get_parent_id(script_dir, platform, product_type):
    mapping_file = os.path.join(script_dir, "config", "parent_id_mapping.yaml")
    with open(mapping_file, "r") as file:
        mapping = yaml.safe_load(file)
    parent_id = mapping[platform][product_type]
    return parent_id


def get_id_from_mapping_file(script_dir, filename):
    try:
        # Regex pattern to match the first occurrence of a 4-digit year
        pattern = re.compile(r"(\d{4})")
        match = pattern.search(filename)
        year = str(match.group(1))
        mission = get_collection_from_filename(filename).replace("Sentinel", "Sentinel-").replace("Sentinel-5P", "Sentinel-5")
        filename_pattern = os.path.join(script_dir, f"mapping/{mission}_{year}0101-{year}*mapping*")
        matching_files = sorted(glob.glob(filename_pattern))
        mapping_file = matching_files[-1]

        with open(mapping_file, "r", encoding="utf-8") as file:
            dic = json.load(file)
        id = dic[filename.split(".")[0]]
        return id
    except Exception:
        return None


def create_xml(script_dir, metadata, id, global_attributes, platform_metadata, product_metadata_df, filename, filepath=None):

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
        gemet_resource.text = 'https://inspire.ec.europa.eu/theme'

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
    if 'cloudCover' in metadata.keys():
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

    parent_ID = get_parent_id(script_dir, filename_platform, product_metadata['product_type'])
    related_dataset = ET.SubElement(root,prepend_mmd('related_dataset'))
    related_dataset.attrib['relation_type'] = "parent"
    related_dataset.text = str(parent_ID)
    return root