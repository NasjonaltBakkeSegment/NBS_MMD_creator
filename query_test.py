import requests

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
        raise ValueError('No metadata found for the given filename.')

product = 'S1C_S4_GRDH_1SDH_20250118T171404_20250118T171421_000638_000538_4B8B'

metadata,id = get_metadata_from_opensearch(product)

for key, val in metadata.items():
    print(key, val)