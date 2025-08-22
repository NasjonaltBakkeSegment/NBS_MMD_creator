import os
import hashlib
import re
import zipfile
from shapely.geometry import Polygon, LinearRing, box


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

    lon_lat_pairs = [tuple(map(float, coord.split())) for coord in coords]

    lons, lats = zip(*lon_lat_pairs)

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
    #    try:
        # Attempt to parse coordinates directly from coord_strings
        linear_ring_coords = [tuple(map(float, coord.split())) for coord in coord_strings]

        # Create a Shapely LinearRing from the coordinates
        linear_ring = LinearRing(linear_ring_coords)

        # Check if the LinearRing intersects the SIOS polygon
        return linear_ring.intersects(sios_polygon)

    #     except ValueError:
    #         # If a ValueError occurs (e.g., invalid format like XML/GML), fallback to regex method
    #         linear_ring_coords = []

    #         # Regular expression to match coordinates (e.g., -8.766348 57.313187)
    #         coord_pattern = re.compile(r"(-?\d+\.\d+) (-?\d+\.\d+)")

    #         # Loop through each coordinate string
    #         for coord_string in coord_strings:
    #             # Find all coordinate pairs in the string using regex
    #             matches = coord_pattern.findall(coord_string)

    #             # Convert matched coordinates to tuples of floats and add to the list
    #             for match in matches:
    #                 linear_ring_coords.append((float(match[0]), float(match[1])))

    #         # Create a Shapely LinearRing from the coordinates
    #         linear_ring = LinearRing(linear_ring_coords)

    #         # Check if the LinearRing intersects the SIOS polygon
    #         return linear_ring.intersects(sios_polygon)

    # elif north is not None and south is not None and east is not None and west is not None:
    #     # Create a bounding box Polygon
    #     bbox = box(west, south, east, north)

    #     # Check if the bounding box intersects the SIOS polygon
    #     return bbox.intersects(sios_polygon)

    # return False  # Default return if neither condition is met