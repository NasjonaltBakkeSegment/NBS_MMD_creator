import os
import hashlib
import re
import zipfile
from shapely.geometry import Polygon, MultiPolygon, box
from shapely import wkt
from lxml import etree as ET

def extract_polygon(gmlgeometry: str):
    gmlgeometry = gmlgeometry.strip()

    # Case 1: WKT string
    if "SRID=4326;" in gmlgeometry:
        clean = gmlgeometry.split("SRID=4326;")[-1].rstrip("'")
        polygon = wkt.loads(clean)
        return polygon

    # Case 2: GML string
    elif gmlgeometry.startswith("<gml:"):
        try:
            # Add namespace if missing
            if 'xmlns:gml=' not in gmlgeometry:
                gmlgeometry = gmlgeometry.replace("<gml:Polygon",
                    '<gml:Polygon xmlns:gml="http://www.opengis.net/gml"', 1)

            root = ET.fromstring(gmlgeometry.encode("utf-8"))
            ns = {"gml": "http://www.opengis.net/gml"}

            def parse_coords(coords_text):
                coords = []
                for pair in coords_text.strip().split():
                    x, y = pair.split(",")
                    coords.append((float(x), float(y)))
                return coords

            # Exterior
            exterior_elem = root.find(".//gml:outerBoundaryIs/gml:LinearRing/gml:coordinates", ns)
            exterior_coords = parse_coords(exterior_elem.text)

            # Interiors
            interior_coords_list = []
            for interior_elem in root.findall(".//gml:innerBoundaryIs/gml:LinearRing/gml:coordinates", ns):
                interior_coords_list.append(parse_coords(interior_elem.text))

            polygon = Polygon(shell=exterior_coords, holes=interior_coords_list)
            return polygon

        except Exception as e:
            raise ValueError(f"Failed to parse GML Polygon: {e}")

    else:
        raise TypeError(f"Unsupported polygon format: {type(gmlgeometry)}")

def get_bounding_box(polygon):
    """
    Returns the bounding box of a Shapely polygon.
    """
    minx, miny, maxx, maxy = polygon.bounds
    north = maxy
    south = miny
    east = maxx
    west = minx
    return north, south, east, west

def extract_coordinates(xml_string):
    match = re.search(r"<gml:coordinates>(.*?)</gml:coordinates>", xml_string)
    if match:
        return match.group(1).strip()
    return ""

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


def within_sios(polygon=None, north=None, south=None, east=None, west=None):
    # Define the SIOS polygon (rough bounding box around Svalbard/Arctic region)
    sios_polygon = Polygon([
        (-20, 70),
        (-20, 90),
        (40, 90),
        (40, 70),
        (-20, 70)
    ])

    if polygon:
        # Ensure input is a Shapely Polygon or MultiPolygon
        if not isinstance(polygon, (Polygon, MultiPolygon)):
            raise TypeError("polygon must be a shapely.geometry.Polygon or MultiPolygon")

        # Return True if the geometry intersects with SIOS polygon
        return polygon.intersects(sios_polygon)

    elif north is not None and south is not None and east is not None and west is not None:
        # Create a bounding box Polygon
        bbox = box(west, south, east, north)

        # Check if the bounding box intersects the SIOS polygon
        return bbox.intersects(sios_polygon)

    return False  # Default return if neither condition is met