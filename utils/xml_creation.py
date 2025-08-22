from lxml import etree as ET
from datetime import datetime
import os
import glob
import json
import re 
import pandas as pd


def prepend_mmd(tag: str) -> str:
        return f'{{http://www.met.no/schema/mmd}}{tag}'


def prepend_gml(tag: str) -> str:
        return f'{{http://www.opengis.net/gml}}{tag}'


def prepend_xml(tag: str) -> str:
        return f'{{http://www.w3.org/XML/1998/namespace}}{tag}'


def create_root_with_namespaces(tag: str, namespaces: dict) -> ET.Element:
    nsmap = {f'xmlns:{prefix}': uri for prefix, uri in namespaces.items()}
    return ET.Element(f'{{{namespaces["mmd"]}}}{tag}', nsmap)


