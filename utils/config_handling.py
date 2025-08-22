import yaml
#import pandas as pd
from lxml import etree as ET
import os



def load_config(yaml_path):
    with open(yaml_path, 'r') as file:
        return yaml.safe_load(file)
    

def save_xml_to_file(xml_element, output_path):

    tree = ET.ElementTree(xml_element)
    output_path = output_path.split('.')[0]+'.xml'
    tree.write(output_path, encoding='utf-8', xml_declaration=True, pretty_print=True)


