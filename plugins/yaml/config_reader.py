import os
import yaml
from pathlib import Path
from typing import Union
import logging

def read_yaml_files_from_directory(path_to_directory: Union[str, Path]):
    for filename in os.listdir(path_to_directory):
        # Check if the file is a YAML file
        if filename.endswith(".yaml") or filename.endswith(".yml"):
            filepath = os.path.join(path_to_directory, filename)
            with open(filepath, "r") as file:
                try:
                    yield yaml.safe_load(file)
                except yaml.YAMLError as e:
                    logging.error(f"Error reading {filename}: {e}")

