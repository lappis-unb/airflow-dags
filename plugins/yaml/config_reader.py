import logging
import os
from pathlib import Path
from typing import Any, Dict, Generator, Union

import yaml


def read_yaml_files_from_directory(
    path_to_directory: Union[str, Path]
) -> Generator[Dict[str, Any], Any, Any]:
    """
    Lê arquivos YAML do diretório especificado e gera seu conteúdo.

    Args:
    ----
        path_to_directory (Union[str, Path]): O caminho para o diretório contendo arquivos YAML.

    Yields:
    ------
        Generator[dict[str, Any], None, None]: Gerador com o yaml carregado em memoria.

    Raises:
    ------
        FileNotFoundError: Se o diretório especificado não existir.
        IsADirectoryError: Se o caminho especificado for um diretório.
    """
    for entry in os.scandir(path_to_directory):
        # Check if the file is a YAML file
        if entry.name.endswith(".yaml") or entry.name.endswith(".yml"):
            with open(entry.path) as file:
                try:
                    yield yaml.safe_load(file)
                except yaml.YAMLError as e:
                    logging.error("Error reading %s: %s", entry.name, e)
        elif entry.is_dir():
            yield from read_yaml_files_from_directory(entry.path)
