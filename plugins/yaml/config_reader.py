import logging
import os
from pathlib import Path
from typing import Any, Dict, Generator, Union

import yaml

from contextlib import closing
class IsAFileError(Exception):
    """Exceção indicando que um diretório era esperado, mas um arquivo foi fornecido."""

    pass

def load_yaml(file_path):
    with closing(open(file_path)) as file:
        return yaml.safe_load(file)

def dump_yaml(data, file_path):
    with closing(open(file_path, mode="w")) as file:
        yaml.safe_dump(data, file)


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
        IsAFileError: Se o caminho especificado for um arquiv.
    """
    if not Path(path_to_directory).exists():
        raise FileNotFoundError

    if Path(path_to_directory).is_file():
        raise IsAFileError

    for entry in os.scandir(path_to_directory):
        # Check if the file is a YAML file
        if entry.name.endswith(".yaml") or entry.name.endswith(".yml"):
            try:
                yield load_yaml(entry.path)
            except yaml.YAMLError as e:
                logging.error("Error reading %s: %s", entry.name, e)
                raise e
        elif entry.is_dir():
            yield from read_yaml_files_from_directory(entry.path)
