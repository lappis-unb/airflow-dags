import shutil
from contextlib import closing
from pathlib import Path
from typing import List

import pytest
import yaml
from faker import Faker

from plugins.yaml.config_reader import IsAFileError, read_yaml_files_from_directory


@pytest.fixture
def setup_yaml_tests():
    yamls_folder_root = Path(__file__).parent.joinpath("./yamls_tests")
    yamls_folder_root.mkdir()
    yamls_folder = yamls_folder_root

    fake = Faker()
    yamls_files: List[Path] = []
    for folder_deep in range(10):
        current_file = yamls_folder.joinpath(f"./{folder_deep}.yaml")
        with closing(open(current_file, "w")) as yaml_file:
            yaml_content = {
                "id": fake.uuid4(),
                "name": fake.name(),
                "address": fake.address(),
                "email": fake.email(),
                "phone_number": fake.phone_number(),
                "job": fake.job(),
                "date_of_birth": fake.date_of_birth(
                    minimum_age=18, maximum_age=65
                ).strftime("%Y-%m-%d"),
            }
            yaml.dump(yaml_content, yaml_file)
        yamls_files.append(yaml_content)

        yamls_folder = yamls_folder.joinpath(f"./{folder_deep}-folder")
        yamls_folder.mkdir()

    yield {"folder": yamls_folder_root, "expected": yamls_files}

    shutil.rmtree(yamls_folder_root)


@pytest.fixture
def setup_invalid_yaml():
    yamls_folder_root = Path(__file__).parent.joinpath("./yamls_tests")
    yamls_folder_root.mkdir()
    invalid_yaml = yamls_folder_root.joinpath("./invalid.yaml")
    invalid_yaml2 = yamls_folder_root.joinpath("./invalid2.yaml")
    invalid_yaml_content = """invalid_yaml: !!python/object/apply:os.system ["echo", "This is an intentional YAML error"]"""

    with closing(open(invalid_yaml, "w")) as yaml_file:
        yaml_file.write(invalid_yaml_content)

    with closing(open(invalid_yaml2, "w")) as yaml_file:
        yaml_file.write(invalid_yaml_content)

    yield yamls_folder_root

    shutil.rmtree(yamls_folder_root)


def test_success_read_yaml_from_tree(setup_yaml_tests):
    for expected, return_from_func in zip(
        sorted(
            read_yaml_files_from_directory(setup_yaml_tests["folder"]),
            key=lambda x: x["id"],
        ),
        sorted(setup_yaml_tests["expected"], key=lambda x: x["id"]),
    ):
        assert expected == return_from_func


def test_folder_not_exists():
    with pytest.raises(FileNotFoundError):
        next(read_yaml_files_from_directory("./FolderNotExists"))


def test_pass_file_to_read_yaml_from_directory():
    with pytest.raises(IsAFileError):
        next(read_yaml_files_from_directory(Path(__file__)))


def test_invalid_yaml(setup_invalid_yaml):
    with pytest.raises(yaml.YAMLError):
        next(read_yaml_files_from_directory(setup_invalid_yaml))
