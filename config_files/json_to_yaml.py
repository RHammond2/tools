"""
Converts JSON documents to YAML documents.

Python
------
3.8+

Dependencies
------------
pyyaml
"""


import os
import json
import argparse
from pathlib import Path
from yaml import dump

try:
    from yaml import CDumper as Dumper
except ImportError:
    from yaml import Dumper


def convert_file(name: str):
    """
    Converts a single file from JSON to YAML.

    Parameters
    ----------
    name : str
        Name of the absolute <path_to_file>/<`name`.json>
    """

    with open(name, "r") as f:
        data = json.load(f)

    name = name.replace("json", "yaml")
    dump(data, open(name, "w"), Dumper=Dumper)

def convert_tree(path: str):
    """
    Converts all JSON files in a directory tree to YAML.

    Parameters
    ----------
    path : str
        Directory to convert all files.
    """
    path = Path(path)
    for el in os.listdir(path):
        _path = os.path.join(path, el)
        if _path.endswith("json"):
            convert_file(_path)
        elif os.path.isdir(_path):
            convert_tree(_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Converts files/paths from JSON to YAML."
    )

    parser.add_argument(
        "-f",
        "--file",
        dest="filename",
        type=str,
        nargs="+",
        default=[],
        help=(
            "Name(s) of the file(s) to convert from JSON to YAML. Should"
            "contain the absolute path name"
        ),
    )
    parser.add_argument(
        "-p",
        "--path",
        dest="path",
        type=str,
        nargs="+",
        default=[],
        help=(
            "Name(s) of the directory tree(s) to convert all files from JSON"
            "to YAML"
        ),
    )

    args = parser.parse_args()

    for filename in args.filename:
        convert_file(filename)
        print(f"Converted {filename} to YAML")

    for path in args.path:
        convert_tree(path)
        print(f"Converted JSON files in {path} to YAML")
