import base64
#import boto3
import json
import os
import errno
import sys
import configparser
from contextlib import contextmanager
from time import perf_counter

# Loads the specified config file
# The config file is looked for within Python's path.
# If the config file isn't found, a FileNotFoundError is thrown.
def load_config(name):
    config_path_and_name = None
    config = configparser.ConfigParser()
    config_path_and_name = find_file(name)
    if config_path_and_name is None:
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), name)
    config.read(config_path_and_name, encoding="utf-8")
    return config


# Attempts to find the named file.
# If the path parameter is passed, the file will be looked for there, first; otherwise,
# the system path will be searched.
# If it's found, the full path and filename are returned.
# If it's not found, None is returned.
# This function is useful for finding a file when the full path may not be known.
def find_file(name, path=None):
    if path is not None:
        path_and_name = os.path.join(path, name)
        if os.is_file(path_and_name):
            print(f'Found file {path_and_name} in {path}.')
            return path_and_name
    for thePath in sys.path:
        for root, dirs, files in os.walk(thePath):
            if name in files:
                path_and_name = os.path.join(root, name)
                print(f'Found file {path_and_name} on the system path.')
                return path_and_name
    return None