import json
import sys
import os
import logging

logger = logging.getLogger("CONFIG LOADER")


def load_config() -> dict:
    possible_paths = ["config.json", "../config.json"]
    config_path = None

    for path in possible_paths:
        if os.path.exists(path):
            config_path = path
            break

    if not config_path:
        logger.critical("Critical Error: config.json file could not be found anywhere!")
        sys.exit(1)

    try:
        with open(config_path, "r") as config_file:
            return json.load(config_file)
    except Exception as e:
        logger.error(f"Error loading config.json file: {e}")
        sys.exit(1)
