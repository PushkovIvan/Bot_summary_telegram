import os
import yaml

def get_config():
    path_to_config = "config.yaml"
    yaml_config_path = os.getenv("APP_CONFIG_FILE_PATH", path_to_config)
    with open(yaml_config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config

CONFIG = get_config()
