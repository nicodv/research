import yaml


def read_local_cfg():
    """Read local settings from a text file. This file should
    contain secret settings that are not to be checked into Git.
    """
    with open('/var/www/local_config.yml') as f:
        return yaml.load(f)
