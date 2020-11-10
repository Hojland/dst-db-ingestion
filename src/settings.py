import os
from pathlib import Path

MARIADB_CONFIG = {
    "user": os.environ["MARIADB_USR"],
    "psw": os.environ["MARIADB_PSW"],
    "host": "cubus.cxxwabvgrdub.eu-central-1.rds.amazonaws.com",
    "port": 3306,
    "db": "bbr",
}

METADATA_PATH = Path('tables')

DST_BASE_URL = 'https://api.statbank.dk/v1'