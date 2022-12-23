#  Polkascan Harvester
#
#  Copyright 2018-2022 Stichting Polkascan (Polkascan Foundation).
#  This file is part of Polkascan.
#
#  Polkascan is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Polkascan is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Polkascan. If not, see <http://www.gnu.org/licenses/>.
import os

DB_NAME = os.environ.get("DB_NAME", "polkascan")
DB_HOST = os.environ.get("DB_HOST", "mysql")
DB_PORT = os.environ.get("DB_PORT", 3306)
DB_USERNAME = os.environ.get("DB_USERNAME", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "root")

DB_CONNECTION = os.environ.get("DB_CONNECTION", "mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8mb4".format(
    DB_USERNAME, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
))

SUBSTRATE_RPC_URL = os.environ.get("SUBSTRATE_RPC_URL", "ws://localhost:9944/")

if os.environ.get("SUBSTRATE_SS58_FORMAT") is not None:
    SUBSTRATE_SS58_FORMAT = int(os.environ.get("SUBSTRATE_SS58_FORMAT"))
else:
    SUBSTRATE_SS58_FORMAT = None

NODE_TYPE = os.environ.get("NODE_TYPE")

SUBSTRATE_TREASURY_ACCOUNTS = [
    "6d6f646c70792f74727372790000000000000000000000000000000000000000",
    "6d6f646c70792f736f6369650000000000000000000000000000000000000000"
]

TYPE_REGISTRY = os.environ.get("TYPE_REGISTRY", None)

DEBUG = bool(os.environ.get("DEBUG", False))

STORAGE_KEY_EVENTS = bytes.fromhex('26aa394eea5630e07c48ae0c9558cef780d41e5e16056765bc8461851072c9d7')
STORAGE_KEY_EVENTS_LEGACY = bytes.fromhex('26aa394eea5630e07c48ae0c9558cef780d41e5e16056765bc8461851072c9d7')

if os.environ.get("INSTALLED_ETL_DATABASES"):
    INSTALLED_ETL_DATABASES = os.environ.get("INSTALLED_ETL_DATABASES").split(',')
else:
    INSTALLED_ETL_DATABASES = []

BLOCK_START = os.environ.get("BLOCK_START")
BLOCK_END = os.environ.get("BLOCK_END")

try:
    from app.local_settings import *
except ImportError:
    pass
