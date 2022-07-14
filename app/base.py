
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

import signal
from datetime import datetime

import colored
from colored import stylize

from app.exceptions import ShutdownException
from app.models.codec import CodecMetadata
from app.models.node import HarvesterStatus, NodeBlockHeader, NodeBlockHeaderDigestLog, NodeBlockExtrinsic, \
    NodeBlockRuntime, NodeMetadata, NodeBlockStorage
from substrateinterface import SubstrateInterface


class Job:

    yield_per = 1000

    @property
    def icon(self) -> str:
        return ''

    def __init__(self, harvester):
        self.harvester = harvester
        self.substrate = harvester.substrate
        self.db_substrate = harvester.db_substrate
        self.session = harvester.session

    def log(self, message, verbose_level=1):
        self.harvester.log(f'{self.icon}  {message}', verbose_level)

    def start(self):
        for task in self.tasks:
            with GracefulInterruptHandler() as interrupt_handler:
                task.execute()
                if interrupt_handler.interrupted:
                    self.log("🛑 Warm shutdown initiated", 1)
                    raise ShutdownException()

    @staticmethod
    def format_hash(_hash: bytes):
        return f'0x{_hash.hex()[0:5]}...{_hash.hex()[-5:]}'


class DatabaseSubstrateInterface(SubstrateInterface):

    def __init__(self, **kwargs):
        self.db_session = kwargs.pop('db_session')
        self.verbose_level = kwargs.pop('verbose_level', 1)
        kwargs['url'] = 'http://dummy'
        super().__init__(**kwargs)

    def log(self, message, verbose_level=1):
        if verbose_level <= self.verbose_level:
            print(stylize(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), colored.fg("gray")), message)

    def get_block_hash(self, block_id):

        block = NodeBlockHeader.query(self.db_session).filter_by(block_number=block_id)

        if block:
            return '0x{}'.format(block.hash.hex())
        else:
            raise ValueError('Block not found')

    def rpc_request(self, method, params, result_handler=None):

        self.log("🔎 [{}]".format(method), 3)

        if method == 'system_name':
            item = HarvesterStatus.query(self.db_session).get('SYSTEM_NAME')
            return {"jsonrpc": "2.0", "result": item.value, "id": self.request_id}

        elif method == 'system_chain':
            item = HarvesterStatus.query(self.db_session).get('SYSTEM_CHAIN')
            return {"jsonrpc": "2.0", "result": item.value, "id": self.request_id}

        elif method == 'system_properties':
            item = HarvesterStatus.query(self.db_session).get('SYSTEM_PROPERTIES')
            return {"jsonrpc": "2.0", "result": item.value, "id": self.request_id}

        elif method == 'chain_getHeader':
            block = NodeBlockHeader.query(self.db_session).get(bytes.fromhex(params[0][2:]))
            if block:

                logs = NodeBlockHeaderDigestLog.query(self.db_session).filter_by(
                    block_hash=block.hash
                ).order_by('log_idx')

                return {
                    "jsonrpc": "2.0",
                    "result": {
                        "digest": {
                            "logs": [
                                '0x{}'.format(log.data.hex()) for log in logs
                            ]
                        },
                        "extrinsicsRoot": '0x{}'.format(block.extrinsics_root.hex()),
                        "number": '0x{}'.format(block.number.hex()),
                        "parentHash": '0x{}'.format(block.parent_hash.hex()),
                        "stateRoot": '0x{}'.format(block.state_root.hex()),
                    },
                    "id": self.request_id
                }
        elif method == 'chain_getBlock':
            block = NodeBlockHeader.query(self.db_session).get(bytes.fromhex(params[0][2:]))
            if block:

                extrinsics = NodeBlockExtrinsic.query(self.db_session).filter_by(
                    block_hash=block.hash
                ).order_by('extrinsic_idx')

                logs = NodeBlockHeaderDigestLog.query(self.db_session).filter_by(
                    block_hash=block.hash
                ).order_by('log_idx')

                return {
                    "jsonrpc": "2.0",
                    "result": {
                        "block": {
                            "extrinsics": [
                                '0x{}{}'.format(extrinsic.length.hex(), extrinsic.data.hex())
                                for extrinsic in extrinsics
                            ],
                            "header": {
                                "digest": {
                                    "logs": [
                                        '0x{}'.format(log.data.hex()) for log in logs
                                    ]
                                },
                                "extrinsicsRoot": '0x{}'.format(block.extrinsics_root.hex()),
                                "number": '0x{}'.format(block.number.hex()),
                                "parentHash": '0x{}'.format(block.parent_hash.hex()),
                                "stateRoot": '0x{}'.format(block.state_root.hex()),
                            }
                        },
                        "justification": None
                    },
                    "id": self.request_id
                }

        elif method == 'chain_getRuntimeVersion':
            block_runtime = NodeBlockRuntime.query(self.db_session).get(bytes.fromhex(params[0][2:]))

            if block_runtime:
                return {
                    "jsonrpc": "2.0",
                    "result": {
                        "apis": None,
                        "authoringVersion": None,
                        "implName": None,
                        "implVersion": None,
                        "specName": block_runtime.spec_name,
                        "specVersion": block_runtime.spec_version,
                        "transactionVersion": 1
                    },
                    "id": self.request_id
                }
            return {
                "jsonrpc": "2.0",
                "result": None,
                "id": self.request_id
            }

        elif method == 'state_getMetadata':
            block_runtime = NodeBlockRuntime.query(self.db_session).get(bytes.fromhex(params[0][2:]))
            metadata = NodeMetadata.query(self.db_session).filter_by(
                spec_name=block_runtime.spec_name, spec_version=block_runtime.spec_version
            ).one()

            return {
                "jsonrpc": "2.0",
                "result": '0x{}'.format(metadata.data.hex()),
                "id": self.request_id
            }
        elif method == 'state_getStorageAt':
            storage_entry = NodeBlockStorage.query(self.db_session).filter_by(
                block_hash=bytes.fromhex(params[1][2:]), storage_key=bytes.fromhex(params[0][2:])
            ).first()
            if storage_entry:
                return {"jsonrpc": "2.0", "result": storage_entry.data, "id": self.request_id}

            raise ValueError("NodeBlockStorage entry expected but not found")

        raise ValueError("No handler for method '{}'".format(method))

    def init_runtime(self, *args, **kwargs):
        super().init_runtime(*args, **kwargs)
        # Reset ss58_format to prevent automatic SS58 encoding of AccountIds
        self.runtime_config.ss58_format = None


class GracefulInterruptHandler(object):

    def __init__(self, sig=signal.SIGINT):
        self.sig = sig
        self.released = False

    def __enter__(self):
        self.interrupted = False
        self.released = False

        self.original_handler = signal.getsignal(self.sig)

        def handler(signum, frame):
            self.release()
            self.interrupted = True

        signal.signal(self.sig, handler)

        return self

    def __exit__(self, type, value, tb):
        self.release()

    def release(self):
        if self.released:
            return False

        signal.signal(self.sig, self.original_handler)

        self.released = True

        return True
