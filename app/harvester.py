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
import argparse

from tabulate import tabulate

from app import settings as app_settings, __version__, jobs

from datetime import datetime
import colored
from colored import stylize

from app.base import DatabaseSubstrateInterface, Job
from time import sleep
from websocket import WebSocketConnectionClosedException, WebSocketBadStatusException
from prometheus_client import start_http_server, Counter, Enum, Histogram

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from substrateinterface import SubstrateInterface
from substrateinterface.exceptions import SubstrateRequestException

from app.exceptions import ShutdownException, BlockDecodeException

from app.models.node import HarvesterStatus, HarvesterStorageCron, HarvesterStorageTask



class Harvester:

    def __init__(self, settings, type='full', verbose_level=1, cron_delay=60, force_start=False,
                 prometheus_endpoint=False):

        self.settings = settings
        self.verbose_level = verbose_level
        self.type = type
        self.cron_delay = cron_delay
        self.prometheus_endpoint = prometheus_endpoint

        self.storage_cron_entries = []

        if not hasattr(self.settings, 'DB_CONNECTION') or self.settings.DB_CONNECTION is None:
            raise ValueError("'DB_CONNECTION' not defined")

        if not hasattr(self.settings, 'TYPE_REGISTRY'):
            raise ValueError("'TYPE_REGISTRY' not in settings")

        if not hasattr(self.settings, 'SUBSTRATE_SS58_FORMAT'):
            raise ValueError("'SUBSTRATE_SS58_FORMAT' not in settings")

        self.engine = create_engine(settings.DB_CONNECTION, echo=False, pool_pre_ping=True)
        session_factory = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

        self.session = scoped_session(session_factory)

        # Set storage key to metadata > 9 version as default
        self.event_storage_key = settings.STORAGE_KEY_EVENTS

        self.substrate = SubstrateInterface(
            url=self.settings.SUBSTRATE_RPC_URL,
            ss58_format=self.settings.SUBSTRATE_SS58_FORMAT,
            type_registry_preset=self.settings.TYPE_REGISTRY
        )
        # Disable automatic SS58 encoding
        self.substrate.runtime_config.ss58_format = None

        self.db_substrate = DatabaseSubstrateInterface(
            db_session=self.session,
            ss58_format=self.settings.SUBSTRATE_SS58_FORMAT,
            type_registry_preset=self.settings.TYPE_REGISTRY,
            auto_discover=False
        )
        # Disable automatic SS58 encoding
        self.db_substrate.runtime_config.ss58_format = None

        self.jobs = {}

        self.prom_block_process_speed = Histogram('block_process_speed', 'Block process speed')

        self.force_start = force_start

    def init(self):

        # Add jobs
        self.add_job('cron', jobs.Cron)
        self.add_job('retrieve_blocks', jobs.RetrieveBlocks)
        self.add_job('retrieve_runtime_state', jobs.RetrieveRuntimeState)
        self.add_job('scale_decode', jobs.ScaleDecode)
        self.add_job('etl_process', jobs.EtlProcess)
        self.add_job('storage_tasks', jobs.StorageTask)

        self.prom_current_job = Enum('current_job', 'Current Job', states=[
            'cron', 'retrieve_blocks', 'retrieve_runtime_state', 'scale_decode', 'etl_process', 'storage_tasks', '-'
        ])

        # Check if status records are present
        if HarvesterStatus.query(self.session).count() == 0:
            # Create status records
            record = HarvesterStatus(
                key='SYSTEM_CHAIN',
                description='Blockchain name',
                value=self.rpc_call('system_chain', []).get('result')
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='SYSTEM_NAME',
                description='Blockchain client name',
                value=self.rpc_call('system_name', []).get('result')
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='SYSTEM_PROPERTIES',
                description='Blockchain properties',
                value=self.rpc_call('system_properties', []).get('result')
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='SYSTEM_VERSION',
                description='Blockchain client version',
                value=self.rpc_call('system_version', []).get('result')
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='CHAINTIP_BLOCKNUMBER',
                description='Blocknumber of chaintip'
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='CHAINTIP_HASH',
                description='Hash of chaintip'
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='FINALIZATION_BLOCKNUMBER',
                description='Blocknumber of finalization head'
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='FINALIZATION_HASH',
                description='Hash of finalization head'
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='NODE_TYPE',
                description='Type of node: archive, full or light',
                value=self.type
            )
            record.save(self.session)

            record = HarvesterStatus(
                key='PROCESS_BLOCKS_MAX_BLOCKNUMBER',
                description='Max blocknumber of blocks process'
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='PROCESS_STATE_MAX_BLOCKNUMBER',
                description='Max blocknumber of state process'
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='PROCESS_DECODER_MAX_BLOCKNUMBER',
                description='Max blocknumber of decoder process'
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='PROCESS_ETL_EXPLORER_LOGS_MAX_BLOCKNUMBER',
                description='Max blocknumber of ETL explorer logs process'
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='PROCESS_ETL_EXPLORER_TRANSFERS_MAX_BLOCKNUMBER',
                description='Max blocknumber of ETL explorer transfers process'
            )
            record.save(self.session)

            record = HarvesterStatus(
                key='ENABLE_HARVESTER_BLOCK',
                description='Enable/disable block harvester',
                value=1
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='ENABLE_HARVESTER_STATE',
                description='Enable/disable state harvester',
                value=1
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='ENABLE_HARVESTER_DECODER',
                description='Enable/disable decoder harvester',
                value=1
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='ENABLE_HARVESTER_ETL',
                description='Enable/disable etl harvester',
                value=1
            )
            record.save(self.session)
            record = HarvesterStatus(
                key='ENABLE_HARVESTER',
                description='Enable/disable harvester (master switch)',
                value=1
            )
            record.save(self.session)

            # Add default storage cron records
            record = HarvesterStorageCron(
                block_number_interval=1,
                storage_module="System",
                storage_name="Events"
            )

            record.save(self.session)

        else:
            record = HarvesterStatus.query(self.session).get('NODE_TYPE')
            if record:
                self.type = record.value

            # Sanity check if system_chain matches chain in database
            db_chain = HarvesterStatus.query(self.session).get('SYSTEM_CHAIN').value
            current_chain = self.rpc_call('system_chain', []).get('result')
            if db_chain != current_chain:
                raise ValueError(
                    f"Connected chain '{current_chain}' does not match chain '{db_chain}' in database"
                )

            # Check for force start
            if self.force_start:
                record = HarvesterStatus.query(self.session).get('ENABLE_HARVESTER')
                if record:
                    record.value = 1
                    record.save(self.session)

        # Load all settings
        for item in HarvesterStatus.query(self.session).all():
            setattr(self.settings, item.key, item.value)

        self.db_substrate = DatabaseSubstrateInterface(
            db_session=self.session,
            ss58_format=self.settings.SUBSTRATE_SS58_FORMAT,
            type_registry_preset=self.settings.TYPE_REGISTRY
        )

        self.storage_cron_entries = HarvesterStorageCron.query(self.session)

    def log(self, message, verbose_level=1):
        if verbose_level <= self.verbose_level:
            print(stylize(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), colored.fg("dark_gray")), message)

    def rpc_call(self, method, params, result_handler=None):
        response = self.substrate.rpc_request(method, params, result_handler=result_handler)
        if 'error' in response:
            raise ValueError(response['error']['data'])
        return response

    def add_job(self, name: str, job):

        if not issubclass(job, Job):
            raise ValueError('No subclass of Job')

        self.jobs[name] = job(harvester=self)

    def process_job(self, name):
        if name in self.jobs:
            self.log(stylize(f'🟢 Job "{name}" started'.ljust(60), colored.bg(235) + colored.fg(246)))
            self.prom_current_job.state(name)
            self.jobs[name].start()
            self.prom_current_job.state('-')

    def run(self, action):

        if self.prometheus_endpoint:
            start_http_server(9616)

        prom_counter = Counter('job_count', 'Jobs processed')

        try:
            self.init()

            while True:

                # Reload settings
                for item in HarvesterStatus.query(self.session).all():
                    setattr(self.settings, item.key, item.value)

                try:
                    if action in ['cron', 'all']:
                        if getattr(self.settings, 'ENABLE_HARVESTER', 0):
                            self.process_job('storage_tasks')
                            self.process_job('cron')
                    if action in ['blocks', 'all']:

                        if getattr(self.settings, 'ENABLE_HARVESTER', 0) and \
                                getattr(self.settings, 'ENABLE_HARVESTER_BLOCK', 0):
                            self.process_job('retrieve_blocks')
                        else:
                            self.log("⏸  Job 'retrieve_blocks' paused", 1)

                    if action in ['state', 'all'] and self.type == 'archive':

                        if getattr(self.settings, 'ENABLE_HARVESTER', 0) and \
                                getattr(self.settings, 'ENABLE_HARVESTER_STATE', 0):
                            self.process_job('retrieve_runtime_state')
                        else:
                            self.log("⏸  Job 'retrieve_runtime_state' paused", 1)

                    if action in ['decode', 'all'] and self.type == 'archive':

                        if getattr(self.settings, 'ENABLE_HARVESTER', 0) and \
                                getattr(self.settings, 'ENABLE_HARVESTER_DECODER', 0):
                            self.process_job('scale_decode')
                        else:
                            self.log("⏸  Job 'scale_decode' paused", 1)

                    if action in ['etl', 'all'] and self.type == 'archive':

                        if getattr(self.settings, 'ENABLE_HARVESTER', 0) and \
                                getattr(self.settings, 'ENABLE_HARVESTER_ETL', 0):
                            self.process_job('etl_process')
                        else:
                            self.log("⏸  Job 'etl_process' paused", 1)

                except BlockDecodeException as e:
                    self.log("⛔ An error occurred: '{}' Restarting ...".format(e))

                except (WebSocketConnectionClosedException, ConnectionRefusedError,
                        WebSocketBadStatusException, BrokenPipeError, SubstrateRequestException) as e:
                    # reestablish connection
                    self.log("⛔ Connection lost: '{}' Reconnecting ...".format(e))
                    try:
                        self.substrate.connect_websocket()
                    except (ConnectionRefusedError, WebSocketBadStatusException, BrokenPipeError,
                            SubstrateRequestException) as e:
                        self.log("⛔ Reconnect failed, retrying in 30 seconds ".format(e))
                        sleep(27)

                # Commit session
                self.session.commit()
                self.log(stylize('⏸️  Jobs finished'.ljust(62), colored.bg(235) + colored.fg(246)))
                prom_counter.inc()
                sleep(3)
        except (ShutdownException, KeyboardInterrupt):
            self.log(stylize("🛑 Shutdown finished".ljust(60), colored.bg(235) + colored.fg(246)))

    def list_storage_tasks(self):

        rows = [
            [item.id, item.storage_pallet, item.storage_name, item.blocks, item.complete]
            for item in HarvesterStorageTask.query(self.session).all()
        ]
        print(tabulate(rows, headers=['Id', 'Pallet', 'Storage name', 'Blocks', 'Complete']))

    def clean_storage_tasks(self):
        HarvesterStorageTask.query(self.session).filter_by(complete=True).delete()
        self.session.commit()

    def add_storage_task(self, pallet: str, storage_function: str, blocks: dict, description=None):

        task = HarvesterStorageTask(
            storage_pallet=pallet,
            storage_name=storage_function,
            blocks=blocks,
            complete=False,
            description=description
        )
        task.save(self.session)
        self.session.commit()

    def remove_storage_task(self, task_id):
        cron = HarvesterStorageTask.query(self.session).get(task_id)
        self.session.delete(cron)
        self.session.commit()

    def list_storage_cron(self):

        rows = [
            [item.id, item.block_number_interval, item.storage_module, item.storage_name]
            for item in HarvesterStorageCron.query(self.session).all()
        ]
        print(tabulate(rows, headers=['Id', 'Block interval', 'Pallet', 'Storage name']))

    def add_storage_cron(self, block_interval: int, pallet: str, storage_function: str):
        cron = HarvesterStorageCron(
            block_number_interval=block_interval,
            storage_module=pallet,
            storage_name=storage_function
        )
        cron.save(self.session)
        self.session.commit()

    def remove_storage_cron(self, cron_id):
        cron = HarvesterStorageCron.query(self.session).get(cron_id)
        self.session.delete(cron)
        self.session.commit()


if __name__ == '__main__':

    from app import cli

    parser = argparse.ArgumentParser(description='Polkascan harvester V2 application')
    parser.add_argument('--verbose', action='store_true', help='Verbose more')
    parser.add_argument('--prometheus', action='store_true', help='Expose Promotheus endpoint on port 9616')
    parser.add_argument('--type', choices=['archive', 'full', 'light'], default='archive',
                        help='Node type: archive, full, or light (default: %(default)s)')
    # parser.add_argument('--limit', type=int, help='Limit number of blocks to be processed per run')
    parser.add_argument('--cron_delay', type=int, help='Cron job delay (default: %(default)s)', default=60)
    parser.add_argument('-v', '--version', action='version', version='%(prog)s ' + __version__)
    parser.add_argument('action', choices=['blocks', 'state', 'decode', 'cron', 'etl', 'all'], default='all',
                        nargs='?', help='Type of job to run (default: %(default)s)')
    parser.add_argument('--force-start', action='store_true', help='Forces the harvester to start')

    args = parser.parse_args()

    if args.verbose:
        verbose_level = 3
        import logging
        logging.basicConfig(level=logging.DEBUG)
    else:
        verbose_level = 2

    node_type = app_settings.NODE_TYPE or args.type

    cli.harvester = Harvester(
        settings=app_settings,
        verbose_level=verbose_level,
        type=args.type,
        cron_delay=args.cron_delay,
        prometheus_endpoint=args.prometheus,
        force_start=args.force_start
    )

    cli.run()


