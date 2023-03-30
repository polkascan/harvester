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
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import click
from app import settings as app_settings
from app.harvester import Harvester
from app import __version__


@click.group()
@click.version_option(version=__version__)
def main():
    pass


@main.command()
@click.option("--verbose", is_flag=True, show_default=True, default=False, help="Verbose more")
@click.option("--prometheus", is_flag=True, show_default=True, default=True, help="Expose Promotheus endpoint on port 9616")
@click.option("--force-start", is_flag=True, show_default=True, default=False, help="Forces the harvester to start")
@click.option('--type', 'type_', type=click.Choice(['archive', 'full', 'light'], case_sensitive=False), default='archive', show_default=True)
@click.option('--job', type=click.Choice(['blocks', 'state', 'decode', 'cron', 'etl', 'event_index', 'all'], case_sensitive=False), default='all', show_default=True)
@click.option('--block-start', type=int)
@click.option('--block-end', type=int)
def run(verbose, prometheus, type_, force_start, job, block_start, block_end):
    if verbose:
        verbose_level = 3
        import logging
        logging.basicConfig(level=logging.DEBUG)
    else:
        verbose_level = 2

    harvester.verbose_level = verbose_level
    harvester.type = type_
    harvester.prometheus_endpoint = prometheus
    harvester.force_start = force_start

    if block_start:
        harvester.block_start = block_start

    if block_end:
        harvester.block_end = block_end

    harvester.run(job)


@main.group()
def storage_tasks():
    pass


@storage_tasks.command('list', help='List storage tasks and monitor its progress')
def list_storage_tasks():
    harvester.list_storage_tasks()


@storage_tasks.command('clean', help='Clean up completed storage tasks')
def clean_storage_tasks():
    harvester.clean_storage_tasks()


@storage_tasks.command('add', help='Add a storage task')
def add_storage_tasks():

    def format_block_range(value: str) -> dict:
        if value.isnumeric():
            return {'block_ids': [int(value)]}
        if ',' in value:
            return {'block_ids': [int(e) for e in value.split(',')]}
        if '-' in value:
            value = value.split('-')
            return {'block_start': int(value[0]), 'block_end': int(value[1])}

    pallet = click.prompt("Pallet", type=str)
    storage_function = click.prompt("Storage function", type=str)
    blocks = click.prompt("Blocks (e.g. '100,104' or '100-200')")

    block_range = format_block_range(blocks)

    harvester.add_storage_task(
        pallet, storage_function, block_range, description=f'{pallet}.{storage_function} for blocks {blocks}'
    )

    click.echo(f'Added task {pallet}.{storage_function} for blocks {blocks}', color=True)


@storage_tasks.command('rm', help='Removes a storage task by its ID')
@click.argument('id', type=int)
def remove_storage_task(id):
    harvester.remove_storage_task(id)


@main.group()
def storage_cron():
    pass


@storage_cron.command('list', help='Lists storage cron records')
def list_storage_cron():
    harvester.list_storage_cron()


@storage_cron.command('add', help='Adds a storage cron record')
def add_storage_cron():
    block_interval = click.prompt("Block interval (e.g. 10 = every 10th block)", type=int)
    pallet = click.prompt("Pallet", type=str)
    storage_function = click.prompt("Storage function", type=str)

    harvester.add_storage_cron(block_interval, pallet, storage_function)
    click.echo(f'Added cron {pallet}.{storage_function} every {block_interval} blocks', color=True)


@storage_cron.command('rm', help='Removes a storage cron by its ID')
@click.argument('id', type=int)
def remove_storage_cron(id):
    harvester.remove_storage_cron(id)


if __name__ == '__main__':
    harvester = Harvester(
        settings=app_settings,
        force_start=True
    )
    main()
