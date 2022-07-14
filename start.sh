#! /usr/bin/env sh
export PYTHONPATH=$PYTHONPATH:./py-substrate-interface/:./py-scale-codec/

python app/prestart.py
alembic upgrade head
python app/harvester.py --force-start
