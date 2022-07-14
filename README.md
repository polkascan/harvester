# Polkascan Harvester

## Running using Docker

Replace ENV settings in `docker-compose.yml` then run

```bash
docker-compose up --build
```

## Running locally

Install package requirements:

```bash
pip install -r requirements.txt
```

Add current path to Python path

```bash
 export PYTHONPATH=$PYTHONPATH:$(pwd)
```

Create ./app/local_settings.py

```python
DB_CONNECTION = "mysql+pymysql://root:root@localhost:3306/polkascan?charset=utf8mb4"
SUBSTRATE_RPC_URL = "ws://127.0.0.1:9944/"

INSTALLED_ETL_DATABASES = []
```

Apply database migrations

```bash
alembic upgrade head
```

Run harvester
```bash
python app/harvester.py --force-start
```

