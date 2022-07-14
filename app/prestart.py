import logging

from tenacity import after_log, before_log, retry, stop_after_attempt, wait_fixed

from app import settings
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

engine = create_engine(settings.DB_CONNECTION, echo=False, pool_pre_ping=True)
session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)

session = scoped_session(session_factory)

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

max_tries = 60 * 5  # 5 minutes
wait_seconds = 1


@retry(
    stop=stop_after_attempt(max_tries),
    wait=wait_fixed(wait_seconds),
    before=before_log(logger, logging.INFO),
    after=after_log(logger, logging.WARN),
)
def init() -> None:
    try:
        # Try to create session to check if DB is awake
        session.execute(f"SELECT 1")

        for etl_db in settings.INSTALLED_ETL_DATABASES:
            session.execute(f"SELECT 1 FROM {etl_db}.alembic_version")

    except Exception as e:
        logger.error(e)
        raise e


def main() -> None:
    logger.info("Initializing service")
    init()
    logger.info("Service finished initializing")


if __name__ == "__main__":
    main()
