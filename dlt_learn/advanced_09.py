"""
dlt pipeline optimisation exercise for dlt Advanced course from dltHub Education.

This pipeline fetches data from the Jaffle Shop API using RESTClient with page-number pagination
and loads it into a DuckDB destination.

The goal is to make the pipeline as fast as possible, using the below techniques, while keeping
the results correct.
 - Chunking
 - Parallelism
 - Buffer control
 - File rotation
 - Worker tuning

For reference, this pipeline is being executed natively on an M2 MacBook Air with 16GB RAM
using dlt v1.20.0 on Python 3.13.2
"""

import logging
import os
import sys

import dlt
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dlt.sources.rest_api import RESTClient
from loguru import logger as loguru_logger


class InterceptHandler(logging.Handler):
    """Handler that intercepts standard logging and routes to loguru."""

    @loguru_logger.catch(default=True, onerror=lambda _: sys.exit(1))
    def emit(self, record: logging.LogRecord) -> None:
        """Emit a log record to loguru."""
        try:
            level = loguru_logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        loguru_logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


# Configure loguru
loguru_logger.remove()  # Remove default handler
loguru_logger.add(
    sys.stdout,
    format="<level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    level=os.getenv("LOG_LEVEL", "INFO"),
)
# Intercept standard logging used by dlt
logger_dlt = logging.getLogger("dlt").addHandler(InterceptHandler())

os.environ["DATA_WRITER__BUFFER_MAX_ITEMS"] = "50000"
os.environ["DATA_WRITER__FILE_MAX_BYTES"] = "5000000"
os.environ["NORMALIZE__WORKERS"] = "6"
os.environ["LOAD__WORKERS"] = "6"


@dlt.source
def jaffle_api():
    """Creates a JaffleShop API source using RESTClient with page-number pagination."""
    jaffle_client = RESTClient(
        base_url="https://jaffle-shop.scalevector.ai/api/v1/",
        paginator=PageNumberPaginator(
            base_page=1,
            page_param="page",
            total_path=None,
            stop_after_empty_page=True,
        ),
    )

    return (
        get_customers(jaffle_client),
        get_orders(jaffle_client),
        get_products(jaffle_client),
    )


@dlt.resource(name="customers", parallelized=True)
def get_customers(client: RESTClient):  # 935 rows
    """Fetch customers from Jaffle Shop API."""
    yield from client.paginate(
        "customers",
        params={"page_size": 1_000},  # chunk size
    )


@dlt.resource(name="orders", parallelized=True)
def get_orders(client: RESTClient):  # 61948 rows
    """Fetch orders from Jaffle Shop API."""
    yield from client.paginate(
        "orders",
        params={"page_size": 5_000},  # chunk size
    )


@dlt.resource(name="products", parallelized=True)
def get_products(client: RESTClient):  # 10 rows
    """Fetch products from Jaffle Shop API."""
    yield from client.paginate(
        "products",
    )


def jaffle_pipeline():
    """Load Jaffle Shop API data into DuckDB."""
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_pipeline",
        destination="duckdb",
        dataset_name="jaffle_shop",
        progress="log",
    )

    load_info = pipeline.run(jaffle_api())
    return load_info


if __name__ == "__main__":
    try:
        loguru_logger.info("Starting Jaffle Shop API pipeline")
        load_info = jaffle_pipeline()

        loguru_logger.info("Pipeline completed successfully")
        loguru_logger.info(f"Loaded packages: {load_info.loads_ids}")
        loguru_logger.debug(f"Load info: {load_info}")

    except Exception as e:
        loguru_logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
