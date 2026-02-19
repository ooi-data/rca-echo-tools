import os
import s3fs
import logging 
import sys

import xarray as xr 

from prefect.exceptions import MissingContextError
from rca_echo_tools.constants import DATA_BUCKET


def select_logger():
    from prefect import get_run_logger
    try:
        logger = get_run_logger()
        print("using prefect logger")
    except MissingContextError as e:
        from loguru import logger
        print(f"using loguru logger: {e}")
    
    return logger


def get_s3_kwargs():
    aws_key = os.environ.get("AWS_KEY")
    aws_secret = os.environ.get("AWS_SECRET")

    if aws_key is None or aws_secret is None:
        raise EnvironmentError("AWS_KEY and AWS_SECRET must be set in environment variables.")

    s3_kwargs = {"key": aws_key, "secret": aws_secret}
    return s3_kwargs


def load_data(stream_name: str):
    fs = s3fs.S3FileSystem()
    zarr_dir = f"{DATA_BUCKET}/{stream_name}"
    print(f"loading zarr metadata from {zarr_dir}")
    zarr_store = fs.get_mapper(zarr_dir)
    ds = xr.open_zarr(zarr_store, consolidated=False)
    return ds


def restore_logging_for_prefect():
    """echopype alters loggin configs in a way that breaks prefect logging. 
    This function should restore it in most cases."""
    root = logging.getLogger()

    # Remove all handlers echopype installed
    for h in list(root.handlers):
        root.removeHandler(h)

    logging.disable(logging.NOTSET)  # undo echopype's global disable
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)