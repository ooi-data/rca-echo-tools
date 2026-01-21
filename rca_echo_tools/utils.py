import os
from prefect.exceptions import MissingContextError
# TODO unify utils for all RCA repos

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

    s3_kwargs = {"key": aws_key, "secret": aws_secret}
    return s3_kwargs