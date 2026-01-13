"""module for harvesting .raw echosounder data and writing to chunked zarr store"""
import sys
import fsspec
import click
import zarr 
import faulthandler
import warnings

import xarray as xr
import echopype as ep

from tqdm import tqdm
from datetime import datetime, timedelta
from rca_echo_tools.constants import (
    DATA_BUCKET, 
    TEST_BUCKET, 
    OFFSHORE_CHUNKING, 
    SUFFIX,
    VARIABLES_TO_EXCLUDE
)
from rca_echo_tools.utils import select_logger, get_s3_kwargs

warnings.filterwarnings("ignore", category=FutureWarning)

# we need to write to zarr at intervals instead of concatenating the whole thing TODO
# batch processing pattern TODO
@click.command()
@click.option("--start-date", required=True, type=str, help="Start date in YYYY/MM/DD format")
@click.option("--end-date", required=True, type=str, help="End date in YYYY/MM/DD format")
@click.option("--refdes", required=True, type=str, help="Reference designator of the echosounder")
@click.option(
    "--waveform-mode",
    required=True,
    type=click.Choice(["CW", "BB"], case_sensitive=False),
    help="Waveform mode: CW or BB"
)
@click.option(
    "--encode-mode",
    required=True,
    type=click.Choice(["power", "complex"], case_sensitive=False),
    help="Encode mode: power or complex"
)
@click.option("--sonar-model", required=True, type=str, help="Sonar model: EK80 or EK60")
@click.option("--data-bucket", required=False, type=str, default=TEST_BUCKET, help="S3 bucket to write zarr store to")
@click.option(
    "--run-type",
    required=False,
    type=click.Choice(["append", "refresh"], case_sensitive=False),
    help="Type of pipeline run. Refresh will overwrite existing zarr store with specified date range."
        "Append will append to existing zarrs store along `ping_time` dimension.",
    default="append"
)
def refresh_full_echo_ds(
    start_date: str,
    end_date: str,
    refdes: str,
    waveform_mode: str,
    encode_mode: str,
    sonar_model: str,
    data_bucket: str,
    run_type: str,
    batch_size_days: int = 2,
) -> None:

    logger = select_logger()
    fs_kwargs = get_s3_kwargs()
    fs = fsspec.filesystem("s3", **fs_kwargs)

    store_path = f"{data_bucket}/{refdes}-{SUFFIX}/"
    store = fs.get_mapper(store_path)
    store_exists = fs.exists(store_path)
    if run_type == "refresh" and store_exists:
        raise FileExistsError("`--refresh` specified, but zarr store already exists. Please either " \
        "delete existing store and run refesh again, or specify `--append` if you just wish to append " \
        "to existing store.")

    start_dt = datetime.strptime(start_date, "%Y/%m/%d")
    end_dt = datetime.strptime(end_date, "%Y/%m/%d")

    batch_start = start_dt

    while batch_start <= end_dt:
        batch_end = min(
            batch_start + timedelta(days=batch_size_days - 1),
            end_dt,
        )

        logger.info(
            f"Processing batch {batch_start:%Y-%m-%d} → {batch_end:%Y-%m-%d}"
        )

        # 1. Collect URLs for this batch only
        batch_urls = []

        dt = batch_start
        while dt <= batch_end:
            daily_urls = get_raw_urls(dt.strftime("%Y/%m/%d"), refdes)
            if daily_urls:
                batch_urls.extend(daily_urls)
            else:
                logger.warning(f"No data for {dt:%Y-%m-%d}")
            dt += timedelta(days=1)

        if not batch_urls:
            logger.warning("No data found for this batch, skipping...")
            batch_start = batch_end + timedelta(days=1)
            continue

        # 2. Parse + compute Sv for this batch
        Sv_list = []

        for url in tqdm(batch_urls, desc="Parsing + computing Sv", unit="file"):
            logger.info(f"Parsing raw data for {url}.")
            ed = ep.open_raw(url, sonar_model=sonar_model)
            logger.info(f"Computing Sv for {url}.")
            ds_Sv = ep.calibrate.compute_Sv(
                ed,
                waveform_mode=waveform_mode,
                encode_mode=encode_mode,
            )

            # TODO variable validation here
            ds_Sv = clean_Sv_ds(ds_Sv, logger)

            Sv_list.append(ds_Sv)

            del ed

        logger.info("<<< Concatenating Sv for this batch. >>>")
        combined_ds = xr.concat(Sv_list, dim="ping_time", join="outer")

        del Sv_list  # free up memory

        # 3. Write / append to Zarr
        write_mode = "w" if not store_exists else "a"

        logger.info("Writing batch to Zarr store.")
        combined_ds.to_zarr(
            store_path,
            mode=write_mode,
            append_dim="ping_time" if store_exists else None,
            storage_options=fs_kwargs,
        )

        store_exists = True 

        del combined_ds  # free up memory

        # 4. Move to next batch
        batch_start = batch_end + timedelta(days=1)

    # 5. Consolidate metadata ONCE
    logger.info("Consolidating Zarr metadata")
    zarr.consolidate_metadata(store)
    

def get_raw_urls(day_str: str, refdes: str):

    base_url = "https://rawdata.oceanobservatories.org/files"
    mainurl = f"{base_url}/{refdes[0:8]}/{refdes[9:14]}/{refdes[18:27]}/{day_str}/"
    FS = fsspec.filesystem("http")
    print(mainurl)
    try:
        data_url_list = sorted(
            f["name"]
            for f in FS.ls(mainurl)
            if f["type"] == "file" and f["name"].endswith(".raw")
        )

    except Exception as e:
        print("Client response: ", str(e))
        return None

    if not data_url_list:
        print("No Data Available for Specified Time")
        return None

    return data_url_list


def clean_Sv_ds(ds_Sv: xr.Dataset, logger):

    for var in ds_Sv.data_vars:
        if var in VARIABLES_TO_EXCLUDE:
            logger.info(f"Dropping variable: {var}")
            ds_Sv = ds_Sv.drop_vars(var)
    
    return ds_Sv


if __name__ == "__main__":
    refresh_full_echo_ds()
