"""module for harvesting .raw echosounder data and writing to chunked zarr store"""
import fsspec
import click
import zarr 
import warnings

import xarray as xr
import echopype as ep

from tqdm import tqdm
from datetime import datetime, timedelta
from rca_echo_tools.constants import (
    DATA_BUCKET, 
    OFFSHORE_CHUNKING, 
    SUFFIX,
    VARIABLES_TO_EXCLUDE
)
from rca_echo_tools.utils import select_logger, get_s3_kwargs

warnings.filterwarnings("ignore", category=FutureWarning)

# we need to write to zarr at intervals instead of concatenating the whole thing TODO
# batch processing pattern TODO
def echo_raw_data_harvest(
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

        # TODO check what auto chunking is doing?
        logger.info("------ Writing batch to Zarr store. ------")
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

    var_dropped_list = []
    for var in ds_Sv.data_vars:
        if var in VARIABLES_TO_EXCLUDE:
            var_dropped_list.append(var)
            ds_Sv = ds_Sv.drop_vars(var)
    
    logger.info(f"Dropped variables from Sv dataset: {var_dropped_list}")
    
    return ds_Sv


if __name__ == "__main__":
    echo_raw_data_harvest()
