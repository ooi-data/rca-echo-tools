"""module for harvesting .raw echosounder data and writing to chunked zarr store"""
import json
import fsspec
import logging 
import sys
import xarray as xr
import echopype as ep

from prefect import flow, task
from datetime import datetime, timedelta
from rca_echo_tools.constants import (
    SUFFIX,
    VARIABLES_TO_EXCLUDE,
    METADATA_JSON_BUCKET,
)
from rca_echo_tools.utils import get_s3_kwargs 


# we need to write to zarr at intervals instead of concatenating the whole thing TODO
# batch processing pattern TODO
@flow(log_prints=True)
def echo_raw_data_harvest(
    start_date: str,
    end_date: str,
    refdes: str,
    waveform_mode: str,
    encode_mode: str,
    sonar_model: str,
    data_bucket: str,
    run_type: str,
    batch_size_days: int = 2
):
    restore_logging_for_prefect()

    fs_kwargs = get_s3_kwargs()
    fs = fsspec.filesystem("s3", **fs_kwargs)

    store_path = f"{data_bucket}/{refdes}-{SUFFIX}/"
    metadata_json_path = f"{METADATA_JSON_BUCKET}/harvest-status/{refdes}-{SUFFIX}/"

    if run_type not in ["refresh"]:
        with fs.open(metadata_json_path, "r") as f:
            metadata_dict = json.load(f)

    if run_type in ["prepend"]:
        if datetime.strptime(end_date, "%Y/%m/%d") >= datetime.strptime(metadata_dict["start_date"], "%Y/%m/%d"):
            raise ValueError("`--prepend` specified, but end_date is after or equal to existing start_date in metadata. " \
            "Please adjust date range or use `--append` or `--refresh` instead.")
    if run_type in ["append"]:
        if datetime.strptime(start_date, "%Y/%m/%d") <= datetime.strptime(metadata_dict["end_date"], "%Y/%m/%d"):
            raise ValueError("`--append` specified, but start_date is before or equal to existing end_date in metadata. " \
            "Please adjust date range or use `--prepend` or `--refresh` instead.")

    # store = fs.get_mapper(store_path) #TODO uneeded without metadat?
    store_exists = fs.exists(store_path)
    if run_type == "refresh" and store_exists:
        raise FileExistsError("`--refresh` specified, but zarr store already exists. Please either " \
        "delete existing store and run refesh again, or specify `--prepend/--append` if you just wish to modify " \
        "existing store.")

    start_dt = datetime.strptime(start_date, "%Y/%m/%d")
    end_dt = datetime.strptime(end_date, "%Y/%m/%d")

    batch_start = start_dt

    while batch_start <= end_dt:
        batch_end = min(
            batch_start + timedelta(days=batch_size_days - 1),
            end_dt,
        )

        print(
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
                print(f"No data for {dt:%Y-%m-%d}")
            dt += timedelta(days=1)

        if not batch_urls:
            print("No data found for this batch, skipping...")
            batch_start = batch_end + timedelta(days=1)
            continue

        # 2. Parse + compute Sv for this batch
        Sv_list = []

        for url in batch_urls:
            print(f"Parsing raw data for {url}.")
            ed = ep.open_raw(url, sonar_model=sonar_model)
            print(f"Computing Sv for {url}.")
            ds_Sv = ep.calibrate.compute_Sv(
                ed,
                waveform_mode=waveform_mode,
                encode_mode=encode_mode,
            )

            # TODO variable validation here
            ds_Sv = clean_Sv_ds(ds_Sv)

            Sv_list.append(ds_Sv)

            del ed

        print("<<< Concatenating Sv for this batch. >>>")
        combined_ds = xr.concat(Sv_list, dim="ping_time", join="outer")

        del Sv_list  # free up memory

        # 3. Write / append to Zarr
        write_mode = "w" if not store_exists else "a"

        # TODO check what auto chunking is doing?
        print("------ Writing batch to Zarr store. ------")
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
    
    print("Updating metadata JSON.")
    if run_type in ["prepend"]:
        start_dt = start_date
        end_dt = metadata_dict["end_date"]
    elif run_type in ["append"]:
        start_dt = metadata_dict["start_date"]
        end_dt = end_date
    elif run_type in ["refresh"]:
        start_dt = start_date
        end_dt = end_date
    update_metadata_json(
        start_dt=start_dt, 
        end_dt=end_dt, 
        fs=fs, 
        metadata_path=metadata_json_path
    )

    # 5. Consolidate metadata ONCE 
    #print("Consolidating Zarr metadata") #TODO zarr 3 doesn't use consolidate metadata
    #zarr.consolidate_metadata(store) # TODO


@task
def update_metadata_json(
    start_dt: str, 
    end_dt: str, 
    fs: fsspec.filesystem, 
    metadata_path: str
):
    metadata_dict = {
        "start_date": start_dt,
        "end_date": end_dt,
    }

    with fs.open(metadata_path, "w") as f:
        json.dump(metadata_dict, f)

    
    
@task
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

@task
def clean_Sv_ds(ds_Sv: xr.Dataset):

    var_dropped_list = []
    for var in ds_Sv.data_vars:
        if var in VARIABLES_TO_EXCLUDE:
            var_dropped_list.append(var)
            ds_Sv = ds_Sv.drop_vars(var)
    
    print(f"Dropped variables from Sv dataset: {var_dropped_list}")
    
    return ds_Sv

@task
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

