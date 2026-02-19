"""module for harvesting .raw echosounder data and writing to chunked zarr store"""

import json
import fsspec

import xarray as xr
import echopype as ep

from prefect import flow, task
from datetime import datetime, timedelta
from rca_echo_tools.constants import (
    SUFFIX,
    VARIABLES_TO_EXCLUDE,
    VARIABLES_TO_INCLUDE,
    METADATA_JSON_BUCKET,
)
from rca_echo_tools.utils import get_s3_kwargs, restore_logging_for_prefect


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
    batch_size_days: int = 1,
):
    restore_logging_for_prefect()
    # installed_packages = {dist.metadata["Name"]: dist.version for dist in distributions()}
    # print(f"Installed packages: {installed_packages}")

    fs_kwargs = get_s3_kwargs()
    fs = fsspec.filesystem("s3", **fs_kwargs)

    store_path = f"{data_bucket}/{refdes}-{SUFFIX}/"
    metadata_json_path = f"{METADATA_JSON_BUCKET}/harvest-status/{refdes}-{SUFFIX}/"

    days_strings = get_day_strings(start_date, end_date)

    if run_type in ["append"]:
        with fs.open(metadata_json_path, "r") as f:
            metadata_dict = json.load(f)
        overlap_days = []
        for day in days_strings:
            if day in metadata_dict.keys():
                overlap_days.append(day)
        if len(overlap_days) > 0:
            raise ValueError(
                f"Date {overlap_days} already exists in metadata JSON. Please either "
                "remove this date from the JSON if you wish to reprocess it, or "
                "specify `--refresh` if you wish to overwrite existing data for "
                "the entire date range."
            )

    store_exists = fs.exists(store_path)
    if run_type == "refresh" and store_exists:
        raise FileExistsError(
            "`--refresh` specified, but zarr store already exists. Please either "
            "delete existing store and run refesh again, or specify `--append` if you just wish to modify "
            "existing store."
        )
    if run_type == "refresh":
        print("WIPING EXISTING METADATA JSON")
        fs.rm(metadata_json_path, recursive=True)

    start_dt = datetime.strptime(start_date, "%Y/%m/%d")
    end_dt = datetime.strptime(end_date, "%Y/%m/%d")

    batch_start = start_dt

    while batch_start <= end_dt:
        batch_end = min(
            batch_start + timedelta(days=batch_size_days - 1),
            end_dt,
        )

        # convert to str for metadata tracking purposes, update metadata after each day
        batch_start_str = batch_start.strftime("%Y/%m/%d")
        batch_end_str = batch_end.strftime("%Y/%m/%d")
        batch_days_strings = get_day_strings(batch_start_str, batch_end_str)

        print(f"Processing day {batch_start_str}")
        # 1. Collect URLs for this batch (day) only
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
        for url in batch_urls:
            print(f"Parsing raw data for {url}.")
            ed = ep.open_raw(url, sonar_model=sonar_model)
            print(f"Computing Sv for {url}.")
            ds_Sv = ep.calibrate.compute_Sv(
                ed,
                waveform_mode=waveform_mode,
                encode_mode=encode_mode,
            )

            # variable validation here in future if needed
            ds_Sv = clean_and_validate_Sv_ds(ds_Sv)
            del ed

            # 3. Write / append to Zarr
            write_mode = "w" if not store_exists else "a"

            print("------ Writing backscatter variables to Zarr store. ------")
            ds_Sv.to_zarr(
                store_path,
                mode=write_mode,
                append_dim="ping_time" if store_exists else None,
                storage_options=fs_kwargs,
            )

            store_exists = True
            del ds_Sv  # free up memory

        # 4. Move to next batch
        print("------ Updating metadata JSON. ------")
        update_metadata_json(
            metadata_day_keys=batch_days_strings,
            waveform_mode=waveform_mode,
            encode_mode=encode_mode,
            sonar_model=sonar_model,
            fs=fs,
            metadata_path=metadata_json_path,
        )
        batch_start = batch_end + timedelta(days=1)
        # NOTE no metadata consolidation in zarr v3


@task
def update_metadata_json(
    metadata_day_keys: list[str],
    waveform_mode: str,
    encode_mode: str,
    sonar_model: str,
    fs: fsspec.filesystem,
    metadata_path: str,
):
    # Build new entries for this run
    new_entries = {
        day: {
            "waveform_mode": waveform_mode,
            "encode_mode": encode_mode,
            "sonar_model": sonar_model,
        }
        for day in metadata_day_keys
    }

    # Load existing metadata
    if fs.exists(metadata_path):
        with fs.open(metadata_path, "r") as f:
            existing_metadata = json.load(f)
    else:
        existing_metadata = {}

    final_metadata = {**existing_metadata, **new_entries}

    # Write final metadata in one shot
    with fs.open(metadata_path, "w") as f:
        json.dump(final_metadata, f, indent=2)


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


@task  # missing variable padding could occur here as well if needed
def clean_and_validate_Sv_ds(ds_Sv: xr.Dataset):

    var_dropped_list = []
    for var in ds_Sv.data_vars:
        if var in VARIABLES_TO_EXCLUDE:
            var_dropped_list.append(var)
    ds_Sv = ds_Sv.drop_vars(var_dropped_list)

    print(f"Dropped variables from Sv dataset: {var_dropped_list}")

    for var in VARIABLES_TO_INCLUDE:
        if var not in ds_Sv.data_vars:
            raise ValueError(f"Expected variable {var} not found in Sv dataset.")

    return ds_Sv


def get_day_strings(start_date: str, end_date: str) -> list[str]:
    start_dt = datetime.strptime(start_date, "%Y/%m/%d")
    end_dt = datetime.strptime(end_date, "%Y/%m/%d")

    days = []
    current = start_dt
    while current <= end_dt:
        days.append(current.strftime("%Y/%m/%d"))
        current += timedelta(days=1)

    return days
