"""module for harvesting .raw echosounder data and writing to chunked zarr store"""
import fsspec
import click
import zarr 

import xarray as xr
import echopype as ep

from tqdm import tqdm
from datetime import datetime, timedelta
from rca_echo_tools.constants import DATA_BUCKET, TEST_BUCKET, OFFSHORE_CHUNKING, SUFFIX
from rca_echo_tools.utils import select_logger, get_s3_kwargs


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
def refresh_full_echo_ds(
    start_date: str, 
    end_date: str, 
    refdes: str,
    waveform_mode: str, #CW or BB
    encode_mode: str, #power or complex
    sonar_model: str, #EK80 or EK60
    data_bucket: str
    ) -> None:

    logger = select_logger()
    fs_kwargs = get_s3_kwargs()
    start_dt = datetime.strptime(start_date, "%Y/%m/%d")
    end_dt = datetime.strptime(end_date, "%Y/%m/%d")

    full_url_list = []
    while start_dt <= end_dt:
        daily_url_list = get_raw_urls(start_dt.strftime("%Y/%m/%d"), refdes)

        if daily_url_list is not None:
            logger.info(f"Found {len(daily_url_list)} files for {start_dt.strftime('%Y/%m/%d')}")
            full_url_list.extend(daily_url_list)

        elif daily_url_list is None:
            logger.warning(f"No data for {start_dt.strftime('%Y/%m/%d')}")
        
        start_dt += timedelta(days=1)

    ed_list = [] # echopype data objects
    for url in tqdm(full_url_list, desc="parsing raw data", unit="file"):
        logger.info(f"Parsing raw data for {url}.")
        ed = ep.open_raw(url, sonar_model=sonar_model)
        ed_list.append(ed)

    Sv_list = [] # scatter volume 
    for ed in tqdm(ed_list, desc="generating sv", unit="data array"):
        ds_Sv = ep.calibrate.compute_Sv(ed, waveform_mode=waveform_mode, encode_mode=encode_mode)
        Sv_list.append(ds_Sv)

    logger.info("Concatenating all Sv arrays into single dataset.")
    combined_ds = xr.concat(Sv_list, dim="ping_time")

    logger.info("Combined dataset ping time dim:")
    logger.info(combined_ds['ping_time'])

    logger.info("writing complete dataset to zarr store")
    store_path = f"{data_bucket}/{refdes}-{SUFFIX}/"
    combined_ds.chunk(OFFSHORE_CHUNKING)
    combined_ds.to_zarr(
        store_path, 
        mode="w", 
        storage_options=fs_kwargs)
    
    logger.info("Consolidating zarr metadata.")
    zarr.consolidate_metadata(store_path, storage_options=fs_kwargs)
    

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

if __name__ == "__main__":
    refresh_full_echo_ds()