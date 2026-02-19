import fsspec

from pathlib import Path
from prefect import task

from datetime import datetime
from rca_echo_tools.constants import VIZ_BUCKET

@task
def sync_png_to_s3(instrument: str, date: str, fs_kwargs: dict, local_dir: Path):
    """sync .nc and .png files to S3 based on the given date and refdes."""
    year = datetime.strptime(date, "%Y/%m/%d").year
    s3_fs = fsspec.filesystem("s3", **fs_kwargs)

    def is_valid_file(fp: Path):
        filename = fp.name

        return instrument in filename and str(year) in filename

    # Upload .png files to spectrograms/YYYY/
    png_files = local_dir.glob("*ZPLS*.png")
    for fp in png_files:
        if fp.is_file():
            s3_uri = f"{VIZ_BUCKET}/echograms/{year}/{instrument}/{fp.name}"
            print(f"Uploading {fp} to {s3_uri}")
            s3_fs.put(str(fp), s3_uri)