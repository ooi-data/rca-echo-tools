import click

from fastapi import params
from prefect.deployments import run_deployment

from rca_echo_tools.harvest import echo_raw_data_harvest
from rca_echo_tools.utils import select_logger
from rca_echo_tools.constants import HARVEST_DEPLOYMENT, TEST_BUCKET

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
@click.option("--cloud", required=False, is_flag=True, default=False, help="Flag to indicate if harvest should run on RCA cloud.")
def run_echo_raw_data_harvest(
    start_date: str,
    end_date: str,
    refdes: str,
    waveform_mode: str,
    encode_mode: str,
    sonar_model: str,
    data_bucket: str,
    run_type: str,
    batch_size_days: int = 2,
    cloud: bool = False,
) -> None:
    
    logger = select_logger()
    run_name = f"{refdes}_{start_date.replace('/', '')}_{end_date.replace('/', '')}"

    if cloud:
        logger.info(f"Launching pipeline in cloud for {run_name}")
        params = {
        "start_date": start_date,
        "end_date": end_date,
        "refdes": refdes,
        "waveform_mode": waveform_mode,
        "encode_mode": encode_mode,
        "sonar_model": sonar_model,
        "data_bucket": data_bucket,
        "run_type": run_type,
        "batch_size_days": batch_size_days,
        }   

        run_deployment(
            name=f"echo-raw-data-harvest/echo_tools_16vcpu_80gb",
            parameters=params,
            flow_run_name=run_name,
            timeout=12,
        )
    
    else:
        logger.info(f"Launching pipeline locally for {run_name}")
        echo_raw_data_harvest(
            start_date=start_date,
            end_date=end_date,
            refdes=refdes,
            waveform_mode=waveform_mode,
            encode_mode=encode_mode,
            sonar_model=sonar_model,
            data_bucket=data_bucket,
            run_type=run_type,
            batch_size_days=batch_size_days,
        )