import click

from prefect.deployments import run_deployment
from datetime import datetime, timedelta, timezone

from rca_echo_tools.harvest import echo_raw_data_harvest
from rca_echo_tools.constants import DATA_BUCKET, DEFAULT_DEPLOYMENT
from rca_echo_tools.utils import select_logger
from rca_echo_tools.echogram import plot_daily_echogram

# get yesterday's date in YYYY/MM/DD format
now_utc = datetime.now(timezone.utc)
yesterday_utc = now_utc - timedelta(days=1)
yesterday = yesterday_utc.strftime("%Y/%m/%d")

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
@click.option("--data-bucket", required=False, type=str, default=DATA_BUCKET, help="S3 bucket to write zarr store to")
@click.option(
    "--run-type",
    required=False,
    type=click.Choice(["append", "refresh"], case_sensitive=False),
    help="Type of pipeline run. Refresh will overwrite existing zarr store with specified date range."
        "Append will append to existing zarrs store along `ping_time` dimension.",
    default="append"
)
@click.option("--cloud", type=bool, default=False, show_default=True, help="Flag to indicate if harvest should run on RCA cloud.")
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
        print(f"Launching pipeline in cloud for {run_name}")
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
            name=f"echo-raw-data-harvest/{DEFAULT_DEPLOYMENT}",
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


@click.command()
@click.option("--refdes", required=True, type=str, help="Reference designator of the echosounder")
@click.option("--start-date", required=True, type=str, default=yesterday, help="Date in the format YYYY/MM/DD (e.g., '2025/01/16'). Default is yesterday's date (UTC).")
@click.option("--end-date", type=str, default=None, help="YYYY/MM/DD leave blank to run a single day")
@click.option("--ping-time-bin", required=False, type=str, default="4s", help="Time bin size for ping_time dimension default is 4s")
@click.option("--range-bin", required=False, type=str, default="0.1m", help="Range bin size for range dimension, default is 0.1m")
@click.option("--parallel-in-cloud", type=bool, default=False, show_default=True, help="run prefect deployment in parellel in cloud, parallelized by date")
def run_daily_echograms(
    refdes: str,
    start_date: str,
    end_date: str,
    ping_time_bin: str,
    range_bin: str,
    parallel_in_cloud: bool,
):
    start_dt = datetime.strptime(start_date, "%Y/%m/%d")
    end_dt = datetime.strptime(end_date, "%Y/%m/%d") if end_date else start_dt
    dt_list = [start_dt + timedelta(days=i) for i in range((end_dt - start_dt).days + 1)]

    # Build params for each date
    all_params = [
        {"date": d.strftime("%Y/%m/%d"), "refdes": refdes, "ping_time_bin": ping_time_bin,"range_bin": range_bin}
        for d in dt_list
    ]

    # Dispatch — same loop, different runner
    runner = _run_cloud if parallel_in_cloud else _run_local
    for params in all_params:
        runner(params)


def _run_cloud(params):
    date_str = params['date']
    run_name = f"{params['refdes']}_{date_str.replace('/', '')}_echogram"

    print(f"Launching workflow for {run_name} in cloud")
    run_deployment(
        name=run_name,
        parameters=params,
        flow_run_name=run_name,
        timeout=3, # seconds to not hammer the zarr too hard
    )


def _run_local(params):
    plot_daily_echogram(**params)


if __name__ == "__main__":
    #run_echo_raw_data_harvest()
    run_daily_echograms()
