import echopype as ep
import matplotlib.pyplot as plt

from datetime import datetime, timedelta
from pathlib import Path
from prefect import flow

from rca_echo_tools.constants import SUFFIX
from rca_echo_tools.utils import load_data, restore_logging_for_prefect

plt.switch_backend('Agg') # use non-interactive backend for plotting

@flow(log_prints=True)
def plot_daily_echogram(
    date: str,
    refdes: str,
    ping_time_bin: str="4s",
    range_bin: str="0.1m",
    ):
    """
    Wraps echopype commongrid. From echopype docs:
    commongrid: Enhance the spatial and temporal coherence of data. Currently contains functions 
    to compute mean volume backscattering strength (MVBS) that result in gridded data at uniform 
    spatial and temporal intervals based on either number of indices or label values (phyiscal units).
    """
    restore_logging_for_prefect()
    print(f"---- Launching: daily echogram for {refdes} on {date} with"
          f" ping_time_bin={ping_time_bin} and range_bin={range_bin} ----")

    dt = datetime.strptime(date, "%Y/%m/%d")
    output_dir = Path("./output")
    output_dir.mkdir(parents=True, exist_ok=True)

    instrument = refdes[-9:]

    unbinned_ds = load_data(f"{refdes}-{SUFFIX}")
    unbinned_ds_day = unbinned_ds.sel(ping_time=slice(dt, dt + timedelta(days=1)))

    print("Downsampling data with ep commongrid to deal with offset ping nans.")
    # Reduce data based on sample number
    ds_MVBS = ep.commongrid.compute_MVBS(
        unbinned_ds_day, # calibrated Sv dataset
        #range_bin_num=30,  # number of sample bins to average along the range_bin dimensionm
        ping_time_bin=ping_time_bin,
        range_bin=range_bin,
    )

    print("Plotting downsampled array.")
    facet_grid = ds_MVBS["Sv"].plot(
    x="ping_time",
    row="channel",
    figsize=(14, 7),
    vmin=-100,
    vmax=-30,
    cmap="jet"
    )

    channels = ds_MVBS["channel"].values
    for ax, channel in zip(facet_grid.axes.flat, channels):
        ax.set_title(channel)

    plt.savefig(f"{str(output_dir)}/{instrument}_{date}.png")
