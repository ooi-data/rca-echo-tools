import roseus.mpl as rs
import echopype as ep
import matplotlib.pyplot as plt

from datetime import datetime, timedelta
from pathlib import Path
from prefect import flow

from rca_echo_tools.constants import SUFFIX, VIZ_BUCKET
from rca_echo_tools.utils import load_data, restore_logging_for_prefect, get_s3_kwargs
from rca_echo_tools.cloud import sync_png_to_s3

plt.switch_backend('Agg') # use non-interactive backend for plotting

@flow(log_prints=True)
def plot_daily_echogram(
    date: str,
    refdes: str,
    ping_time_bin: str="4s",
    range_bin: str="0.1m",
    s3_sync: bool = False,
    ):
    """
    Wraps echopype commongrid. From echopype docs:
    commongrid: Enhance the spatial and temporal coherence of data. Currently contains functions 
    to compute mean volume backscattering strength (MVBS) that result in gridded data at uniform 
    spatial and temporal intervals based on either number of indices or label values (phyiscal units).
    """
    restore_logging_for_prefect()
    s3_kwargs = get_s3_kwargs() 
    print(f"---- Launching: daily echogram for {refdes} on {date} with"
          f" ping_time_bin={ping_time_bin} and range_bin={range_bin} ----")

    dt = datetime.strptime(date, "%Y/%m/%d") # python datetime format
    date_tag = date.replace("/", "") # for file naming no '/'

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

    # Map full channel strings to clean frequency labels
    channels = ds_MVBS["channel"].values
    channel_labels = {ch: ch for ch in channels}  # fallback
    freq_map = {"38": "38 kHz", "120": "120 kHz", "200": "200 kHz"}
    for ch in channels:
        for freq, label in freq_map.items():
            if freq in ch:
                channel_labels[ch] = label

    print("Plotting downsampled array.")
    facet_grid = ds_MVBS["Sv"].plot(
        x="ping_time",
        row="channel",
        figsize=(18, 9),
        vmin=-100,
        vmax=-30,
        cmap=rs.lavendula
    )

    for ax, channel in zip(facet_grid.axes.flat, channels):
        ax.set_title(channel_labels[channel])
        ax.set_xlabel("") # remove ping time label
        ax.set_ylabel("Vertical Range (m)")

    # Fix colorbar label
    facet_grid.cbar.set_label("Sv (dB re 1 m$^{-1}$)")

    fig = facet_grid.fig
    fig.text(
        0.99, 0.01,
        f"ping_time_bin={ping_time_bin}\nrange_bin={range_bin}",
        ha='right', va='bottom',
        fontsize=8, color='black',
        transform=fig.transFigure
    )


    plt.savefig(f"{str(output_dir)}/{instrument}_{date_tag}.png")

    if s3_sync:
        print(f"Syncing echograms to {VIZ_BUCKET}")
        sync_png_to_s3(instrument, date, s3_kwargs, output_dir)
