import echopype as ep

from datetime import datetime, timedelta
from rca_echo_tools.constants import SUFFIX
from rca_echo_tools.utils import load_data


def plot_daily_echogram(
    date: datetime,
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
    unbinned_ds = load_data(f"{refdes}-{SUFFIX}")
    unbinned_ds_day = unbinned_ds.sel(ping_time=slice(date, date + timedelta(days=1)))

    # Reduce data based on sample number
    ds_MVBS = ep.commongrid.compute_MVBS(
        unbinned_ds_day, # calibrated Sv dataset
        #range_bin_num=30,  # number of sample bins to average along the range_bin dimensionm
        ping_time_bin=ping_time_bin,
        range_bin=range_bin,
    )
