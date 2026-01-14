from datetime import datetime

#ZARR_DAY_0 = datetime(2025, 9, 1) #TODO remove? or have some constant about 
# time periods when echosounders were in different modes 

DATA_BUCKET = "s3://ooi-data"
TEST_BUCKET = "s3://temp-ooi-data-prod" # TODO

VIZ_BUCKET = "s3://ooi-rca-qaqc-prod"

ECHO_REFDES_LIST = [
     "CE02SHBP-MJ01C-07-ZPLSCB101",
     "CE04OSPS-PC01B-05-ZPLSCB102",
]

SUFFIX = "streamed-zplsc_volume_scattering"

OFFSHORE_CHUNKING = {
    "ping_time": 512,
    "range_sample": -1,
    "channel": -1,
    "filenames": -1,
}

# exclude simrad specific variables in echopype output
VARIABLES_TO_EXCLUDE = [
    "angle_offset_alongship",
    "angle_offset_athwartship",
    "angle_sensitivity_alongship",
    "angle_sensitivity_athwartship",
    "beamwidth_alongship",
    "beamwidth_athwartship",
]

HARVEST_DEPLOYMENT = "echo_raw_data_harvest_8vcpu_60gb"
