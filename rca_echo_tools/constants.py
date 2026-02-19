DATA_BUCKET = "s3://ooi-data"
VIZ_BUCKET = "s3://ooi-rca-qaqc-prod"
METADATA_JSON_BUCKET = "s3://flow-process-bucket"

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

# exclude simrad specific variables in echopype output and reduntant configs
# TODO revisit if any of these are needed down the line, we might need to do some data padding in the pipeline
VARIABLES_TO_EXCLUDE = [
    "angle_offset_alongship",  # simrad specific variable?
    "angle_offset_athwartship",  # simrad specific variable?
    "angle_sensitivity_alongship",  # simrad specific variable?
    "angle_sensitivity_athwartship",  # simrad specific variable?
    "beamwidth_alongship",  # simrad specific variable?
    "beamwidth_athwartship",  # simrad specific variable?
    "pressure",  # from APL config not onboard CTD
    "temperature",  # from APL config not onboard CTD
    "salinity",  # from APL config not onboard CTD
    "pH",  # from APL config not onboard CTD
    "sa_correction",  # from APL config not onboard CTD
]

VARIABLES_TO_INCLUDE = [
    "equivalent_beam_angle",
    "echo_range",
    "Sv",
    "gain_correction",
    "impedance_transceiver",
    "formula_absorption",
    "receiver_sampling_frequency",
    "impedance_transducer",
    "frequency_nominal",
    "sound_absorption",
    "sound_speed",
    "source_filenames",
    "water_level",
]

DEFAULT_HARVEST_DEPLOYMENT = "echo_raw_data_harvest_8vcpu_60gb"
DEFAULT_ECHOGRAM_DEPLOYMENT = "daily_echogram_2vcpu_16gb"
