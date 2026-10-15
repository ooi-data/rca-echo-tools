# rca-echo-tools
Built on echopype to create rca echosounder zarrs and visualize echosounder data.

# echopype!
https://echopype.readthedocs.io/en/latest/

# RCA echogram cli usage

Harvest a range of days, create new zarr store: 
```
rca-echo-harvest --refdes "CE04OSPS-PC01B-05-ZPLSCB102" \
--start-date "2026/01/01" \
--end-date "2026/02/18" \
--waveform-mode "CW" \
--encode-mode "power" \
--sonar-model "EK80" \
--run-type "refresh" \
--cloud "True"
```
Append to existing zarr store:
```
rca-echo-harvest --refdes "CE04OSPS-PC01B-05-ZPLSCB102" \
--start-date "2026/02/19" \
--end-date "2026/02/21" \
--waveform-mode "CW" \
--encode-mode "power" \
--sonar-model "EK80" \
--run-type "append" \
--cloud "True"
```
Create daily echograms in parallel on RCA ECS cluster:
```
rca-daily-echograms --refdes "CE04OSPS-PC01B-05-ZPLSCB102" \
--start-date "2026/01/01" \
--end-date "2026/02/21" \
--parallel-in-cloud "False" \
--s3-sync "True"
```

# S3 storage locations

| Data | Bucket | Path pattern |
|------|--------|--------------|
| Zarr data store | `s3://ooi-data` | `{refdes}-streamed-zplsc_volume_scattering/{subdeployment_id}` |
| Harvest status metadata JSON | `s3://flow-process-bucket` | `harvest-status/{refdes}-streamed-zplsc_volume_scattering/{subdeployment_id}` |
| Echogram PNGs | `s3://ooi-rca-qaqc-prod` | `echograms/{year}/{instrument}/` |

`subdeployment_id` is an integer defined in `rca_echo_tools/config/processing_deployments.yaml` that groups date ranges sharing the same EK80 configuration.

# EK80 deployment configuration notes 
Oregon Shelf system `CE02SHBP-MJ01C-07-ZPLSCB101` was running in CW mode for almost all of the 2024-2025 deployment. 
The Offshore system `CE04OSPS-PC01B-05-ZPLSCB102` was in CW for the first week or so and then in limited FM mode.

Both systems were put into alternating CW/FM between `2025-05-30` and `2025-07-18`.

# Planned improvements

Drawn from [ooi-zpls-echograms#8](https://github.com/oceanobservatories/ooi-zpls-echograms/pull/8), which processes the same two instruments as a filesystem-based weekly batch.

- **Per-file error isolation.** Skip a file that fails `compute_Sv`, log the reason, carry on with the rest of the day, and record the skip in the harvest status JSON. Today the exception propagates out of the write loop: files already appended stay in the zarr as a partial day with no metadata entry, so a re-run clears the overlap guard and appends them a second time.
- **Detect `encode_mode` from the data** instead of hardcoding `--encode-mode power`, so complex-only files calibrate rather than raising `RuntimeError`. Two limits worth knowing: a file carrying CW power in `Beam_group2` alongside BB complex in `Beam_group1` already works today, and echopype cannot split CW from BB within a channel (it reads `transmit_type` at ping zero only, upstream TODO), so per-channel is the finest selection available to either pipeline.
- **Channel count guard.** Assert `compute_Sv` returns the expected three channels before writing or plotting, and skip with a logged reason when it does not.
- **Fixed vertical range.** Per-refdes y-axis limits on the echogram, so range is comparable across days the way the fixed 0000-2400 x-axis already makes time comparable.
- **Explicit `env_params`.** We pass none, so `compute_Sv` calibrates against the temperature/salinity/pressure constants embedded in the file by the APL config — dropping those variables from the stored output does not change the calibration. Worth deciding whether to supply site values deliberately.
