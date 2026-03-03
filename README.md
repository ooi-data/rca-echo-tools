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

# EK80 deployment configuration nodes 
Oregon Shelf system `CE02SHBP-MJ01C-07-ZPLSCB101` was running in CW mode for almost all of the 2024-2025 deployment. 
The Offshore system `CE04OSPS-PC01B-05-ZPLSCB102` was in CW for the first week or so and then in limited FM mode.

Both systems were put into alternating CW/FM between `2026-05-30` and `2025-07-18`.
