## MARBL Diagnostics

This package uses `xarray` and `Cartopy` to read GCM output / observational datasets and plot state variables.
It can compute climatologies (a task that will eventually be pushed off to `esmlab`) from time slice files or time series files, or it can read pre-computed climatologies.
It can be run as a stand-alone package, or incorporated into something like `CESM_postprocessing`.
