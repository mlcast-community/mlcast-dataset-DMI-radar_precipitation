# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a
Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.1.0](https://github.com/mlcast-community/mlcast-dataset-DMI-radar_precipitation/releases/tag/v0.1.0)

First tagged release of zarr version of the DMI dataset of 10-minute data from 2016-2024. Includes fixes to projection `crs_wkt` string to set x/y coord
units correctly (as `km` rather than `m`) and domain bounds (which are required
when `cartopy` when using the projection for plotting).

## [v0.1.1](https://github.com/mlcast-community/mlcast-dataset-DMI-radar_precipitation/releases/tag/v0.1.1)

Second tagged release fixing a bug in the original dataset. 
