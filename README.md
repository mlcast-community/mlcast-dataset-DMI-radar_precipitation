# mlcast-dataset-DMI
This repository contains code to convert the HDF5 files from the DMI-radar dataset to Zarr format. The process is as follow:
  1. Extract the HDF5 files from the tar archive file. For this the `tarfile` python package.
  2. Then using  the `xarray` python package to read the data of the HDF5 and create a dataset.
  3. Combine all the datasets through the `time` dimension.
  4. Finally, convert the final dataset in to zarr by using the `to_zarr()` function from `xarray`.

##Usage

All dependencies can be installed with [uv](https://docs.astral.sh/uv/).
And the conversion can be done by just running [mlcast-dataset-DMI/src
/conversion.py
](https://github.com/mlcast-community/mlcast-dataset-DMI/blob/main/src/conversion.py):
```bash
python conversion.py
```
