import sys
import os
import numpy as np
import xarray as xr
import zarr
from config import *
from utils_new import extract_tars,create_empty_data,get_coords
import logging
from numcodecs import Zstd #Blosc
import calendar
from itertools import chain
import datetime

def logging_extract_tars(full_path,X,Y,lat,lon,crs_attrs,it_exist):
    if it_exist == 1:
        try:
            return extract_tars(full_path,X,Y,lat,lon,crs_attrs)
        except Exception as e:
            logging.exception(e)
    else:
        try:
            return create_empty_data(full_path,X,Y,lat,lon,crs_attrs)
        except Exception as e:
            logging.exception(e)
    return None

def extract_tars_and_store(full_path,X,Y,lat,lon,crs_attrs,encoding,zarr_path,it_exist):
    ds = logging_extract_tars(full_path,X,Y,lat,lon,crs_attrs,it_exist)
    version = '0.1.0'
    ds.attrs['mlcast_dataset_version'] = version
    ds.attrs['mlcast_created_with'] = f'https://github.com/mlcast-community/mlcast-dataset-DMI-radar_precipitation@{version}'
    now = datetime.datetime.now().isoformat()
    ds.attrs['mlcast_created_on'] = now
    if not os.path.exists(zarr_path):
        ds.to_zarr(zarr_path,mode='w',encoding = encoding,consolidated=True,zarr_format=2)
    else:
        ds.to_zarr(zarr_path,mode='a', append_dim='time',consolidated=True,zarr_format=2)
    with open("back_log.txt","w") as fwa:
        fwa.write(str(full_path))
    return None
if __name__ == "__main__":
    X,Y,lat,lon,crs_attrs = get_coords()
    All_ds = []
    encoding = {"dbz": {"compressor":Zstd(level=3)}}
    cc = calendar.Calendar()

    if is_new == 1:
        st_year = int(init_year)
        mm = 1
        dd = 1
    else:
        with open("back_log.txt","r") as fwa:
            s = fwa.readlines()
        last_date = os.path.basename(s[-1])
        last_date = last_date.split('.')[1]
        last_date = datetime.datetime.strptime(last_date,"%Y%m%d")
        last_date += datetime.timedelta(days=1)
        st_year = last_date.year
        mm = last_date.month
        dd = last_date.day

    en_year = int(final_year)
    years = np.arange(st_year,en_year+1).astype(str)
    years = list(years)

    for year in years:
        yr = int(year)
        year_ds = []
        arg_list = []
        year_path = RADAR_COMPOS / year
        if yr == 2016:
            doy = [cc.itermonthdates(int(year),m) for m in range(3,13)]
        else:
            doy = [cc.itermonthdates(int(year),m) for m in range(1,13)]
        doy = list(chain.from_iterable(doy))

        l_limit = datetime.date(yr,mm,dd)
        u_limit = datetime.date(yr+1,1,1)
        doy = np.unique(doy)
        doy = doy[np.where(np.logical_and(doy>=l_limit,doy<u_limit))]
        for day in doy:#tarFil in os.listdir(year_path):
            day = day.strftime("composite.%Y%m%d.max.h5.tar")
            full_path = year_path / day
            if os.path.exists(full_path):
                it_exist = 1
            else:
                it_exist = 0
            out_name = f'DMI_DataSet_{SpatialRes}m_10min_sec.zarr'
            extract_tars_and_store(full_path,X,Y,lat,lon,crs_attrs,encoding,RADAR_ZARR/out_name,it_exist)

                
