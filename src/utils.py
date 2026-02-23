import os
import h5py
import xarray as xr
import tarfile
from datetime import datetime,timedelta
import numpy as np
import cartopy.crs as ccrs
from pyproj import CRS
import dask.array as da
from config import *

def get_coords():
        proj_str = '+proj=stere +ellps=WGS84 +lat_0=56 +lon_0=10.5666 +lat_ts=56'
        proj = ccrs.CRS(CRS.from_proj4(str(proj_str)))
        src_crs = ccrs.PlateCarree()
        coordArr = np.array([[52.29427206,4.3790827],
                            [52.29427206,18.89328087],[60.,3.],[59.82770843,20.73514017]])
        XY_corners = np.empty(coordArr.shape)
        for n_row in range(4):
            lat,lon = coordArr[n_row,:]
            XY_corners[n_row,:] = proj.transform_point(lon, lat, src_crs)
        Xs = np.linspace(XY_corners[:,0].min(),XY_corners[:,0].max(),Xpixels)
        Ys = np.linspace(XY_corners[:,1].min(),XY_corners[:,1].max(),Ypixels)
        X,Y = np.meshgrid(Xs,Ys)
        lonlat = src_crs.transform_points(proj, X, Y)
        lon = lonlat[..., 0]
        lat = lonlat[..., 1]
        crs_wkt = proj.to_wkt()
        last_bracket_index = crs_wkt.rfind("]")
        min_lat = np.min(lat)
        min_lon = np.min(lon)
        max_lat = np.max(lat)
        max_lon = np.max(lon)
        bbox_str = f"BBOX[{min_lat}, {min_lon}, {max_lat}, {max_lon}]"
        crs_wkt = crs_wkt[:last_bracket_index] + f", {bbox_str}" + crs_wkt[last_bracket_index:]
        crs_attrs = {'spatial_ref':proj.proj4_init,'proj4':proj_str,'crs_wkt':crs_wkt}
        return Xs,Ys,lat,lon,crs_attrs
    
def extract_tars(filename,X,Y,lat,lon,crs_attrs):
    base_name = os.path.basename(filename)
    yymmdd = base_name.split('.')[1]
    yy = yymmdd[:4]
    mm = yymmdd[4:6]
    dd = yymmdd[6:]
    start = datetime.strptime("00:00", "%H:%M")
    end = datetime.strptime("23:59", "%H:%M")
    step = timedelta(minutes=10)
    ds_members = []
    while start<=end:
        HH = str(start.hour)
        if int(HH)<10:
            HH = '0' + HH
        MM = str(start.minute)
        if int(MM)<10:
            MM = '0' + MM
        memberN = f'data/radman/archive/prd/composite/max/{yy}/{yy}-{mm}-{dd}/dk.com.{yymmdd}{HH}{MM}.{SpatialRes}_max.h5'
        with tarfile.open(filename,"r") as tar:
            ls_names = np.array(tar.getnames())
            dateTime = get_time(f'{yymmdd}{HH}{MM}')
            if memberN in ls_names:
                with tar.extractfile(memberN) as tarFile:
                    X_dataset = create_xrdata(tarFile,dateTime)
                    ds_members.append(X_dataset.ds)
            else:
                ds = empty_timestep(dateTime)
                ds_members.append(ds)
        start+=step
    Dagens_dataSet = xr.concat(ds_members,dim='time')
    Dagens_dataSet = AddCoordsAttrs(Dagens_dataSet,X,Y,lat,lon,crs_attrs)
    return Dagens_dataSet

def create_empty_data(filename,X,Y,lat,lon,crs_attrs):
    base_name = os.path.basename(filename)
    yymmdd = base_name.split('.')[1]
    yy = yymmdd[:4]
    mm = yymmdd[4:6]
    dd = yymmdd[6:]
    start = datetime.strptime("00:00", "%H:%M")
    end = datetime.strptime("23:59", "%H:%M")
    step = timedelta(minutes=10)
    ds_members = []
    while start<=end:
        HH = str(start.hour)
        if int(HH)<10:
            HH = '0' + HH
        MM = str(start.minute)
        if int(MM)<10:
            MM = '0' + MM

        dateTime = get_time(f'{yymmdd}{HH}{MM}')
        ds = empty_timestep(dateTime)
        ds_members.append(ds)
        start+=step
    Dagens_dataSet = xr.concat(ds_members,dim='time')
    Dagens_dataSet = AddCoordsAttrs(Dagens_dataSet,X,Y,lat,lon,crs_attrs)
    return Dagens_dataSet

def get_time(dateTime):
    dateTime = datetime.strptime(dateTime, '%Y%m%d%H%M')
    dateTime = np.datetime64(dateTime,"ns")
    return np.array([dateTime])
    
def AddCoordsAttrs(ds,X,Y,lat,lon,crs_attrs):
    ds.coords['y'] = Y
    ds['y'].attrs={'units':'m'}
    ds.coords['x'] = X
    ds['x'].attrs={'units':'m'}
    lat = da.from_array(lat,chunks=(Ypixels,Xpixels))
    lon = da.from_array(lon,chunks=(Ypixels,Xpixels))#864,992
    ds = ds.assign_coords(lon=(('y','x'),lon),lat=(('y','x'),lat))
    ds['lat'].attrs = {'long_name': 'Latitude', 'standard_name': 'latitude', 'units': 'degrees_north'}
    ds['lon'].attrs = {'long_name': 'Longitude', 'standard_name': 'longitude', 'units': 'degrees_east'}
    ds = ds.chunk({"time":1,"y":Ypixels,"x":Xpixels})
    ds['crs'] = xr.DataArray(attrs=crs_attrs)
    #ds['time'].attrs = {'long_name': 'Time','standard_name':'time'}
    ds['dbz'].attrs = {'grid_mapping': 'crs','long_name':'10 min radar reflectivity','standard_name': 'equivalent_reflectivity_factor', 'units':'dBZ'}
    ds.attrs = {'Author':'Thomas Bøvith (tbh@dmi.dk)', 'Conventions': 'CF-1.6','institution': 'Danmarks Meteorologiske Institute (DMI)','license':'CC-BY-4.0', 'title': 'radar-based precipitation', 'mlcast_created_by': 'Ricardo Jara <arjj@dmi.dk>','mlcast_dataset_identifier':"DK-DMI-precipitation",'consistent_timestep_start':'2016-02-29T00:00:00.000000000'}
    return ds

class create_xrdata():
    def __init__(self,tarFile,dateTime):#,X,Y,lat,lon,crs_attrs):
        self.tFile = tarFile
        self.dateTime = dateTime
        self.gain = 0.5
        self.offset = -32
        self.get_info_from_composite()
        self.create_xarray()
    def get_info_from_composite(self):
        self.attrDict = {}
        with h5py.File(self.tFile,'r') as h:
            for key,val in h['where'].attrs.items():
                if isinstance(val,np.bytes_):
                   val = val.astype(str)
                elif isinstance(val,np.ndarray):
                   val=val[0]
                self.attrDict[key] = val
            self.attrDict['date'] = h['what'].attrs['date'].astype(str)
            self.attrDict['time'] = h['what'].attrs['time'].astype(str)
        return self.attrDict
    def create_xarray(self):
        try:
            ds = xr.open_dataset(self.tFile,group='dataset1/data1',engine="h5netcdf", chunks = -1,phony_dims='sort')
            ds = ds.rename({'data':'dbz','phony_dim_0':'y','phony_dim_1':'x'})
            ds['dbz'] = ds['dbz'].where((ds['dbz']!=255)),other=np.nan)
            ds['dbz'] = ds['dbz']*self.gain +self.offset
            ds['dbz'] = ds['dbz'][::-1, :]
            self.ds = ds.expand_dims(time=self.dateTime)
            self.ds['time'].attrs = {'long_name': 'Time','standard_name':'time'}
            self.ds.load()
            return self.ds
        except Exception as e:
            self.ds = empty_timestep(self.dateTime)
            return self.ds

def empty_timestep(dateTime):
    data = np.ones((Ypixels,Xpixels))*np.nan
    data = da.from_array(data,chunks=-1)
    ds = xr.Dataset(
            data_vars={'dbz':(("y","x"), data)})
    ds = ds.expand_dims(time=dateTime)
    ds['time'].attrs = {'long_name': 'Time','standard_name':'time'}
    return ds





