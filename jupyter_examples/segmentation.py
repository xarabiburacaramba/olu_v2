# %%
!!pip install sentinelsat

# %%
#system libraries
import datetime
import zipfile
import shutil
import math
import os
import sys
import json
from importlib import reload
#for matrix computations
import numpy as np
from numpy import ma
np.set_printoptions(threshold=sys.maxsize)
#for image segmentation
from skimage.color import rgb2gray
from skimage.filters import sobel
from skimage.segmentation import slic
from skimage.segmentation import mark_boundaries
#for downloading Sentinel imagery
from sentinelsat import SentinelAPI
#for working with geospatial data
from osgeo import gdal,ogr, gdalnumeric, osr, gdal_array
#helping classes
import tridy
from tridy import GeoConcept, SubGeoConcept, MetaData, Table, View, DBStorage, DataSource, Feature, FeatureWithID,  AdmUnitFeature, FeatureWithRasterMap, OLUFeature, Grid, Imagee, ds_from_metadata, xml_lpis_cz_reader, lpis_cz__posledni_aktualizace, get_listvalues_from_generator, apply_function, select_nodes_from_graph, unzip_file, find_neighbors_till, connection_parameters_to_pg, transform_name_to_postgresql_format, transform_wkt_geometry, world_to_pixel 
#for plotting
import matplotlib.pyplot as plt
#for curiosity
from scipy.stats import wasserstein_distance

# %%
del(Imagee)
reload(tridy)
from tridy import Imagee

# %%
json_feature_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_feature_with_raster_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"raster_maps","type":"raster"}]

# %%
#helping function that creates regular grid from affine transformation parameters
def grid_from_affine_transformation(imagee):
    return Grid((imagee.get_metadata()['affine_transformation'][0],imagee.get_metadata()['affine_transformation'][3]),(imagee.get_metadata()['affine_transformation'][1],imagee.get_metadata()['affine_transformation'][5]),*imagee.get_data().shape)

# %%
#helping function that is used to select just segments that have land use homogenity over given percentage
def select_segment_lu(intersecting_features,percentage):
    for i in range(len(intersecting_features)):
        if i==0:
            dictionar=intersecting_features[i]
        else:
            key=list(intersecting_features[i].keys())[0] 
            if key in list(dictionar.keys()):
                dictionar[key]=dictionar[key]+intersecting_features[i][key]
            else:
                dictionar[key]=intersecting_features[i][key]
    for key in dictionar.keys():
        if dictionar[key]/np.sum(list(dictionar.values())) >= percentage:
            return key
        else:
            return '0'

# %%
#instantiating SentinelAPI connection for imagery download
api = SentinelAPI('mitja', 'Copernicus12!','https://scihub.copernicus.eu/dhus')

# %%
wkt_point='POINT (23.6383667 40.5790194)'
time_period=(datetime.date(2020, 3, 1),datetime.date(2020,5,1))
cloudcover_range=(0, 1)

# %%
products = api.query(wkt_point, date=time_period, platformname='Sentinel-2', cloudcoverpercentage=cloudcover_range)

# %%
min_coverage = 1
for p in products:
    if 'tileid' in list(products[p].keys()):
        if products[p]['cloudcoverpercentage']<min_coverage:
            min_coverage=products[p]['cloudcoverpercentage']
            product=p

# %%
title=api.download(product)['title']

# %%
k=products[product]['tileid']

# %%
zip=zipfile.ZipFile(title+'.zip')
os.mkdir(str(k))
for band in (2,4,8):
        with zip.open([b for b in zip.namelist() if b.endswith('B0%s.jp2' % band)][0]) as zf, open(os.getcwd()+'/'+str(k)+'/'+title+'_b%d.jp2' % band, 'wb') as f:
                shutil.copyfileobj(zf, f) 

# %%
ds_b2=gdal.Open(str(k)+'/'+[i for i in os.listdir(str(k)) if i.endswith('b2.jp2')][0])

# %%
ds_b4=gdal.Open(str(k)+'/'+[i for i in os.listdir(str(k)) if i.endswith('b4.jp2')][0])
ds_b8=gdal.Open(str(k)+'/'+[i for i in os.listdir(str(k)) if i.endswith('b8.jp2')][0])

# %%
wgs84_sr,utm32635_sr=[osr.SpatialReference() for i in range(2)]

wgs84_sr.ImportFromProj4('+proj=longlat +datum=WGS84 +no_defs')
utm32635_sr.ImportFromProj4('+proj=utm +zone=35 +datum=WGS84 +units=m +no_defs')

utm32635_to_wgs84=osr.CoordinateTransformation(utm32635_sr,wgs84_sr)
wgs84_to_utm32635=osr.CoordinateTransformation(wgs84_sr,utm32635_sr)

# %%
metadata_dict={}
metadata_dict['affine_transformation']=ds_b2.GetGeoTransform()
metadata_dict['nodata']=ds_b2.GetRasterBand(1).GetNoDataValue()
metadata_dict['proj_wkt']=utm32635_sr.ExportToWkt()

# %%
grid=Grid((metadata_dict['affine_transformation'][0],metadata_dict['affine_transformation'][3]),(metadata_dict['affine_transformation'][1],metadata_dict['affine_transformation'][5]))

# %%
geom_4326=ogr.CreateGeometryFromWkt(wkt_point)

# %%
geom_4326.Transform(wgs84_to_utm32635)

# %%
geom_utm_envelope=( geom_4326.GetX()-1000, geom_4326.GetX()+1000, geom_4326.GetY()-1000, geom_4326.GetY()+1000 )

# %%
origin=tuple(grid.find_index((geom_utm_envelope[0],geom_utm_envelope[3])))

# %%
size=tuple(np.array(grid.find_index((geom_utm_envelope[1],geom_utm_envelope[2])))-np.array(grid.find_index((geom_utm_envelope[0],geom_utm_envelope[3]))))+np.array([1,1])

# %%
a=np.array(ds_b2.GetRasterBand(1).ReadAsArray(xoff=int(origin[0]),yoff=int(origin[1]),win_xsize=int(size[0]),win_ysize=int(size[1])))

# %%
grid_clip=Grid(np.array(grid.get_gridorigin())+np.array(tuple(grid.find_index((geom_utm_envelope[0],geom_utm_envelope[3]))))*np.array(grid.get_gridstepsize()),(metadata_dict['affine_transformation'][1],metadata_dict['affine_transformation'][5]))

# %%
clip_metadata_dict={}
clip_metadata_dict['affine_transformation']=grid_clip.get_affinetransformation()
clip_metadata_dict['nodata']=float(np.min(a)) if np.min(a)<0 else np.nan
clip_metadata_dict['proj_wkt']=utm32635_sr.ExportToWkt()

# %%
im=Imagee(a,clip_metadata_dict)

# %%
im.export_as_tif('vyrez.tif')

# %%
a=np.array(ds_b4.GetRasterBand(1).ReadAsArray(xoff=int(origin[0]),yoff=int(origin[1]),win_xsize=int(size[0]),win_ysize=int(size[1])))
im_b4=Imagee(a,clip_metadata_dict)
im_b4.export_as_tif('vyrez_b4.tif')

# %%
a=np.array(ds_b8.GetRasterBand(1).ReadAsArray(xoff=int(origin[0]),yoff=int(origin[1]),win_xsize=int(size[0]),win_ysize=int(size[1])))
im_b8=Imagee(a,clip_metadata_dict)
im_b8.export_as_tif('vyrez_b8.tif')

# %%
b_all=np.dstack((im_b4.get_data(),im_b8.get_data(),im.get_data()))

# %%
segments_slic = slic(b_all, n_segments=round(math.sqrt(ma.count(im.get_data()))*1.5), max_iter=100, compactness=0.5, sigma=1, multichannel=True, convert2lab=True, enforce_connectivity=True, min_size_factor=0.03, max_size_factor=5, slic_zero=True)

# %%
segments=Imagee(segments_slic,im.get_metadata())

# %%
segments.export_as_tif('vyrez_segmenty.tif')

# %%
c=np.dstack((b_all,segments.get_data()))
#vectorize segment data
segments_polygons=gdal_array.OpenNumPyArray(segments.get_data(),binterleave=True)
segments_polygons.SetGeoTransform(segments.get_metadata()['affine_transformation'])
segments_polygons.SetProjection(segments.get_metadata()['proj_wkt'])
srs=osr.SpatialReference()
srs.ImportFromWkt(segments.get_metadata()['proj_wkt'])
#outDriver=ogr.GetDriverByName('MEMORY')
outDriver=ogr.GetDriverByName('ESRI Shapefile')
outDataSource=outDriver.CreateDataSource('segments.shp')
outLayer = outDataSource.CreateLayer("data", srs,geom_type=ogr.wkbPolygon)
pixel_value = ogr.FieldDefn("pixel_value", ogr.OFTInteger)
outLayer.CreateField(pixel_value)
gdal.Polygonize( segments_polygons.GetRasterBand(1) , None, outLayer, 0)

# %%
del(outLayer,outDataSource,outDriver)

# %%
fields_metadata=MetaData('Identified segments (fields)',
                             {"local":"segments.shp", "format":"ESRI Shapefile"},
                             'data')

fields_ds=ds_from_metadata(fields_metadata)

fields=GeoConcept('Identified segments (fields)','Identified segments (fields) based on 2,4 and 8 bands of Sentinel',
                      'Feature',json_feature_structure, data_source=fields_ds, subgeoconcepts=[] )

# %%
fields.get_data_source().set_data_file(fields.get_data_source().get_attributes()['local'])

# %%
fields.set_geojson_output_backend(os.getcwd()+'/','segments.geojson')
with open(fields.get_geojson_output_backend(), 'w', encoding='utf-8') as file:
    geojson={"type": "FeatureCollection", "features": [] }
    features=fields.get_data_source().read_features('feature',number=10)
    for f in features:
            if len(f)>0:
                for feature in f:
                    geojson["features"].append(feature.export_to_geojson())
    json.dump(geojson, file, ensure_ascii=False, indent=4)

# %%
def compilable_transformation_dictionary(feature_var, sentinel_im, geometry_transformation=None):
    t_dict=\
    {'feature':{'object':feature_var}, \
    'feature_id':{'function':(lambda y: y.get_id() if type(y)!=tridy.Feature else y.get_data()['pixel_valu']),'parameters':['feature']},\
    'feature_data':{'function':(lambda y: y.get_data()),'parameters':['feature']},\
    'feature_geometry':{'function':(lambda y: y.get_geometry()),'parameters':['feature']},\
    'sentinel_im':{'object': sentinel_im}, \
    'im_band2_cropped':{'function':(lambda y,  z:  Imagee(*y.get_raster_map()['im_band2'].clip_by_shape(z))    ),'parameters':['sentinel_im','feature_geometry']},\
    'im_band4_cropped':{'function':(lambda y,  z:  Imagee(*y.get_raster_map()['im_band4'].clip_by_shape(z))    ),'parameters':['sentinel_im','feature_geometry']},\
    'im_band8_cropped':{'function':(lambda y,  z:  Imagee(*y.get_raster_map()['im_band8'].clip_by_shape(z))    ),'parameters':['sentinel_im','feature_geometry']},\
    'cri2': {'function':(lambda y,  z:  Imagee((1 / (y.get_data()/10000) ) - (1 / (z.get_data()/10000) ),  y.get_metadata() ) ),'parameters':['im_band2_cropped', 'im_band8_cropped']},\
    'msr': {'function':(lambda x,  y,  z:  Imagee( (z.get_data()-x.get_data())/(y.get_data()-x.get_data()) ,  y.get_metadata() ) ),'parameters':['im_band2_cropped', 'im_band4_cropped', 'im_band8_cropped']},\
    'geometry_transformation':{'object': geometry_transformation}, \
    'transformed_geometry':{'function':(lambda y,  z:  transform_wkt_geometry(y,z) if z!=None else y ),'parameters':['feature_geometry','geometry_transformation']},\
    'result_feature':{'function':(lambda v,  w,  x,  y,  z:  FeatureWithID(data={**v, **{'cri2':y.get_statistics()}, **{'msr':z.get_statistics()}}, geom=w, id=x)   ),'parameters':['feature_data','transformed_geometry','feature_id','cri2', 'msr']},\
    }
    return t_dict

# %%
sentinel_im=FeatureWithRasterMap(data={},geom=('POLYGON ((%d %d, %d %d, %d %d, %d %d, %d %d))' % (geom_utm_envelope[0],geom_utm_envelope[2],geom_utm_envelope[0],geom_utm_envelope[3],geom_utm_envelope[1],geom_utm_envelope[3],geom_utm_envelope[1],geom_utm_envelope[2],geom_utm_envelope[0],geom_utm_envelope[2])),id=1,raster_map={'im_band2':im,'im_band4':im_b4,'im_band8':im_b8})

# %%
features=fields.read_features_from_geojson_output_backend('feature',number=10)

# %%
with open('testovaci.geojson', 'w', encoding='utf-8') as file:
    geojson={"type": "FeatureCollection", "features": [] }
    for f in features:
        for ff in f:
            if len(ff)>0:
                for fff in ff:
                    geojson["features"].append(apply_function(compilable_transformation_dictionary(fff,sentinel_im,utm32635_to_wgs84),'result_feature').export_to_geojson())
    json.dump(geojson, file, ensure_ascii=False, indent=4)

# %%
from ipyleaflet import Map, GeoJSON
m = Map(center=(ogr.CreateGeometryFromWkt(transform_wkt_geometry(sentinel_im.get_geometry(),utm32635_to_wgs84)).Centroid().GetY(),ogr.CreateGeometryFromWkt(transform_wkt_geometry(sentinel_im.get_geometry(),utm32635_to_wgs84)).Centroid().GetX()), zoom=14)

# %%
geo_json = GeoJSON(data=geojson, style = {'color': 'green'})
m.add_layer(geo_json)

# %%
m

# %%
for feature in geojson['features']:
    if feature['properties']['cri2']['max']<9:
        geojson['features'].remove(feature)

# %%
geo_json = GeoJSON(data=geojson, style = {'color': 'red'})
m.add_layer(geo_json)

# %%
geojson['features'][0]

# %%
plt.imshow(im_b8.get_data(), origin="upper", cmap='gray', interpolation='nearest')
plt.colorbar()
plt.show()

# %%
plt.imshow(segments.get_data(), origin="upper", cmap='gray', interpolation='nearest')
plt.colorbar()
plt.show()

# %%
products[product]

# %%
