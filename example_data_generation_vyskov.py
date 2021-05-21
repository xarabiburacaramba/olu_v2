
import tridy
from tridy import GeoConcept, SubGeoConcept, MetaData, DBStorage, DataSource, View, Table, Imagee, Grid, ds_from_metadata, lpis_cz__posledni_aktualizace, apply_function, select_nodes_from_graph, unzip_file, transform_name_to_postgresql_format
import requests
import datetime
import re
import os
import json
import sys
import binascii
from copy import copy, deepcopy
from osgeo import gdal, ogr, osr, gdalnumeric

import psycopg2

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from credentials import connectionstring_localhost

import matplotlib.pyplot as plt
from matplotlib.pyplot import imshow

import math
from PIL import Image, ImageDraw
from numpy import ma
import numpy as np
np.set_printoptions(threshold=sys.maxsize)

from pygeoprocessing import routing

import tempfile
from tempfile import TemporaryDirectory


from importlib import reload


del Imagee, Table, MetaData, DataSource, DBStorage
reload(tridy)
from tridy import Imagee, Table, MetaData, DataSource, DBStorage


def get_twi(im_object):
    with TemporaryDirectory() as tempdir:
        im_object.export_as_tif('%s/dem.tif' %tempdir)
        routing.fill_pits(('%s/dem.tif' %tempdir,1),'%s/cdem.tif' %tempdir)
        routing.flow_dir_mfd(('%s/cdem.tif' %tempdir,1),'%s/dir.tif' %tempdir)
        routing.flow_accumulation_mfd(('%s/dir.tif' %tempdir,1),'%s/twi.tif' %tempdir)
        ds = gdal.Open('%s/twi.tif' %tempdir)
        metadata_dict=im_object.get_metadata()
        twi=Imagee(np.array(ds.GetRasterBand(1).ReadAsArray()),metadata_dict)
        return twi

def sequence_generator(starting_number):
    while starting_number<=10000000000000:
        starting_number+=1
        yield(starting_number)
        
def find_intersecting_features(geom,lyr):
    features=[]
    for i in range(lyr.GetFeatureCount()):
        feature_in_lyr=lyr.GetNextFeature()
        geom_in_lyr=feature_in_lyr.GetGeometryRef()
        if geom.Intersects(geom_in_lyr):
            features.append(feature_in_lyr)
        else:
            pass   
    return features

def find_neighbors_level(graph,start_node,level):
    if graph.nodes()[start_node]['level']==level:
        yield start_node
    else:
        for n in graph.neighbors(start_node):
            yield from find_neighbors_level(graph,n,level) 
    
def ua_cz_gpkg_layer(data_file):
    return re.search('CZ.*_.*_',data_file.split('/')[-1])[0]+'UA2012'

def compilable_ua_dictionary(object): 
    o_dict=\
    {'geoconcept':{'object':object},\
     'geoconcept_name':{'object':'geoconcept','function':'get_name'},\
     'geoconcept_data_source':{'object':'geoconcept','function':'get_data_source'},\
     'geoconcept_data_source_file':{'object':'geoconcept_data_source','function':'get_data_file'},\
     'ua_gpkg_layer':{'function':ua_cz_gpkg_layer, 'parameters':['geoconcept_data_source_file']},\
    }
    return o_dict
    
def compilable_tree_dictionary(object): 
    g_dict=\
    {'admunit':{'object':object},\
    'admunit__tree':{'object':'admunit','function':'return_graph_representation'},\
    'admunit__tree__reverse':{'object':'admunit__tree','function':'reverse'},\
    'admunit__tree__level3':{'function':select_nodes_from_graph,'parameters':['admunit__tree','level',3]},\
    'admunit__tree__level4':{'function':select_nodes_from_graph,'parameters':['admunit__tree','level',4]}}
    return g_dict    

def compilable_node_dictionary(object,node__level=0,node__name='1'): 
    g_dict=\
    {'admunit':{'object':object},\
    'admunit__tree':{'object':'admunit','function':'return_graph_representation'},\
    'admunit__tree__reverse':{'object':'admunit__tree','function':'reverse'},\
    'node__level':{'object':node__level},\
    'node__name':{'object':node__name},\
    'admunit__tree__level':{'function':select_nodes_from_graph,'parameters':['admunit__tree','level','node__level']},\
    'admunit__tree__neighbors':{'function':find_neighbors_level,'parameters':['admunit__tree','node__name','node__level']},\
    'admunit__tree__neighbors__3201__4':{'function':find_neighbors_level,'parameters':['admunit__tree__reverse','3201',4]},\
    'admunit__tree__neighbors__3201__3':{'function':find_neighbors_level,'parameters':['admunit__tree__reverse','3201',3]},\
    }
    return g_dict

def isric_reader(bbox_homolosine,short_name,full_name,depth,statistical_measure,location_name,download=True):
    depth_um='cm'
    ds=None
    if download is True:
        homolosine_sr=osr.SpatialReference()
        homolosine_sr.ImportFromProj4('+proj=igh +lat_0=0 +lon_0=0 +datum=WGS84 +units=m +no_defs')
        res=250
        location = "https://files.isric.org/soilgrids/latest/data/"
        sg_url = f"/vsicurl?max_retry=3&retry_delay=1&list_dir=no&url={location}"
        kwargs = {'format': 'GTiff', 'projWin': bbox_homolosine, 'projWinSRS': homolosine_sr.ExportToProj4(), 'xRes': res, 'yRes': res, 'creationOptions': ["TILED=YES", "COMPRESS=DEFLATE", "PREDICTOR=2", "BIGTIFF=YES"]}
        fn_local= location_name+'_'+short_name+'_'+str(depth[0])+'-'+str(depth[1])+depth_um+'_'+statistical_measure+'.tif'
        ds = gdal.Translate(fn_local, sg_url + short_name+'/'+short_name+'_'+str(depth[0])+'-'+str(depth[1])+depth_um+'_'+statistical_measure+'.vrt', **kwargs)
        del(ds)
    else:
        fn_local=location_name+'_'+short_name+'_'+str(depth[0])+'-'+str(depth[1])+depth_um+'_'+statistical_measure+'.tif'
    return fn_local if os.path.isfile(fn_local) else None


replacement_dictionary = {"[posledni_den_mesice]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d'),"[lpis_cz__posledni_aktualizace]":lpis_cz__posledni_aktualizace().strftime('%Y%m%d'), "[vcera]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d')} 
json_feature_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_feature_with_bigid_structure=[{"name":"id","type":"bigint primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_feature_with_bigid__without_pk_structure=[{"name":"id","type":"bigint"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_admin_unit_structure=[{"name":"id","type":"integer primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_admin_unit_structure_at=[{"name":"id","type":"text primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_feature_with_raster_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"raster_maps","type":"raster"}]


lpis_hilucs={'2':'111', '3':'111', '4':'111', '5':'111', '6':'110', '7':'110', '9':'110', '10':'110', '11':'110', '12':'631', '91':'120', '97':'142', '98':'120', '99':'120'} 
ruian_zpv_hilucs={'1':'112', '2':'120', '3':'121', '4':'120', '5':'120', '6':'632', '7':'632', '8':'632', '9':'632', '10':'632', '11':'632', '12':'530', '13':'530', '14':'411', '15':'411', '16':'411', '17':'411', '18':'415', '19':'344', '20':'343', '21':'335', '22':'341', '23':'411', '24':'130', '25':'433', '26':'650', '27':'630', '28':'632', '29':'244','30':'630'}  
ruian_dp_hilucs={'2':'111', '3':'111', '4':'111', '5':'500', '6':'110', '7':'110', '8':'110', '10':'120', '11':'632', '13':'510', '14':'660'} 
corine_hilucs={'111':'500', '112':'500', '121':'660', '122':'410', '123':'414', '124':'413', '131':'130', '132':'433', '133':'660', '141':'340', '142':'340', '211':'111', '212':'111', '213':'111', '221':'111', '222':'111', '223':'111', '231':'111', '241':'111', '242':'111', '243':'111', '244':'120', '311':'120', '312':'120', '313':'120', '321':'631', '322':'631', '323':'631', '324':'631', '331':'631', '332':'631', '333':'631', '334':'620', '335':'631', '411':'631', '412':'631', '421':'631', '422':'631', '423':'631', '511':'414', '512':'660', '521':'660', '522':'414', '523':'660'} 
ua_hilucs={'11100':'500', '11200':'500', '11210':'500', '11220':'500', '11230':'500', '11240':'500', '11300':'500', '12100':'300', '12200':'410', '12210':'411', '12220':'411', '12230':'412', '12300':'414', '12400':'413', '13100':'130', '13300':'600', '13400':'600', '14100':'344', '14200':'340', '21000':'110', '22000':'110', '23000':'110', '24000':'110', '25000':'110', '30000':'120', '31000':'120', '32000':'120', '33000':'660', '40000':'660', '50000':'660', '91000':'660', '92000':'660'} 
lpis_clc={'2':'210','3':'220','4':'220','5':'220','6':'200','7':'231','9':'200','10':'200','11':'200','12':'240','91':'200','97':'500','98':'300','99':'300'}
ruian_zpv_clc={'1':'200','2':'200','3':'300','4':'300','5':'300','6':'510','7':'510','8':'510','9':'510','10':'510','11':'410','12':'100','13':'130','14':'122','15':'122','16':'122','17':'122','18':'122','19':'141','20':'142','21':'100','22':'100','23':'120','24':'131','25':'132','26':'100','27':'990','28':'510','29':'120','30':'300'}
ruian_dp_clc={'2':'210','3':'220','4':'220','5':'141','6':'200','7':'231','8':'231','10':'300','11':'510','13':'110','14':'990'}
ua_clc={'11100':'111','11200':'112','11210':'112','11220':'112','11230':'112','11240':'112','11300':'100','12100':'120','12210':'122','12200':'122','12220':'122','12230':'122','12300':'123','12400':'124','13100':'130','13300':'133','13400':'990','14100':'141','14200':'142','21000':'200','22000':'220','23000':'230','24000':'240','25000':'200','30000':'300','31000':'300','32000':'320','33000':'330','40000':'410','50000':'510','91000':'999','92000':'999'}


wgs84_sr=osr.SpatialReference()
wgs84_sr.ImportFromProj4('+proj=longlat +datum=WGS84 +no_defs')

sjtsk5514_sr=osr.SpatialReference()
sjtsk5514_sr.ImportFromProj4('+proj=krovak +lat_0=49.5 +lon_0=24.83333333333333 +alpha=30.28813975277778 +k=0.9999 +x_0=0 +y_0=0 +ellps=bessel +units=m +towgs84=570.8,85.7,462.8,4.998,1.587,5.261,3.56 +no_defs')

etrs3035_sr=osr.SpatialReference()
etrs3035_sr.ImportFromProj4('+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')

utm32633_sr=osr.SpatialReference()
utm32633_sr.ImportFromProj4('+proj=utm +zone=33 +datum=WGS84 +units=m +no_defs')

homolosine_sr=osr.SpatialReference()
homolosine_sr.ImportFromProj4('+proj=igh +lat_0=0 +lon_0=0 +datum=WGS84 +units=m +no_defs')

sjtsk5514_to_etrs3035=osr.CoordinateTransformation(sjtsk5514_sr,etrs3035_sr)
sjtsk5514_to_wgs84=osr.CoordinateTransformation(sjtsk5514_sr,wgs84_sr)
wgs84_to_sjtsk5514=osr.CoordinateTransformation(wgs84_sr,sjtsk5514_sr)
wgs84_to_etrs3035=osr.CoordinateTransformation(wgs84_sr,etrs3035_sr)
etrs3035_to_wgs84=osr.CoordinateTransformation(etrs3035_sr,wgs84_sr)
etrs3035_to_sjtsk5514=osr.CoordinateTransformation(etrs3035_sr,sjtsk5514_sr)
utm32633_to_wgs84=osr.CoordinateTransformation(utm32633_sr,wgs84_sr)
wgs84_to_utm32633=osr.CoordinateTransformation(wgs84_sr,utm32633_sr)
wgs84_to_wgs84=osr.CoordinateTransformation(wgs84_sr,wgs84_sr)
wgs84_to_homolosine=osr.CoordinateTransformation(wgs84_sr,homolosine_sr)
homolosine_to_wgs84=osr.CoordinateTransformation(homolosine_sr,wgs84_sr)


def ua_transformations(object,object_fid,attributes_fid,admunit_id,object_table_name,attributes_table_name,attributes2object_table_name,object2nuts_table_name): 
    o_dict=\
    {'feature':{'object':object},\
     'feature_json':{'function':(lambda y: json.loads(y.ExportToJson()) ),'parameters':['feature']},\
     'object_id':{'object':object_fid},\
     'attributes_id':{'object':attributes_fid},\
     'admunit_id':{'object':admunit_id},\
     'object_table_name':{'object':object_table_name},\
     'attributes_table_name':{'object':attributes_table_name},\
     'attributes2object_table_name':{'object':attributes2object_table_name},\
     'object2nuts_table_name':{'object':object2nuts_table_name},\
     'object_geom':{'function':(lambda y: y.GetGeometryRef().ExportToWkt() ),'parameters':['feature']},\
     'object_properties':{'function':(lambda x,y,z: {**{'fid':x},**{'dataset_id':3},**{'z_value':1000},**{'admunit_id':y},**{'valid_from':(z['properties']['prod_date']+'-01-01'),'valid_to':'2024-01-01'} }),'parameters':['object_id','admunit_id','feature_json']},
     'attributes_properties':{'function':(lambda x,y: {**{'fid':x},**{'hilucs_value':ua_hilucs[y['properties']['code2012']]},**{'clc_value':ua_clc[y['properties']['code2012']]},**{'atts':y['properties']},**{'dataset_id':3}}),'parameters':['attributes_id','feature_json']},
     'object_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,dataset_fid,z_value,geom,valid_from,valid_to) VALUES (%s,%s,%s,ST_SetSRID(ST_Multi(ST_GeomFromText('%s')),4326),'%s','%s')" % (w,x,z['dataset_id'],z['z_value'],y,z['valid_from'], z['valid_to'] ) ),'parameters':['object_table_name','object_id','object_geom','object_properties']},\
     'attributes_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,hilucs_id,atts,dataset_fid,clc_id) VALUES (%s,%s,('%s'::json),%s,%s)" % (w, x,z['hilucs_value'],json.dumps(y['properties']),z['dataset_id'],z['clc_value']) ),'parameters':['attributes_table_name','attributes_id','feature_json','attributes_properties']},\
     'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,1)" % (z,x,y) ),'parameters':['object_id','attributes_id','attributes2object_table_name']},\
     'object2admunit_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,unit_fid) VALUES (%s,'%s')" % (z,x,y) ),'parameters':['object_id','admunit_id','object2nuts_table_name']},\
     'four_insert_statements':{'function':(lambda w,x,y,z: [w,x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement','object2admunit_insert_statement']}
    }
    return o_dict

def corine_transformations(object,object_fid,attributes_fid,admunit_id,object_table_name,attributes_table_name,attributes2object_table_name,object2nuts_table_name): 
    o_dict=\
    {'feature':{'object':object},\
     'feature_json':{'function':(lambda y: json.loads(y.ExportToJson()) ),'parameters':['feature']},\
     'object_id':{'object':object_fid},\
     'attributes_id':{'object':attributes_fid},\
     'admunit_id':{'object':admunit_id},\
     'object_table_name':{'object':object_table_name},\
     'attributes_table_name':{'object':attributes_table_name},\
     'attributes2object_table_name':{'object':attributes2object_table_name},\
     'object2nuts_table_name':{'object':object2nuts_table_name},\
     'object_geom':{'function':(lambda y: y.GetGeometryRef().ExportToWkt() ),'parameters':['feature']},\
     'object_properties':{'function':(lambda x,y: {**{'fid':x},**{'dataset_id':4},**{'z_value':100},**{'admunit_id':y},**{'valid_from':'2018-01-01','valid_to':'2024-01-01'} }),'parameters':['object_id','admunit_id']},
     'attributes_properties':{'function':(lambda x,y: {**{'fid':x},**{'hilucs_value':corine_hilucs[y['properties']['clc_code']]},**{'clc_value':y['properties']['clc_code']},**{'atts':y['properties']},**{'dataset_id':4}}),'parameters':['attributes_id','feature_json']},
     'object_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,dataset_fid,z_value,geom,valid_from,valid_to) VALUES (%s,%s,%s,ST_SetSRID(ST_Multi(ST_GeomFromText('%s')),4326),'%s','%s')" % (w,x,z['dataset_id'],z['z_value'],y,z['valid_from'], z['valid_to'] ) ),'parameters':['object_table_name','object_id','object_geom','object_properties']},\
     'attributes_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,hilucs_id,atts,dataset_fid,clc_id) VALUES (%s,%s,('%s'::json),%s,%s)" % (w, x,z['hilucs_value'],json.dumps(y['properties']),z['dataset_id'],z['clc_value']) ),'parameters':['attributes_table_name','attributes_id','feature_json','attributes_properties']},\
     'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,1)" % (z,x,y) ),'parameters':['object_id','attributes_id','attributes2object_table_name']},\
     'object2admunit_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,unit_fid) VALUES (%s,'%s')" % (z,x,y) ),'parameters':['object_id','admunit_id','object2nuts_table_name']},\
     'four_insert_statements':{'function':(lambda w,x,y,z: [w,x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement','object2admunit_insert_statement']}
    }
    return o_dict

def lpis_transformations(object,object_fid,attributes_fid,admunit_id,object_table_name,attributes_table_name,attributes2object_table_name): 
    o_dict=\
    {'feature':{'object':object},\
     'object_id':{'object':object_fid},\
     'attributes_id':{'object':attributes_fid},\
     'admunit_id':{'object':admunit_id},\
     'object_table_name':{'object':object_table_name},\
     'attributes_table_name':{'object':attributes_table_name},\
     'attributes2object_table_name':{'object':attributes2object_table_name},\
     'object_geom':{'function':(lambda y: y.get_geometry()),'parameters':['feature']},\
     'object_properties':{'function':(lambda x,y,z: {**{'fid':x},**{'dataset_id':1},**{'z_value':10000},**{'admunit_id':y},**{'valid_from':z.get_data()['PLATNOSTOD'],'valid_to':'NULL'} }),'parameters':['object_id','admunit_id','feature']},
     'attributes_properties':{'function':(lambda x,y: {**{'fid':x},**{'hilucs_value':lpis_hilucs[y.get_data()['KULTURAID']]},**{'clc_value':lpis_clc[y.get_data()['KULTURAID']]},**{'atts':y.get_data()},**{'dataset_id':1}}),'parameters':['attributes_id','feature']},
     'object_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,dataset_fid,z_value,geom,valid_from,valid_to) VALUES (%s,%s,%s,ST_SetSRID(ST_Multi(ST_GeomFromText('%s')),4326),'%s',%s)" % (w,x,z['dataset_id'],z['z_value'],y,z['valid_from'], z['valid_to'] ) ),'parameters':['object_table_name','object_id','object_geom','object_properties']},\
     'attributes_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,hilucs_id,atts,dataset_fid,clc_id) VALUES (%s,%s,('%s'::json),%s,%s)" % (w, x,z['hilucs_value'],json.dumps(y.get_data()),z['dataset_id'],z['clc_value']) ),'parameters':['attributes_table_name','attributes_id','feature','attributes_properties']},\
     'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,1)" % (z,x,y) ),'parameters':['object_id','attributes_id','attributes2object_table_name']},\
     'object2admunit_insert_statement':{'function':(lambda x,y: "INSERT INTO olu2.olu_object_to_admin_unit (object_fid,unit_fid) VALUES (%s,'%s')" % (x,y) ),'parameters':['object_id','admunit_id']},\
     'three_insert_statements':{'function':(lambda x,y,z: [x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement']},\
     'four_insert_statements':{'function':(lambda w,x,y,z: [w,x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement','object2admunit_insert_statement']},\
    }
    return o_dict

def ruian_transformations(object,object_fid,attributes_fid,admunit_id,object_table_name,attributes_table_name,attributes2object_table_name): 
    o_dict=\
    {'feature':{'object':object},\
     'object_id':{'object':object_fid},\
     'attributes_id':{'object':attributes_fid},\
     'admunit_id':{'object':admunit_id},\
     'object_table_name':{'object':object_table_name},\
     'attributes_table_name':{'object':attributes_table_name},\
     'attributes2object_table_name':{'object':attributes2object_table_name},\
     'object_geom':{'function':(lambda y: y.get_geometry()),'parameters':['feature']},\
     'object_properties':{'function':(lambda x,y,z: {**{'fid':x},**{'dataset_id':2},**{'z_value':100000},**{'admunit_id':y},**{'valid_from':z.get_data()['PlatiOd'],'valid_to':( ('\''+z.get_data()['PlatiDo']+'\'') if z.get_data()['PlatiDo'] is not None else 'NULL')}}),'parameters':['object_id','admunit_id','feature']},
     'attributes_properties':{'function':(lambda x,y: {**{'fid':x},**{'hilucs_value':ruian_zpv_hilucs[str(y.get_data()['ZpusobyVyuzitiPozemku'])] if y.get_data()['ZpusobyVyuzitiPozemku'] is not None else ruian_dp_hilucs[str(y.get_data()['DruhPozemkuKod'])]},**{'clc_value':ruian_zpv_clc[str(y.get_data()['ZpusobyVyuzitiPozemku'])] if y.get_data()['ZpusobyVyuzitiPozemku'] is not None else ruian_dp_clc[str(y.get_data()['DruhPozemkuKod'])]},**{'atts':y.get_data()},**{'dataset_id':2}}),'parameters':['attributes_id','feature']},
     'object_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,dataset_fid,z_value,geom,valid_from,valid_to) VALUES (%s,%s,%s,ST_SetSRID(ST_Multi(ST_GeomFromText('%s')),4326),'%s',%s)" % (w,x,z['dataset_id'],z['z_value'],y,z['valid_from'], z['valid_to'] ) ),'parameters':['object_table_name','object_id','object_geom','object_properties']},\
     'attributes_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,hilucs_id,atts,dataset_fid,clc_id) VALUES (%s,%s,('%s'::json),%s,%s)" % (w,x,z['hilucs_value'],json.dumps(y.get_data()),z['dataset_id'],z['clc_value']) ),'parameters':['attributes_table_name','attributes_id','feature','attributes_properties']},\
     'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,1)" % (z,x,y) ),'parameters':['object_id','attributes_id','attributes2object_table_name']},\
     'object2admunit_insert_statement':{'function':(lambda x,y: "INSERT INTO olu2.olu_object_to_admin_unit (object_fid,unit_fid) VALUES (%s,'%s')" % (x,y) ),'parameters':['object_id','admunit_id']},\
     'three_insert_statements':{'function':(lambda x,y,z: [x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement']},\
     'four_insert_statements':{'function':(lambda w,x,y,z: [w,x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement','object2admunit_insert_statement']},\
    }
    return o_dict

def evi_transformations(ogr_olu_object, attributes_table, attributes2object_table, attributes_fid, evi_im):
    o_dict=\
    {
    'feature':{'object':ogr_olu_object},\
    'feature_id':{'function':(lambda y: int(y.GetFID())),'parameters':['feature']},\
    'feature_geometry':{'function':(lambda y: y.GetGeometryRef().ExportToWkt() ),'parameters':['feature']},\
    'attributes_table':{'object':attributes_table},\
    'attributes2object_table':{'object':attributes2object_table},\
    'attributes_fid':{'object':attributes_fid},\
    'evi_image':{'object':evi_im},\
    'attribute_properties':{'function':(lambda p,q,r: {**{'fid':p},**{'dataset_id':6},**{'atts':{'evi_20200422':Imagee(*r.clip_by_shape(q)).get_statistics(full=True, percentage=True)  } } }),'parameters':['attributes_fid','feature_geometry', 'evi_image']},
    'attributes_insert_statement':{'function':(lambda x, y,z: "INSERT INTO %s (fid,atts,dataset_fid) VALUES (%s,('%s'::json),%s)" % (x,y,json.dumps(z['atts']),z['dataset_id']) ),'parameters':['attributes_table','attributes_fid','attribute_properties']},\
    'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,3)" % (z,x,y) ),'parameters':['feature_id','attributes_fid','attributes2object_table']},\
    'two_insert_statements':{'function':(lambda y,z: [y,z]),'parameters':['attributes_insert_statement','attributes2object_insert_statement']},\
    }
    return o_dict

def s2_evi_class_transformations(ogr_olu_object, attributes_table, attributes2object_table, attributes_fid, s2_evi_class_im):
    o_dict=\
    {
    'feature':{'object':ogr_olu_object},\
    'feature_id':{'function':(lambda y: int(y.GetFID())),'parameters':['feature']},\
    'feature_geometry':{'function':(lambda y: y.GetGeometryRef().ExportToWkt() ),'parameters':['feature']},\
    'attributes_table':{'object':attributes_table},\
    'attributes2object_table':{'object':attributes2object_table},\
    'attributes_fid':{'object':attributes_fid},\
    'image':{'object':s2_evi_class_im},\
    'attribute_properties':{'function':(lambda p,q,r: {**{'fid':p},**{'dataset_id':8},**{'atts':{'s2_unsupervised_classification_2020':Imagee(*r.clip_by_shape(q)).get_statistics(True, True, True)  } } }),'parameters':['attributes_fid','feature_geometry', 'image']},
    'attributes_insert_statement':{'function':(lambda x, y,z: "INSERT INTO %s (fid,atts,dataset_fid) VALUES (%s,('%s'::json),%s)" % (x,y,json.dumps(z['atts']),z['dataset_id']) ),'parameters':['attributes_table','attributes_fid','attribute_properties']},\
    'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,3)" % (z,x,y) ),'parameters':['feature_id','attributes_fid','attributes2object_table']},\
    'two_insert_statements':{'function':(lambda y,z: [y,z]),'parameters':['attributes_insert_statement','attributes2object_insert_statement']},\
    }
    return o_dict

def s12_evi_class_transformations(ogr_olu_object, attributes_table, attributes2object_table, attributes_fid, s12_evi_class_im):
    o_dict=\
    {
    'feature':{'object':ogr_olu_object},\
    'feature_id':{'function':(lambda y: int(y.GetFID())),'parameters':['feature']},\
    'feature_geometry':{'function':(lambda y: y.GetGeometryRef().ExportToWkt() ),'parameters':['feature']},\
    'attributes_table':{'object':attributes_table},\
    'attributes2object_table':{'object':attributes2object_table},\
    'attributes_fid':{'object':attributes_fid},\
    'image':{'object':s12_evi_class_im},\
    'attribute_properties':{'function':(lambda p,q,r: {**{'fid':p},**{'dataset_id':9},**{'atts':{'s1+s2_unsupervised_classification_2020':Imagee(*r.clip_by_shape(q)).get_statistics(True, True, True)  } } }),'parameters':['attributes_fid','feature_geometry', 'image']},
    'attributes_insert_statement':{'function':(lambda x, y,z: "INSERT INTO %s (fid,atts,dataset_fid) VALUES (%s,('%s'::json),%s)" % (x,y,json.dumps(z['atts']),z['dataset_id']) ),'parameters':['attributes_table','attributes_fid','attribute_properties']},\
    'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,3)" % (z,x,y) ),'parameters':['feature_id','attributes_fid','attributes2object_table']},\
    'two_insert_statements':{'function':(lambda y,z: [y,z]),'parameters':['attributes_insert_statement','attributes2object_insert_statement']},\
    }
    return o_dict

def raster_transformations(ogr_olu_object, attributes_table, attributes2object_table, attributes_fid, supergeoconcept):
    o_dict=\
    {
    'feature':{'object':ogr_olu_object},\
    'feature_id':{'function':(lambda y: int(y.GetFID())),'parameters':['feature']},\
    'feature_geometry':{'function':(lambda y: y.GetGeometryRef().ExportToWkt() ),'parameters':['feature']},\
    'attributes_table':{'object':attributes_table},\
    'attributes2object_table':{'object':attributes2object_table},\
    'attributes_fid':{'object':attributes_fid},\
    'supergeoconcept':{'object':supergeoconcept},\
    'attribute_properties':{'function':(lambda p,q,r: {**{'fid':p},**{'dataset_id':10},**{'atts':accumulate_attributes_by_supergeoconcept(r, q) } }),'parameters':['attributes_fid','feature_geometry', 'supergeoconcept']},
    'attributes_insert_statement':{'function':(lambda x, y,z: "INSERT INTO %s (fid,atts,dataset_fid) VALUES (%s,('%s'::json),%s)" % (x,y,json.dumps(z['atts']),z['dataset_id']) ),'parameters':['attributes_table','attributes_fid','attribute_properties']},\
    'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,3)" % (z,x,y) ),'parameters':['feature_id','attributes_fid','attributes2object_table']},\
    'two_insert_statements':{'function':(lambda y,z: [y,z]),'parameters':['attributes_insert_statement','attributes2object_insert_statement']},\
    }
    return o_dict

def geomorphology_transformations(ogr_olu_object, attributes_table, attributes2object_table, attributes_fid,  dem_im,  slope_im,  aspect_im,  twi_im):
    o_dict=\
    {
    'feature':{'object':ogr_olu_object},\
    'feature_id':{'function':(lambda y: int(y.GetFID())),'parameters':['feature']},\
    'feature_geometry':{'function':(lambda y: y.GetGeometryRef().ExportToWkt() ),'parameters':['feature']},\
    'attributes_table':{'object':attributes_table},\
    'attributes2object_table':{'object':attributes2object_table},\
    'attributes_fid':{'object':attributes_fid},\
    'dem_image':{'object':dem_im},\
    'slope_image':{'object':slope_im},\
    'azimuth_image':{'object':aspect_im},\
    'twi_image':{'object':twi_im},\
    'attribute_properties':{'function':(lambda u, v, w, x,y,z: {**{'fid':u},**{'dataset_id':11},**{'atts':{'elevation':Imagee(*w.clip_by_shape(v)).get_statistics(False,True,False), 'slope':Imagee(*x.clip_by_shape(v)).get_statistics(False,True,False), 'azimuth':Imagee(*y.clip_by_shape(v)).get_statistics(True,True,True), 'twi':Imagee(*z.clip_by_shape(v)).get_statistics(False,True,False)} } }),'parameters':['attributes_fid','feature_geometry','dem_image', 'slope_image', 'azimuth_image', 'twi_image']},
    'attributes_insert_statement':{'function':(lambda x, y,z: "INSERT INTO %s (fid,atts,dataset_fid) VALUES (%s,('%s'::json),%s)" % (x,y,json.dumps(z['atts']),z['dataset_id']) ),'parameters':['attributes_table','attributes_fid','attribute_properties']},\
    'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,3)" % (z,x,y) ),'parameters':['feature_id','attributes_fid','attributes2object_table']},\
    'two_insert_statements':{'function':(lambda y,z: [y,z]),'parameters':['attributes_insert_statement','attributes2object_insert_statement']},\
    }
    return o_dict

ua_conn=psycopg2.connect("dbname=urban_atlas host=... user=... password=... port=5432")
corine_conn=psycopg2.connect("dbname=corine_land_cover host=... user=... password=... port=5432")
olu_conn=psycopg2.connect("dbname=olu2_vyskov host=... user=... password=... port=5432")


conn_corine=ogr.Open("PG: host=... dbname=corine_land_cover user=... password=...")
conn_ua=ogr.Open("PG: host=... dbname=urban_atlas user=... password=...")


ua_cur=ua_conn.cursor()
corine_cur=corine_conn.cursor()
olu_cur=olu_conn.cursor()
olu_cur2=olu_conn.cursor()


olu_cur.execute('SELECT max(fid) FROM olu2.olu_object')
max_value=olu_cur.fetchone()[0]
max_value=max_value if max_value is not None else 1
olu_cur.execute("select setval('olu2.olu_object_fid_seq', %s)" % max_value)
olu_conn.commit()

olu_cur.execute('SELECT max(fid) FROM olu2.olu_attribute_set')
max_value=olu_cur.fetchone()[0]
max_value=max_value if max_value is not None else 1
olu_cur.execute("select setval('olu2.olu_attributes_fid_seq', %s)" % max_value)
olu_conn.commit()


olu_cur.execute('SELECT last_value FROM olu2.olu_object_fid_seq')
olu_id_gen=sequence_generator(olu_cur.fetchone()[0])


olu_cur.execute('SELECT last_value FROM olu2.olu_attributes_fid_seq')
olu_atts_id_gen=sequence_generator(olu_cur.fetchone()[0])


olu_cur2.execute("select st_asbinary(st_union(geom)), 'CZ064' as fid from olu2.administrative_unit where fid in (%s)" % str(list(find_neighbors_level(G.reverse(),'3712',3)))[1:-1] ) 


while True:
    geom, nuts_id= olu_cur2.fetchone()
    
    olu_object_table='olu2.olu_object'
    olu_attribute_set_table='olu2.olu_attribute_set'
    olu_atts_to_object_table='olu2.atts_to_object'
    olu_object_to_admin_unit_table='olu2.olu_object_to_admin_unit'
    
    corine_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'data' and table_name = '%s'" % nuts_id[:2].lower())
    
    if corine_cur.fetchone() is not None:
        corine_layer=conn_corine.ExecuteSQL("select * from data.%s where st_intersects(geom,st_geomfromtext('%s',4326)) and year='2018'" % (nuts_id[:2].lower(),ogr.CreateGeometryFromWkb(geom).ExportToWkt() ) )
        parts=corine_layer.GetFeatureCount()
        number=100
        i=1
        count=0
        j=math.ceil(parts/number)
        while i<j:
            for k in range(number):
                olu_id=next(olu_id_gen)
                atts_id=next(olu_atts_id_gen)
                f=corine_layer.GetNextFeature()
                #[print(i) for i in apply_function(corine_transformations(f,olu_id,atts_id,nuts_id,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table,olu_object_to_admin_unit_table),'four_insert_statements')]
                [olu_cur.execute(i) for i in apply_function(corine_transformations(f,olu_id,atts_id,nuts_id,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table,olu_object_to_admin_unit_table),'four_insert_statements')]
                count+=1
            olu_conn.commit()
            i+=1
        
        for k in range(parts-count):
            olu_id=next(olu_id_gen)
            atts_id=next(olu_atts_id_gen)
            f=corine_layer.GetNextFeature()
            [olu_cur.execute(i) for i in apply_function(corine_transformations(f,olu_id,atts_id,nuts_id,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table,olu_object_to_admin_unit_table),'four_insert_statements')]

        olu_conn.commit()
        del(corine_layer)
    ua_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'data' and table_name = '%s'" % nuts_id[:2].lower())
    if ua_cur.fetchone() is not None:
        ua_layer=conn_ua.ExecuteSQL("select * from data.%s where st_intersects(geom,st_geomfromtext('%s',4326)) and code2012 not in ('12210','12220','12230')" % (nuts_id[:2].lower(),ogr.CreateGeometryFromWkb(geom).ExportToWkt() ) )
        parts=ua_layer.GetFeatureCount()
        number=100
        i=1
        count=0
        j=math.ceil(parts/number)
        while i<j:
            for k in range(number):
                olu_id=next(olu_id_gen)
                atts_id=next(olu_atts_id_gen)
                f=ua_layer.GetNextFeature()
                #[print(i) for i in apply_function(corine_transformations(f,olu_id,atts_id,nuts_id,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table,olu_object_to_admin_unit_table),'four_insert_statements')]
                [olu_cur.execute(i) for i in apply_function(ua_transformations(f,olu_id,atts_id,nuts_id,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table,olu_object_to_admin_unit_table),'four_insert_statements')]
                count+=1
            olu_conn.commit()
            i+=1
        
        for k in range(parts-count):
            olu_id=next(olu_id_gen)
            atts_id=next(olu_atts_id_gen)
            f=ua_layer.GetNextFeature()
            [olu_cur.execute(i) for i in apply_function(ua_transformations(f,olu_id,atts_id,nuts_id,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table,olu_object_to_admin_unit_table),'four_insert_statements')]

        olu_conn.commit()
        del(ua_layer)


olu_conn.commit()


dbs_admin_connection={**{'dbname':'olu_administrative_units'},**connectionstring_localhost}
dbs_admin=DBStorage(dbs_admin_connection)
dbs_admin.connect()
dbs_admin.disconnect()
dbs_admin.connect()

dbs_ruian_cz_connection={**{'dbname':'ruian_cz'},**connectionstring_localhost}
dbs_ruian_cz=DBStorage(dbs_ruian_cz_connection)
dbs_ruian_cz.connect()
dbs_ruian_cz.disconnect()
dbs_ruian_cz.connect()

dbs_lpis_cz_connection={**{'dbname':'lpis_cz'},**connectionstring_localhost}
dbs_lpis_cz=DBStorage(dbs_lpis_cz_connection)
dbs_lpis_cz.connect()
dbs_lpis_cz.disconnect()
dbs_lpis_cz.connect()

dbs_olu_cz_connection={**{'dbname':'olu2_vyskov'},**connectionstring_localhost}
dbs_olu_cz=DBStorage(dbs_olu_cz_connection)
dbs_olu_cz.connect()
dbs_olu_cz.disconnect()
dbs_olu_cz.connect()


admunit_cz__metadata=MetaData('Administrative units in Czech Republic',
                              {"url":"https://vdp.cuzk.cz/vymenny_format/soucasna/[posledni_den_mesice]_ST_UKSG.xml.zip",
                               "format":"GML", "compression":"zip"},'data')

ruian_cz__metadata=MetaData('RUIAN in Czech Republic',
                              {"url":"https://vdp.cuzk.cz/vymenny_format/soucasna/[vcera]_OB_{admunit__tree__level3}_UKSH.xml.zip",
                               "format":"GML", "compression":"zip"},'data')
                            
lpis_cz__metadata=MetaData('LPIS in Czech Republic',
                              [{"url":"http://eagri.cz/public/app/eagriapp/lpisdata/[lpis_cz__posledni_aktualizace]-{admunit__tree__level4}-DPB-SHP.zip",
                               "format":"SHP", "compression":"zip"},{"url":"http://eagri.cz/public/app/eagriapp/lpisdata/[lpis_cz__posledni_aktualizace]-{admunit__tree__level4}-DPB-XML-A.zip",
                               "format":"XML", "compression":"zip"}],'data')


admunit_cz__ds=ds_from_metadata(admunit_cz__metadata)

ruian_cz__ds=ds_from_metadata(ruian_cz__metadata)
ruian_cz__ds.set_attributes({**ruian_cz__ds.get_attributes(),**{'layer':'Parcely'}})

lpis_cz__ds_xml=ds_from_metadata(lpis_cz__metadata,format='XML')


admunit_cz=GeoConcept('Administrative units in Czech Republic','Administrative units in Czech Republic. All levels.',
                      'AdmUnitFeature',json_admin_unit_structure, data_source=admunit_cz__ds, subgeoconcepts=[] )

ruian_parcely_cz=GeoConcept('Land parcels in Czech Republic','Digital land parcels (parcely) in Czech Republic.',
                      'FeatureWithID',json_feature_with_bigid_structure, data_source=ruian_cz__ds,subgeoconcepts=[],adm_graph_node='1')

lpis_cz=GeoConcept('LPIS in Czech Republic','LPIS in Czech Republic. All levels.',
                      'FeatureWithID',json_feature_structure, data_source=lpis_cz__ds_xml, subgeoconcepts=[], adm_graph_node='1')


if 'local' in ruian_parcely_cz.get_data_source().get_attributes():
    ruian_parcely_cz.get_data_source().set_data_file(ruian_parcely_cz.get_data_source().get_attributes()['local']) 


url_adresa=admunit_cz.get_data_source().get_attributes()['url']
for i in re.findall('\[.*?\]',url_adresa):
    if i in list(replacement_dictionary.keys()):
        url_adresa=url_adresa.replace(i,replacement_dictionary[i])        
admunit_cz.get_data_source().set_attribute({'url':url_adresa})
del(url_adresa)

url_adresa=lpis_cz.get_data_source().get_attributes()['url']
for i in re.findall('\[.*?\]',url_adresa):
    if i in list(replacement_dictionary.keys()):
        url_adresa=url_adresa.replace(i,replacement_dictionary[i])
lpis_cz.get_data_source().set_attribute({'url':url_adresa})
del(url_adresa)

url_adresa=ruian_parcely_cz.get_data_source().get_attributes()['url']
for i in re.findall('\[.*?\]',url_adresa):
    if i in list(replacement_dictionary.keys()):
        url_adresa=url_adresa.replace(i,replacement_dictionary[i])
ruian_parcely_cz.get_data_source().set_attribute({'url':url_adresa})
del(url_adresa)


admunit_cz.create_table(dbs_admin, name='default',scheme='cz',conflict='append')

ruian_parcely_cz.create_table(dbs_ruian_cz, name='default',scheme='public',conflict='append',adm_graph_node='1')

lpis_cz.create_table(dbs_lpis_cz,name='default',scheme='public',conflict='append', adm_graph_node='1')


admunit_cz__ds.set_data_file('20201031_ST_UKSG.xml')


concept_list=['Staty','Vusc','Okresy','Obce','KatastralniUzemi']
concept_additional_attributes={'Staty':{'level_value':0,'parent_value':'null','id_attribute':'Kod'},
                               'Vusc':{'level_value':1,'parent_value':'1','id_attribute':'Kod'},
                               'Okresy':{'level_value':2,'parent_attribute':'VuscKod','id_attribute':'Kod'},
                               'Obce':{'level_value':3,'parent_attribute':'OkresKod','id_attribute':'Kod'},
                               'KatastralniUzemi':{'level_value':4,'parent_attribute':'ObecKod','id_attribute':'Kod'}}
                               
for l in list(set(concept_list).intersection(set(admunit_cz.get_data_source().list_layers()))):
    admunit_cz.append_subgeoconcept(SubGeoConcept(l,l,'AdmUnitFeature',admunit_cz.get_attributes(),data_source=DataSource(admunit_cz.get_data_source().get_type(),admunit_cz.get_data_source().get_name(),({**admunit_cz.get_data_source().get_attributes(),**{'layer':l}}),None,admunit_cz.get_data_source().get_data_file()),supergeoconcept=admunit_cz,table_inheritance=False,type='semantic',subgeoconcepts=[]))


concept_list=['Parcely']
concept_additional_attributes={'Parcely':{'id_attribute':'Id'}}

for i in re.findall('\{.*?\}',ruian_parcely_cz.get_data_source().get_attributes()['url']): 
    if i[1:-1] in list(compilable_tree_dictionary(admunit_cz).keys()):
        for j in apply_function(compilable_tree_dictionary(admunit_cz),i[1:-1]):
            ruian_parcely_cz.append_subgeoconcept(SubGeoConcept(str(j),'RUIAN land parcels in Czech administrative territorial unit %s ' % str(j),'FeatureWithID',ruian_parcely_cz.get_attributes(),data_source=DataSource(ruian_parcely_cz.get_data_source().get_type(),ruian_parcely_cz.get_data_source().get_name(),(dict(ruian_parcely_cz.get_data_source().get_attributes(),**{'url':ruian_parcely_cz.get_data_source().get_attributes()['url'].replace(i,str(j))})),None,None),adm_graph_node=str(j),supergeoconcept=ruian_parcely_cz,table_inheritance=True,subgeoconcepts=[]))



for i in re.findall('\{.*?\}',lpis_cz.get_data_source().get_attributes()['url']): 
    if i[1:-1] in list(compilable_tree_dictionary(admunit_cz).keys()):
        for j in apply_function(compilable_tree_dictionary(admunit_cz),i[1:-1]):
            lpis_cz.append_subgeoconcept(SubGeoConcept(str(j),'LPIS in Czech administrative territorial unit %s ' % str(j),'FeatureWithID',lpis_cz.get_attributes(),data_source=DataSource(lpis_cz.get_data_source().get_type(),lpis_cz.get_data_source().get_name(),(dict(lpis_cz.get_data_source().get_attributes(),**{'url':lpis_cz.get_data_source().get_attributes()['url'].replace(i,str(j))})),None,None),supergeoconcept=lpis_cz,table_inheritance=True,subgeoconcepts=[],type='spatial:admin',adm_graph_node=str(j)))


G=apply_function(compilable_node_dictionary(admunit_cz),'admunit__tree')


for s in lpis_cz.get_subgeoconcepts():
    municipality=[i for i in G.neighbors(str(s.get_adm_graph_node()))][0]
    s.create_table(dbs_lpis_cz,name='ob_'+str(municipality),scheme='data',conflict='append',adm_graph_node=municipality)
    s.set_table(View('ku_'+s.get_name(), s.get_attributes(), s.get_table(), "dictionary_select5((data->'PREKRYVKATUZE')::json, 'KUKOD == \"%s\" AND PLATNOSTDO>\"[aktualni_cas]\"')::jsonb not in ('[]'::jsonb,'{}'::jsonb)" % s.get_name(), dbs=dbs_lpis_cz, scheme='data',type='usual'))
    dbs_lpis_cz.execute(s.get_table().create_script())


for sub in ruian_parcely_cz.get_subgeoconcepts():
    sub.create_table(dbs_ruian_cz,name='ob_'+sub.get_name(),scheme='data',conflict='append',adm_graph_node=sub.get_adm_graph_node())


olu_object_table='olu2.olu_object'
olu_attribute_set_table='olu2.olu_attribute_set'
olu_atts_to_object_table='olu2.atts_to_object'
for ku in find_neighbors_level(G.reverse(),'3712',4):
    admunit_id=list(find_neighbors_level(G,ku,3))[0]
    try:
        sub=lpis_cz.get_subgeoconcept_by_adm_node(ku)
        print(sub.get_name())
        lpis_feature_gen=sub.read_features_from_table(100)
        for feature_batch in lpis_feature_gen:
            if len(feature_batch)>0:
                print(len(feature_batch))
                for feature in feature_batch:
                    feature.transform_geometry(sjtsk5514_to_wgs84)
                    olu_id=next(olu_id_gen)
                    atts_id=next(olu_atts_id_gen)
                    #print([i for i in apply_function(lpis_transformations(feature,olu_id,atts_id,kod_obce,'olu2.olu_object','olu2.olu_attribute_set'),'three_insert_statements')])
                    [olu_cur.execute(i) for i in apply_function(lpis_transformations(feature,olu_id,atts_id,admunit_id,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table),'four_insert_statements')]
            else:
                break
        olu_conn.commit()
    except:
        olu_conn.rollback()
        continue


olu_conn.commit()


olu_object_table='olu2.olu_object'
olu_attribute_set_table='olu2.olu_attribute_set'
olu_atts_to_object_table='olu2.atts_to_object'
for obec in find_neighbors_level(G.reverse(),'3712',3):
    admunit_id=obec
    try:
        sub=ruian_parcely_cz.get_subgeoconcept_by_adm_node(obec)
        print(sub.get_name())
        ruian_feature_gen=sub.read_features_from_table(1000)
        for feature_batch in ruian_feature_gen:
            if len(feature_batch)>0:
                print(len(feature_batch))
                for feature in feature_batch:
                    feature.transform_geometry(sjtsk5514_to_wgs84)
                    olu_id=next(olu_id_gen)
                    atts_id=next(olu_atts_id_gen)
                    #print([i for i in apply_function(lpis_transformations(feature,olu_id,atts_id,kod_obce,'olu2.olu_object','olu2.olu_attribute_set'),'three_insert_statements')])
                    [olu_cur.execute(i) for i in apply_function(ruian_transformations(feature,olu_id,atts_id,admunit_id,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table),'four_insert_statements')]
            else:
                break
        olu_conn.commit()
    except:
        olu_conn.rollback()
        print("obec %s nedobehla" % obec)
        continue


olu_conn.commit()


rostenice__metadata=MetaData('Rostenice/pozemky',
                              {"local":'/data/rostenice_fields/rostenice_2020.gpkg',
                               "format":"GPKG"},'vector',example_feature=feature,olu_id=5)
rostenice__ds=ds_from_metadata(rostenice__metadata)


if 'local' in list(rostenice__ds.get_attributes().keys()):
    rostenice__ds.set_data_file(rostenice__ds.get_attributes()['local'])


feature_gen=rostenice__ds.read_features('feature',number=1)


feature=next(feature_gen)[0]


feature.get_data()


metadata_table=Table('origin_metadata',
[{'name': 'fid', 'type': 'integer primary key'},
 {'name': 'metadata', 'type': 'text'},
 {'name': 'origin_name', 'type': 'text'},
 {'name': 'valid_from', 'type': 'date'},
 {'name': 'valid_to', 'type': 'date'},
 {'name': 'origin_type', 'type': 'text'},
 {'name': 'column_names', 'type': 'json'}],
dbs_olu_cz, 'olu2')


ds_table=Table('origin_dataset',
[{'name': 'fid', 'type': 'integer primary key'},
 {'name': 'metadata_fid', 'type': 'integer'},
 {'name': 'uri', 'type': 'text'},
 {'name': 'dataset_type', 'type': 'integer'}],
dbs_olu_cz, 'olu2')


rostenice__metadata.set_metatable(metadata_table)


rostenice__metadata.get_metatable().get_self_sequence_name('fid')


rostenice__metadata.set_validitydates([datetime.datetime.now().strftime('%Y-%m-%d'),None])


metadata_fid_sequence=sequence_generator(dbs_olu_cz.get_last_value_of_seq(rostenice__metadata.get_metatable().get_self_sequence_name('fid')))


#dbs_olu_cz.execute(rostenice__metadata.convert_to_sql_insert())


next(metadata_fid_sequence)


rostenice__ds.set_metatable(ds_table)


ds_fid_sequence=sequence_generator(dbs_olu_cz.get_last_value_of_seq(rostenice__ds.get_metatable().get_self_sequence_name('fid'),set_last_value=True,data_table=rostenice__ds.get_metatable().get_scheme()+'.'+rostenice__ds.get_metatable().get_name(),id_attribute='fid'))


rostenice__ds.set_olu_id(next(ds_fid_sequence))


rostenice__ds.get_olu_id()


rostenice__ds.set_type(1)


dbs_olu_cz.execute(rostenice__ds.convert_to_sql_insert())


rostenice__ds.set_type('GPKG')


rostenice=GeoConcept('Rostenice','Rostenice fields.', 'FeatureWithID',json_feature_structure, data_source=rostenice__ds)


dbs_olu_cz.execute('create schema temp')


rostenice.create_table(dbs_olu_cz,name='default',scheme='temp',conflict='append')


feature_gen=rostenice.get_data_source().read_features('feature',number=10)


dbs_olu_cz.insert_many('insert into %s (geom,data) ' % (rostenice.get_table().get_scheme()+'.'+rostenice.get_table().get_name()) ,feature_gen,20)





dbs_olu_cz.disconnect()
dbs_olu_cz.connect()


def rostenice_transformations(object,object_fid,attributes_fid,admunit_id,object_table_name,attributes_table_name,attributes2object_table_name): 
    o_dict=\
    {'feature':{'object':object},\
     'object_id':{'object':object_fid},\
     'attributes_id':{'object':attributes_fid},\
     'admunit_id':{'object':admunit_id},\
     'object_table_name':{'object':object_table_name},\
     'attributes_table_name':{'object':attributes_table_name},\
     'attributes2object_table_name':{'object':attributes2object_table_name},\
     'object_geom':{'function':(lambda y: y.get_geometry()),'parameters':['feature']},\
     'object_properties':{'function':(lambda x,y: {**{'fid':x},**{'dataset_id':5},**{'z_value':15000},**{'admunit_id':y},**{'valid_from':'NULL','valid_to':'NULL'} }),'parameters':['object_id','admunit_id']},
     'attributes_properties':{'function':(lambda x,y: {**{'fid':x},**{'hilucs_value':111},**{'clc_value':200},**{'atts':y.get_data()},**{'dataset_id':5}}),'parameters':['attributes_id','feature']},
     'object_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,dataset_fid,z_value,geom,valid_from,valid_to) VALUES (%s,%s,%s,ST_SetSRID(ST_Multi(ST_GeomFromText('%s')),4326),%s,%s)" % (w,x,z['dataset_id'],z['z_value'],y,z['valid_from'], z['valid_to'] ) ),'parameters':['object_table_name','object_id','object_geom','object_properties']},\
     'attributes_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,hilucs_id,atts,dataset_fid,clc_id) VALUES (%s,%s,('%s'::json),%s,%s)" % (w, x,z['hilucs_value'],json.dumps(y.get_data()),z['dataset_id'],z['clc_value']) ),'parameters':['attributes_table_name','attributes_id','feature','attributes_properties']},\
     'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,1)" % (z,x,y) ),'parameters':['object_id','attributes_id','attributes2object_table_name']},\
     'object2admunit_insert_statement':{'function':(lambda x,y: "INSERT INTO olu2.olu_object_to_admin_unit (object_fid,unit_fid) VALUES (%s,'%s')" % (x,y) ),'parameters':['object_id','admunit_id']},\
     'three_insert_statements':{'function':(lambda x,y,z: [x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement']},\
     'four_insert_statements':{'function':(lambda w,x,y,z: [w,x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement','object2admunit_insert_statement']},\
    }
    return o_dict


olu_id_gen=sequence_generator(dbs_olu_cz.get_last_value_of_seq('olu2.olu_object_fid_seq',set_last_value=True,data_table='olu2.olu_object',id_attribute='fid'))


olu_atts_id_gen=sequence_generator(dbs_olu_cz.get_last_value_of_seq('olu2.olu_attributes_fid_seq',set_last_value=True,data_table='olu2.olu_attribute_set',id_attribute='fid'))


olu_object_table='olu2.olu_object'
olu_attribute_set_table='olu2.olu_attribute_set'
olu_atts_to_object_table='olu2.atts_to_object'
admunit_id='CZ064'
rostenice_feature_gen=rostenice.read_features_from_table(1000)
for feature_batch in rostenice_feature_gen:
    if len(feature_batch)>0:
        print(len(feature_batch))
        for feature in feature_batch:
            feature.transform_geometry(utm32633_to_wgs84)
            olu_id=next(olu_id_gen)
            atts_id=next(olu_atts_id_gen)
            [olu_cur.execute(i) for i in apply_function(rostenice_transformations(feature,olu_id,atts_id,admunit_id,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table),'four_insert_statements')]
    else:
        break


olu_conn.commit()








id_attribute,data_table='fid',rostenice__metadata.get_table().get_scheme()+'.'+rostenice__metadata.get_table().get_name()
dbs_olu_cz.execute('select max(%s) from %s' % (id_attribute, data_table) )[0][0]


#dbs_olu_cz.get_last_value_of_seq('olu2.origin_metadata_metadata_id_seq',True,'olu2.origin_metadata','fid')


rostenice__metadata


[f for f in os.listdir('/dpz/produkty/S2/2020') if '20200420' in f]


evi_vyskov_20200422__metadata=MetaData('EVI index in Vyskov lokality at 2020-04-22', {"local":"/dpz/produkty/S2/2020/S2B_MSIL2A_20200420T100019_N9999_R122_T33UXQ_20201127T124250_EVI.tif", "format":"GTiff"},'raster data')
evi_vyskov_20200422__ds=ds_from_metadata(evi_vyskov_20200422__metadata)
evi_vyskov_20200422=GeoConcept('EVI index in Vyskov lokality at 2020-04-22','EVI index in Vyskov lokality at 2020-04-22. ', 'Feature',json_feature_with_raster_structure, data_source=evi_vyskov_20200422__ds,subgeoconcepts=[])
evi_vyskov_20200422.set_raster_output_backend('',evi_vyskov_20200422.get_data_source().get_attributes()['local'])


evi_vyskov_20200422__metadata.set_metatable(metadata_table)


evi_vyskov_20200422__metadata.set_validitydates([datetime.datetime(2020,4,22).strftime('%Y-%m-%d'),None])


metadata_fid_sequence=sequence_generator(dbs_olu_cz.get_last_value_of_seq(evi_vyskov_20200422__metadata.get_metatable().get_self_sequence_name('fid'),set_last_value=True,data_table=evi_vyskov_20200422__metadata.get_metatable().get_scheme()+'.'+evi_vyskov_20200422__metadata.get_metatable().get_name(),id_attribute='fid'))


evi_vyskov_20200422__metadata.set_olu_id(next(metadata_fid_sequence))


dbs_olu_cz.execute(evi_vyskov_20200422__metadata.convert_to_sql_insert())


evi_vyskov_20200422__ds.set_metatable(ds_table)


ds_fid_sequence=sequence_generator(dbs_olu_cz.get_last_value_of_seq(evi_vyskov_20200422__ds.get_metatable().get_self_sequence_name('fid'),set_last_value=True,data_table=evi_vyskov_20200422__ds.get_metatable().get_scheme()+'.'+evi_vyskov_20200422__ds.get_metatable().get_name(),id_attribute='fid'))


evi_vyskov_20200422__ds.set_olu_id(next(ds_fid_sequence))


evi_vyskov_20200422__ds.set_type(2)


dbs_olu_cz.execute(evi_vyskov_20200422__ds.convert_to_sql_insert())


evi_vyskov_20200422__ds.set_type('GTiff')





conn_olu=ogr.Open("PG: host=... dbname=olu2_vyskov user=... password=...")


lyr=conn_olu.GetLayerByName('olu2.olu_object')


lyr.GetFeatureCount()


lyr_f=lyr.GetNextFeature()


lyr_f.GetGeometryRef().IsEmpty()


lyr_f.GetGeometryRef().Transform(wgs84_to_utm32633)


evi_im=evi_vyskov_20200422.read_raster_output_backend(1)


lyr_f_im=Imagee(*evi_im.clip_by_shape(lyr_f.GetGeometryRef().ExportToWkt()))


lyr_f_im.get_statistics(full=True,percentage=True)


evi_im.image_to_geo_coordinates(0,0)


evi_im.image_to_geo_coordinates(*evi_im.get_data().shape)


evi_im_geom=ogr.CreateGeometryFromWkt('POLYGON((600005 5390215,600005 5500015,709805 5500015,709805 5390215,600005 5390215))')


evi_im_geom.Transform(utm32633_to_wgs84)


lyr.ResetReading()


lyr.SetSpatialFilter(evi_im_geom)


lyr.GetFeatureCount()


olu_atts_id_gen=sequence_generator(dbs_olu_cz.get_last_value_of_seq('olu2.olu_attributes_fid_seq',set_last_value=True,data_table='olu2.olu_attribute_set',id_attribute='fid'))


bad_list=[]
count=0
for c in range(lyr.GetFeatureCount()):
    lyr_f=lyr.GetNextFeature()
    if lyr_f.GetGeometryRef().IsEmpty():
        continue
    lyr_f.GetGeometryRef().Transform(wgs84_to_utm32633)
    attributes_fid=next(olu_atts_id_gen)
    attributes_table='olu2.olu_attribute_set'
    attributes2object_table='olu2.atts_to_object'
    lyr_f_im=Imagee(*evi_im.clip_by_shape(lyr_f.GetGeometryRef().ExportToWkt()))
    [olu_cur.execute(i) for i in apply_function(evi_transformations(lyr_f,attributes_table,attributes2object_table,attributes_fid,evi_im),'two_insert_statements')]
    count+=1
    if count%1000==0:
        olu_conn.commit()
        count=0


count


olu_conn.commit()


lyr_f.GetFID()


s2_evi_classification__metadata=MetaData('Sentinel-2 based unsupervised classification 2020', {"local":"/data/classification/unsupervised_rostenice/40cl_km_rd_md_evi20.tif", "format":"GTiff"},'raster')
s2_evi_classification__ds=ds_from_metadata(s2_evi_classification__metadata)
s2_evi_classification=GeoConcept('Sentinel-2 based unsupervised classification 2020','Sentinel-2 based unsupervised classification 2020. ', 'Feature',json_feature_with_raster_structure, data_source=s2_evi_classification__ds,subgeoconcepts=[])
s2_evi_classification.set_raster_output_backend('',s2_evi_classification.get_data_source().get_attributes()['local'])


s2_evi_classification__metadata.set_metatable(metadata_table)
s2_evi_classification__metadata.set_validitydates([datetime.datetime(2020,8,31).strftime('%Y-%m-%d'),None])
metadata_fid_sequence=sequence_generator(dbs_olu_cz.get_last_value_of_seq(s2_evi_classification__metadata.get_metatable().get_self_sequence_name('fid'),set_last_value=True,data_table=s2_evi_classification__metadata.get_metatable().get_scheme()+'.'+s2_evi_classification__metadata.get_metatable().get_name(),id_attribute='fid'))
s2_evi_classification__metadata.set_olu_id(next(metadata_fid_sequence))
dbs_olu_cz.execute(s2_evi_classification__metadata.convert_to_sql_insert())


s2_evi_classification__ds.set_metatable(ds_table)
ds_fid_sequence=sequence_generator(dbs_olu_cz.get_last_value_of_seq(s2_evi_classification__ds.get_metatable().get_self_sequence_name('fid'),set_last_value=True,data_table=s2_evi_classification__ds.get_metatable().get_scheme()+'.'+s2_evi_classification__ds.get_metatable().get_name(),id_attribute='fid'))
s2_evi_classification__ds.set_olu_id(next(ds_fid_sequence))
s2_evi_classification__ds.set_type(2)
dbs_olu_cz.execute(s2_evi_classification__ds.convert_to_sql_insert())
s2_evi_classification__ds.set_type('GTiff')


s2_evi_classification__im=s2_evi_classification.read_raster_output_backend(1)


s2_evi_classification__im.image_to_geo_coordinates(0,0)


s2_evi_classification__im.image_to_geo_coordinates(*s2_evi_classification__im.get_data().shape)


s2_evi_classification__im_geom=ogr.CreateGeometryFromWkt('POLYGON((600005 5390215,600005 5500015,709805 5500015,709805 5390215,600005 5390215))')


s2_evi_classification__im_geom.Transform(utm32633_to_wgs84)


lyr.SetSpatialFilter(s2_evi_classification__im_geom)


lyr.ResetReading()


lyr.GetFeatureCount()


olu_atts_id_gen=sequence_generator(dbs_olu_cz.get_last_value_of_seq('olu2.olu_attributes_fid_seq',set_last_value=True,data_table='olu2.olu_attribute_set',id_attribute='fid'))


bad_list=[]
count=0
for c in range(lyr.GetFeatureCount()):
    lyr_f=lyr.GetNextFeature()
    if lyr_f.GetGeometryRef().IsEmpty():
        continue
    lyr_f.GetGeometryRef().Transform(wgs84_to_utm32633)
    attributes_fid=next(olu_atts_id_gen)
    attributes_table='olu2.olu_attribute_set'
    attributes2object_table='olu2.atts_to_object'
    try:
        [olu_cur.execute(i) for i in apply_function(s2_evi_class_transformations(lyr_f,attributes_table,attributes2object_table,attributes_fid,s2_evi_classification__im),'two_insert_statements')]
    except:
        bad_list.append(lyr_f.GetFID())
        continue
    count+=1
    if count%1000==0:
        olu_conn.commit()
        count=0


olu_conn.commit()


len(bad_list)


s12_evi_classification__metadata=MetaData('Sentinel-1 and Sentinel-2 based combined unsupervised classification 2020', {"local":"/data/classification/unsupervised_rostenice/40cl_evi-rvi4s1-20_km_15it_rd_md.tif", "format":"GTiff"},'raster')
s12_evi_classification__ds=ds_from_metadata(s12_evi_classification__metadata)
s12_evi_classification=GeoConcept('Sentinel-1 and Sentinel-2 based combined unsupervised classification 2020','Sentinel-1 and Sentinel-2 based combined unsupervised classification 2020. ', 'Feature',json_feature_with_raster_structure, data_source=s12_evi_classification__ds,subgeoconcepts=[])
s12_evi_classification.set_raster_output_backend('',s12_evi_classification.get_data_source().get_attributes()['local'])


s12_evi_classification__metadata.set_metatable(metadata_table)
s12_evi_classification__metadata.set_validitydates([datetime.datetime(2020,8,31).strftime('%Y-%m-%d'),None])
metadata_fid_sequence=sequence_generator(dbs_olu_cz.get_last_value_of_seq(s12_evi_classification__metadata.get_metatable().get_self_sequence_name('fid'),set_last_value=True,data_table=s12_evi_classification__metadata.get_metatable().get_scheme()+'.'+s12_evi_classification__metadata.get_metatable().get_name(),id_attribute='fid'))
s12_evi_classification__metadata.set_olu_id(next(metadata_fid_sequence))
dbs_olu_cz.execute(s12_evi_classification__metadata.convert_to_sql_insert())


s12_evi_classification__ds.set_metatable(ds_table)
ds_fid_sequence=sequence_generator(dbs_olu_cz.get_last_value_of_seq(s12_evi_classification__ds.get_metatable().get_self_sequence_name('fid'),set_last_value=True,data_table=s12_evi_classification__ds.get_metatable().get_scheme()+'.'+s12_evi_classification__ds.get_metatable().get_name(),id_attribute='fid'))
s12_evi_classification__ds.set_olu_id(next(ds_fid_sequence))
s12_evi_classification__ds.set_type(2)
dbs_olu_cz.execute(s12_evi_classification__ds.convert_to_sql_insert())
s12_evi_classification__ds.set_type('GTiff')


s12_evi_classification__im=s12_evi_classification.read_raster_output_backend(1)


s12_evi_classification__im.image_to_geo_coordinates(0,0)==(600005.0, 5500015.0)


s12_evi_classification__im.image_to_geo_coordinates(*s12_evi_classification__im.get_data().shape)==(709805.0, 5390215.0)


s12_evi_classification__im_geom=ogr.CreateGeometryFromWkt('POLYGON((600005 5390215,600005 5500015,709805 5500015,709805 5390215,600005 5390215))')


s12_evi_classification__im_geom.Transform(utm32633_to_wgs84)


lyr.SetSpatialFilter(s12_evi_classification__im_geom)
lyr.ResetReading()
lyr.GetFeatureCount()


olu_atts_id_gen=sequence_generator(dbs_olu_cz.get_last_value_of_seq('olu2.olu_attributes_fid_seq',set_last_value=True,data_table='olu2.olu_attribute_set',id_attribute='fid'))


bad_list=[]
count=0
for c in range(lyr.GetFeatureCount()):
    lyr_f=lyr.GetNextFeature()
    if lyr_f.GetGeometryRef().IsEmpty():
        continue
    lyr_f.GetGeometryRef().Transform(wgs84_to_utm32633)
    attributes_fid=next(olu_atts_id_gen)
    attributes_table='olu2.olu_attribute_set'
    attributes2object_table='olu2.atts_to_object'
    try:
        [olu_cur.execute(i) for i in apply_function(s12_evi_class_transformations(lyr_f,attributes_table,attributes2object_table,attributes_fid,s12_evi_classification__im),'two_insert_statements')]
    except:
        bad_list.append(lyr_f.GetFID())
        continue
    count+=1
    if count%1000==0:
        olu_conn.commit()
        count=0


olu_conn.commit()


lyr_f=lyr.GetFeature(5089)


lyr_f.GetGeometryRef().Transform(wgs84_to_utm32633)


Imagee(*s2_evi_classification__im.clip_by_shape(lyr_f.GetGeometryRef().ExportToWkt())).get_statistics(True,True,True)


import matplotlib.pyplot as plt
from matplotlib.pyplot import imshow
plt.rcParams["figure.figsize"] = (10,5)


plt.imshow(Imagee(*s2_evi_classification__im.clip_by_shape(lyr_f.GetGeometryRef().ExportToWkt())).get_data(), origin="upper", cmap='gray', interpolation='nearest')
plt.colorbar()
plt.show()


cropped_im=Imagee(*s2_evi_classification__im.clip_by_shape(lyr_f.GetGeometryRef().ExportToWkt()))


from scipy.stats import mode


mode(cropped_im.get_data()[cropped_im.get_data().mask==False])[0][0]


soilgrids__metadata=MetaData('ISRIC - Soilgrids - selected attributes', {"url":"https://files.isric.org/soilgrids/latest/data/", "format":"VRT"},'raster data')
soilgrids_ds=ds_from_metadata(soilgrids__metadata)
soilgrids=GeoConcept('ISRIC Soilgrids','ISRIC - Soilgrids - selected attributes describing soil quality.', 'Feature',json_feature_with_raster_structure, data_source=soilgrids_ds,subgeoconcepts=[])


#bbox_homo=(1959701, 5605502, 2071259, 5491536)
wgs84_to_homolosine.TransformPoint(15.54245,49.633326)


wgs84_to_homolosine.TransformPoint(17.64693,48.617245)


bbox_homo=(2234625, 5498231, 2383190, 5391121)
statistical_value='mean'

depths=[0,5,15,30,60,100]
depth_um='cm'

intervals1=[(depths[i],depths[i+1]) for i in range(len(depths)-1)]
intervals2=[(0,30)]

soil_parameters_dictionary={'ocd':{'name':'organic carbon density','depth':intervals1},
'ocs': {'name':'organic carbon stocks','depth':intervals2},
'phh2o':{'name':'pH water','depth':intervals1},'nitrogen':{'name':'nitrogen','depth':intervals1}}


for key, value in soil_parameters_dictionary.items():
    for depth in value['depth']:
        try:
            fn=isric_reader(bbox_homo,key,key,[depth[0],depth[1]],'mean','jm_kraj',download=True)
            if fn is None:
                print(fn+'failed')
                continue
            else:
                soilgrids.append_subgeoconcept(SubGeoConcept( (key+'_'+str(depth[0])+'-'+str(depth[1])+depth_um+'_'+statistical_value), (key+'_'+str(depth[0])+'-'+str(depth[1])+depth_um+'_'+statistical_value),'Feature', soilgrids.get_attributes(),data_source=DataSource('GeoTIFF',soilgrids.get_data_source().get_name(),({'local':fn,'format':'GeoTIF'}),None,fn),supergeoconcept=soilgrids,table_inheritance=False,type='semantic',subgeoconcepts=[]))
                print(fn+'done')
        except:
            print(key+'/'+key+'_'+str(depth[0])+'-'+str(depth[1])+depth_um+'_'+statistical_value+'failed')
            continue


for sub in soilgrids.get_subgeoconcepts():
    sub.set_raster_output_backend(os.getcwd()+'/',sub.get_data_source().get_data_file())
    sub.set_imagee(sub.read_raster_output_backend())


[s.get_raster_output_backend() for s in soilgrids.get_subgeoconcepts()]


soilgrids.get_subgeoconcepts()


def accumulate_attributes_by_supergeoconcept(supergeoconcept,feature_wkt_geometry):
    attribute_dictionary={}
    for subgeoconcept in supergeoconcept.get_subgeoconcepts():
        working_image=Imagee(*subgeoconcept.get_imagee().clip_by_shape(feature_wkt_geometry))
        attribute_dictionary[re.sub('[^0-9a-zA-Z]+', '_', subgeoconcept.get_name()).lower()]=working_image.get_statistics()
    return attribute_dictionary


lyr_f.GetGeometryRef().Transform(wgs84_to_homolosine)


accumulate_attributes_by_supergeoconcept(soilgrids,lyr_f.GetGeometryRef().ExportToWkt())


soilgrids__metadata.set_metatable(metadata_table)
soilgrids__metadata.set_validitydates([datetime.datetime(2021,4,30).strftime('%Y-%m-%d'),None])
metadata_fid_sequence=sequence_generator(dbs_olu_cz.get_last_value_of_seq(soilgrids__metadata.get_metatable().get_self_sequence_name('fid'),set_last_value=True,data_table=soilgrids__metadata.get_metatable().get_scheme()+'.'+soilgrids__metadata.get_metatable().get_name(),id_attribute='fid'))
soilgrids__metadata.set_olu_id(next(metadata_fid_sequence))
dbs_olu_cz.execute(soilgrids__metadata.convert_to_sql_insert())


soilgrids_ds.set_metatable(ds_table)
ds_fid_sequence=sequence_generator(dbs_olu_cz.get_last_value_of_seq(soilgrids_ds.get_metatable().get_self_sequence_name('fid'),set_last_value=True,data_table=soilgrids_ds.get_metatable().get_scheme()+'.'+soilgrids_ds.get_metatable().get_name(),id_attribute='fid'))
soilgrids_ds.set_olu_id(next(ds_fid_sequence))
soilgrids_ds.set_type(2)
dbs_olu_cz.execute(soilgrids_ds.convert_to_sql_insert())
soilgrids_ds.set_type('VRT')


lyr.ResetReading()
lyr.SetSpatialFilter(None)
lyr.GetFeatureCount()


olu_atts_id_gen=sequence_generator(dbs_olu_cz.get_last_value_of_seq('olu2.olu_attributes_fid_seq',set_last_value=True,data_table='olu2.olu_attribute_set',id_attribute='fid'))


bad_list=[]
count=0
for c in range(lyr.GetFeatureCount()):
    lyr_f=lyr.GetNextFeature()
    if lyr_f.GetGeometryRef().IsEmpty():
        continue
    lyr_f.GetGeometryRef().Transform(wgs84_to_homolosine)
    attributes_fid=next(olu_atts_id_gen)
    attributes_table='olu2.olu_attribute_set'
    attributes2object_table='olu2.atts_to_object'
    try:
        [olu_cur.execute(i) for i in apply_function(raster_transformations(lyr_f,attributes_table,attributes2object_table,attributes_fid,soilgrids),'two_insert_statements')]
    except:
        bad_list.append(lyr_f.GetFID())
        continue
    count+=1
    if count%1000==0:
        olu_conn.commit()
        count=0
        
olu_conn.commit()

eudem_cz__metadata=MetaData('EU DEM in Czech Republic', {"local":"/home/jupyter-dima/eu_dem_czat3035.tif", "format":"GTiff"},'raster data')
eudem_cz_ds=ds_from_metadata(eudem_cz__metadata)
eudem_cz=GeoConcept('EU DEM in Czech Republic','EU DEM in Czech Republic.', 'Feature',json_feature_with_raster_structure, data_source=eudem_cz_ds,subgeoconcepts=[])
eudem_cz.set_raster_output_backend('',eudem_cz.get_data_source().get_attributes()['local'])

bbox_r=(4721014,2961829,4883864,2862985)

eudem_im=eudem_cz.read_raster_output_backend(1,bbox_r)

im_slope=Imagee(*eudem_im.calculate_slope())

im_azimuth=Imagee(*eudem_im.calculate_azimuth(exact=True))

im_twi=get_twi(eudem_im)

eudem_cz__metadata.set_metatable(metadata_table)
eudem_cz__metadata.set_validitydates([datetime.datetime(2021,4,30).strftime('%Y-%m-%d'),None])
metadata_fid_sequence=sequence_generator(dbs_olu_cz.get_last_value_of_seq(eudem_cz__metadata.get_metatable().get_self_sequence_name('fid'),set_last_value=True,data_table=eudem_cz__metadata.get_metatable().get_scheme()+'.'+eudem_cz__metadata.get_metatable().get_name(),id_attribute='fid'))
eudem_cz__metadata.set_olu_id(next(metadata_fid_sequence))
dbs_olu_cz.execute(eudem_cz__metadata.convert_to_sql_insert())

eudem_cz_ds.set_metatable(ds_table)
ds_fid_sequence=sequence_generator(dbs_olu_cz.get_last_value_of_seq(eudem_cz_ds.get_metatable().get_self_sequence_name('fid'),set_last_value=True,data_table=eudem_cz_ds.get_metatable().get_scheme()+'.'+eudem_cz_ds.get_metatable().get_name(),id_attribute='fid'))
eudem_cz_ds.set_olu_id(next(ds_fid_sequence))
eudem_cz_ds.set_type(2)
dbs_olu_cz.execute(eudem_cz_ds.convert_to_sql_insert())
eudem_cz_ds.set_type('GeoTIFF')

olu_atts_id_gen=sequence_generator(dbs_olu_cz.get_last_value_of_seq('olu2.olu_attributes_fid_seq',set_last_value=True,data_table='olu2.olu_attribute_set',id_attribute='fid'))

lyr.SetSpatialFilter(s12_evi_classification__im_geom)
lyr.ResetReading()
lyr.GetFeatureCount()

attributes_table='olu2.olu_attribute_set'
attributes2object_table='olu2.atts_to_object'


bad_list=[]
count=0
for c in range(lyr.GetFeatureCount()):
    lyr_f=lyr.GetNextFeature()
    if lyr_f.GetGeometryRef().IsEmpty():
        continue
    lyr_f.GetGeometryRef().Transform(wgs84_to_etrs3035)
    attributes_fid=next(olu_atts_id_gen)
    attributes_table='olu2.olu_attribute_set'
    attributes2object_table='olu2.atts_to_object'
    try:
        [olu_cur.execute(i) for i in apply_function(geomorphology_transformations(lyr_f,attributes_table,attributes2object_table,attributes_fid,eudem_im,im_slope,im_azimuth,im_twi),'two_insert_statements')]
    except:
        bad_list.append(lyr_f.GetFID())
        continue
    count+=1
    if count%1000==0:
        olu_conn.commit()
        count=0

olu_conn.commit()

olu=GeoConcept(x, y, z)
#vybraty vsi spodnii administratyvnyi 
adm_units=dbs_olu.execute('select distinct unit_fid from olu2.olu_object_to_admin_unit ')
def generate_olu_reference_geometries( order):
    for  adm_unit in 
    
    
    
    
    olu_objects_union_cascaded=ogr.CreateGeometryFromWkt('MULTIPOLYGON EMPTY')
    result_list=[]
    ku=find_neighbors_level(G.reverse(),kod_obce,4)
    while True:
        try:
            cislo_ku=next(ku)
            print(cislo_ku)
            sub=lpis_cz.get_subgeoconcept_by_adm_node(cislo_ku)
            print(sub.get_name())
            lpis_feature_gen=sub.read_features_from_table(100)
            for feature_batch in lpis_feature_gen:
                if len(feature_batch)>0:
                    print(len(feature_batch))
                    for feature in feature_batch:
                        feature.transform_geometry(sjtsk5514_to_wgs84)
                        
                else:
                    break
        except:
            print('no next feature')
            break

    obec_gen=admunit_cz.read_features_from_table_by_sqlcondition('id=%s' %kod_obce,1)
    obec=next(obec_gen)[0]
    obec_area=ogr.CreateGeometryFromWkb(obec.get_geometry()).Area()
 
    try:
        olu_objects_union=ogr.CreateGeometryFromWkb(dbs_olu.execute("select st_asbinary(st_union(geom)) from %s where admunit_id='%s'"  % ('olu2.olu_object',kod_obce))[0][0])
        olu_objects_union.Transform(wgs84_to_sjtsk5514)
        if abs(obec_area-olu_objects_union.Area())<100:
            return None
        olu_objects_union_cascaded=olu_objects_union#olu_objects_union.UnionCascaded()
        del(olu_objects_union)
        print(olu_objects_union_cascaded.Area())
        print(ogr.CreateGeometryFromWkb(obec.get_geometry()).Area())
    except:
        dbs_olu.disconnect()
        dbs_olu.connect()
    
    ruian_features_gen=ruian_parcely_cz.get_subgeoconcept_by_name(kod_obce).read_features_from_table_by_sqlcondition("st_intersects(geom,st_setsrid(st_geomfromtext('%s'),5514))" % (ogr.CreateGeometryFromWkb(obec.get_geometry()).Difference(olu_objects_union_cascaded).ExportToWkt()) ,1000)
             
    for feature_batch in ruian_features_gen:
        print(len(feature_batch))
        if len(feature_batch)>0:
            for feature in feature_batch:
                feature.transform_geometry(sjtsk5514_to_wgs84)
                olu_id=next(olu_id_gen)
                atts_id=next(olu_atts_id_gen)
                [dbs_olu.execute(i) for i in apply_function(ruian_transformations(feature,olu_id,atts_id,kod_obce,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table),'three_insert_statements')]
        else:
            break
    
    try:
        olu_objects_union=ogr.CreateGeometryFromWkb(dbs_olu.execute("select st_asbinary(st_union(geom)) from %s where admunit_id='%s'"  % ('olu2.olu_object',kod_obce))[0][0])
        olu_objects_union.Transform(wgs84_to_sjtsk5514)
        if abs(obec_area-olu_objects_union.Area())<100:
            return None
        olu_objects_union_cascaded=olu_objects_union#olu_objects_union.UnionCascaded()
        del(olu_objects_union)
        print(olu_objects_union_cascaded.Area())
    except:
        dbs_olu.disconnect()
        dbs_olu.connect()
        
    if ogr.CreateGeometryFromWkb(obec.get_geometry()).Difference(olu_objects_union_cascaded) is None:
        return None
    
    ua_features_gen=ua_cz.read_features_from_table_by_sqlcondition("st_intersects(geom,st_transform(st_setsrid(st_geomfromtext('%s'),5514),3035)) and data->>'CODE2012' not in ('12210','12220','12230')" % (ogr.CreateGeometryFromWkb(obec.get_geometry()).Difference(olu_objects_union_cascaded).ExportToWkt()) ,100)     
    
    for feature_batch in ua_features_gen:
        print(len(feature_batch))
        if len(feature_batch)>0:
            for feature in feature_batch:
                feature.transform_geometry(etrs3035_to_wgs84)
                olu_id=next(olu_id_gen)
                atts_id=next(olu_atts_id_gen)
                [dbs_olu.execute(i) for i in apply_function(ua_transformations(feature,olu_id,atts_id,kod_obce,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table),'three_insert_statements')]
        else:
            break

    try:
        olu_objects_union=ogr.CreateGeometryFromWkb(dbs_olu.execute("select st_asbinary(st_union(geom)) from %s where admunit_id='%s'"  % ('olu2.olu_object',kod_obce))[0][0])
        olu_objects_union.Transform(wgs84_to_sjtsk5514)
        if abs(obec_area-olu_objects_union.Area())<100:
            return None
        olu_objects_union_cascaded=olu_objects_union#olu_objects_union.UnionCascaded()
        del(olu_objects_union)
        print(olu_objects_union_cascaded.Area())
    except:
        dbs_olu.disconnect()
        dbs_olu.connect()
    
    
    if ogr.CreateGeometryFromWkb(obec.get_geometry()).Difference(olu_objects_union_cascaded) is None:
        return None

    corine_features_gen=corine_cz.get_subgeoconcept_by_name('2018').read_features_from_table_by_sqlcondition("st_intersects(geom,st_transform(st_setsrid(st_geomfromtext('%s'),5514),4326))" % (ogr.CreateGeometryFromWkb(obec.get_geometry()).Difference(olu_objects_union_cascaded).ExportToWkt()) ,10)     
    
    for feature_batch in corine_features_gen:
        print(len(feature_batch))
        if len(feature_batch)>0:
            for feature in feature_batch:
                feature.transform_geometry(wgs84_to_wgs84)
                olu_id=next(olu_id_gen)
                atts_id=next(olu_atts_id_gen)
                [dbs_olu.execute(i) for i in apply_function(corine_transformations(feature,olu_id,atts_id,kod_obce,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table),'three_insert_statements')]
        else:
            break

