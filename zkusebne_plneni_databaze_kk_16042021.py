# %%
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

# %%
from importlib import reload

# %%
del Imagee
reload(tridy)
from tridy import Imagee

# %%
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

# %%
replacement_dictionary = {"[posledni_den_mesice]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d'),"[lpis_cz__posledni_aktualizace]":lpis_cz__posledni_aktualizace().strftime('%Y%m%d'), "[vcera]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d')} 
json_feature_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_feature_with_bigid_structure=[{"name":"id","type":"bigint primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_feature_with_bigid__without_pk_structure=[{"name":"id","type":"bigint"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_admin_unit_structure=[{"name":"id","type":"integer primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_admin_unit_structure_at=[{"name":"id","type":"text primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_feature_with_raster_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"raster_maps","type":"raster"}]

# %%
lpis_hilucs={'2':'111', '3':'111', '4':'111', '5':'111', '6':'110', '7':'110', '9':'110', '10':'110', '11':'110', '12':'631', '91':'120', '97':'142', '98':'120', '99':'120'} 
ruian_zpv_hilucs={'1':'112', '2':'120', '3':'121', '4':'120', '5':'120', '6':'632', '7':'632', '8':'632', '9':'632', '10':'632', '11':'632', '12':'530', '13':'530', '14':'411', '15':'411', '16':'411', '17':'411', '18':'415', '19':'344', '20':'343', '21':'335', '22':'341', '23':'411', '24':'130', '25':'433', '26':'650', '27':'630', '28':'632', '29':'244','30':'630'}  
ruian_dp_hilucs={'2':'111', '3':'111', '4':'111', '5':'500', '6':'110', '7':'110', '8':'110', '10':'120', '11':'632', '13':'510', '14':'660'} 
corine_hilucs={'111':'500', '112':'500', '121':'660', '122':'410', '123':'414', '124':'413', '131':'130', '132':'433', '133':'660', '141':'340', '142':'340', '211':'111', '212':'111', '213':'111', '221':'111', '222':'111', '223':'111', '231':'111', '241':'111', '242':'111', '243':'111', '244':'120', '311':'120', '312':'120', '313':'120', '321':'631', '322':'631', '323':'631', '324':'631', '331':'631', '332':'631', '333':'631', '334':'620', '335':'631', '411':'631', '412':'631', '421':'631', '422':'631', '423':'631', '511':'414', '512':'660', '521':'660', '522':'414', '523':'660'} 
ua_hilucs={'11100':'500', '11200':'500', '11210':'500', '11220':'500', '11230':'500', '11240':'500', '11300':'500', '12100':'300', '12200':'410', '12210':'411', '12220':'411', '12230':'412', '12300':'414', '12400':'413', '13100':'130', '13300':'600', '13400':'600', '14100':'344', '14200':'340', '21000':'110', '22000':'110', '23000':'110', '24000':'110', '25000':'110', '30000':'120', '31000':'120', '32000':'120', '33000':'660', '40000':'660', '50000':'660', '91000':'660', '92000':'660'} 
lpis_clc={'2':'210','3':'220','4':'220','5':'220','6':'200','7':'231','9':'200','10':'200','11':'200','12':'240','91':'200','97':'500','98':'300','99':'300'}
ruian_zpv_clc={'1':'200','2':'200','3':'300','4':'300','5':'300','6':'510','7':'510','8':'510','9':'510','10':'510','11':'410','12':'100','13':'130','14':'122','15':'122','16':'122','17':'122','18':'122','19':'141','20':'142','21':'100','22':'100','23':'120','24':'131','25':'132','26':'100','27':'990','28':'510','29':'120','30':'300'}
ruian_dp_clc={'2':'210','3':'220','4':'220','5':'141','6':'200','7':'231','8':'231','10':'300','11':'510','13':'110','14':'990'}
ua_clc={'11100':'111','11200':'112','11210':'112','11220':'112','11230':'112','11240':'112','11300':'100','12100':'120','12210':'122','12200':'122','12220':'122','12230':'122','12300':'123','12400':'124','13100':'130','13300':'133','13400':'990','14100':'141','14200':'142','21000':'200','22000':'220','23000':'230','24000':'240','25000':'200','30000':'300','31000':'300','32000':'320','33000':'330','40000':'410','50000':'510','91000':'999','92000':'999'}

# %%
wgs84_sr=osr.SpatialReference()
wgs84_sr.ImportFromProj4('+proj=longlat +datum=WGS84 +no_defs')

sjtsk5514_sr=osr.SpatialReference()
sjtsk5514_sr.ImportFromProj4('+proj=krovak +lat_0=49.5 +lon_0=24.83333333333333 +alpha=30.28813975277778 +k=0.9999 +x_0=0 +y_0=0 +ellps=bessel +units=m +towgs84=570.8,85.7,462.8,4.998,1.587,5.261,3.56 +no_defs')

etrs3035_sr=osr.SpatialReference()
etrs3035_sr.ImportFromProj4('+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')

sjtsk5514_to_etrs3035=osr.CoordinateTransformation(sjtsk5514_sr,etrs3035_sr)
sjtsk5514_to_wgs84=osr.CoordinateTransformation(sjtsk5514_sr,wgs84_sr)
wgs84_to_sjtsk5514=osr.CoordinateTransformation(wgs84_sr,sjtsk5514_sr)
wgs84_to_etrs3035=osr.CoordinateTransformation(wgs84_sr,etrs3035_sr)
etrs3035_to_wgs84=osr.CoordinateTransformation(etrs3035_sr,wgs84_sr)
etrs3035_to_sjtsk5514=osr.CoordinateTransformation(etrs3035_sr,sjtsk5514_sr)
wgs84_to_wgs84=osr.CoordinateTransformation(wgs84_sr,wgs84_sr)

# %%
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

# %%
ua_conn=psycopg2.connect("dbname=urban_atlas host=localhost user=... password=... port=5432")
corine_conn=psycopg2.connect("dbname=corine_land_cover host=localhost user=... password=... port=5432")
olu_conn=psycopg2.connect("dbname=czoluv2 host=localhost user=... password=... port=5432")

# %%
conn_corine=ogr.Open("PG: host=localhost dbname=corine_land_cover user=... password=...")

# %%
conn_ua=ogr.Open("PG: host=localhost dbname=urban_atlas user=... password=...")

# %%
ua_cur=ua_conn.cursor()

# %%
corine_cur=corine_conn.cursor()

# %%
olu_cur=olu_conn.cursor()

# %%
olu_cur2=olu_conn.cursor()

# %%
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

# %%
olu_cur.execute('SELECT last_value FROM olu2.olu_object_fid_seq')

# %%
olu_id_gen=sequence_generator(olu_cur.fetchone()[0])

# %%
olu_cur.execute('SELECT last_value FROM olu2.olu_attributes_fid_seq')

# %%
olu_atts_id_gen=sequence_generator(olu_cur.fetchone()[0])

# %%
olu_cur2.execute("select st_asbinary(geom), fid from olu2.administrative_unit where level_code=3 and fid='CZ041'")

# %%
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

# %%
olu_conn.commit()

# %%
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

# %%
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

# %%
admunit_cz__ds=ds_from_metadata(admunit_cz__metadata)

ruian_cz__ds=ds_from_metadata(ruian_cz__metadata)
ruian_cz__ds.set_attributes({**ruian_cz__ds.get_attributes(),**{'layer':'Parcely'}})

lpis_cz__ds_xml=ds_from_metadata(lpis_cz__metadata,format='XML')

# %%
admunit_cz=GeoConcept('Administrative units in Czech Republic','Administrative units in Czech Republic. All levels.',
                      'AdmUnitFeature',json_admin_unit_structure, data_source=admunit_cz__ds, subgeoconcepts=[] )

ruian_parcely_cz=GeoConcept('Land parcels in Czech Republic','Digital land parcels (parcely) in Czech Republic.',
                      'FeatureWithID',json_feature_with_bigid_structure, data_source=ruian_cz__ds,subgeoconcepts=[],adm_graph_node='1')

lpis_cz=GeoConcept('LPIS in Czech Republic','LPIS in Czech Republic. All levels.',
                      'FeatureWithID',json_feature_structure, data_source=lpis_cz__ds_xml, subgeoconcepts=[], adm_graph_node='1')

# %%
if 'local' in ruian_parcely_cz.get_data_source().get_attributes():
    ruian_parcely_cz.get_data_source().set_data_file(ruian_parcely_cz.get_data_source().get_attributes()['local']) 

# %%
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

# %%
admunit_cz.create_table(dbs_admin, name='default',scheme='cz',conflict='append')

ruian_parcely_cz.create_table(dbs_ruian_cz, name='default',scheme='public',conflict='append',adm_graph_node='1')

lpis_cz.create_table(dbs_lpis_cz,name='default',scheme='public',conflict='append', adm_graph_node='1')

# %%
admunit_cz__ds.set_data_file('20201031_ST_UKSG.xml')

# %%
concept_list=['Staty','Vusc','Okresy','Obce','KatastralniUzemi']
concept_additional_attributes={'Staty':{'level_value':0,'parent_value':'null','id_attribute':'Kod'},
                               'Vusc':{'level_value':1,'parent_value':'1','id_attribute':'Kod'},
                               'Okresy':{'level_value':2,'parent_attribute':'VuscKod','id_attribute':'Kod'},
                               'Obce':{'level_value':3,'parent_attribute':'OkresKod','id_attribute':'Kod'},
                               'KatastralniUzemi':{'level_value':4,'parent_attribute':'ObecKod','id_attribute':'Kod'}}
                               
for l in list(set(concept_list).intersection(set(admunit_cz.get_data_source().list_layers()))):
    admunit_cz.append_subgeoconcept(SubGeoConcept(l,l,'AdmUnitFeature',admunit_cz.get_attributes(),data_source=DataSource(admunit_cz.get_data_source().get_type(),admunit_cz.get_data_source().get_name(),({**admunit_cz.get_data_source().get_attributes(),**{'layer':l}}),None,admunit_cz.get_data_source().get_data_file()),supergeoconcept=admunit_cz,table_inheritance=False,type='semantic',subgeoconcepts=[]))

# %%
concept_list=['Parcely']
concept_additional_attributes={'Parcely':{'id_attribute':'Id'}}

for i in re.findall('\{.*?\}',ruian_parcely_cz.get_data_source().get_attributes()['url']): 
    if i[1:-1] in list(compilable_tree_dictionary(admunit_cz).keys()):
        for j in apply_function(compilable_tree_dictionary(admunit_cz),i[1:-1]):
            ruian_parcely_cz.append_subgeoconcept(SubGeoConcept(str(j),'RUIAN land parcels in Czech administrative territorial unit %s ' % str(j),'FeatureWithID',ruian_parcely_cz.get_attributes(),data_source=DataSource(ruian_parcely_cz.get_data_source().get_type(),ruian_parcely_cz.get_data_source().get_name(),(dict(ruian_parcely_cz.get_data_source().get_attributes(),**{'url':ruian_parcely_cz.get_data_source().get_attributes()['url'].replace(i,str(j))})),None,None),adm_graph_node=str(j),supergeoconcept=ruian_parcely_cz,table_inheritance=True,subgeoconcepts=[]))


# %%
for i in re.findall('\{.*?\}',lpis_cz.get_data_source().get_attributes()['url']): 
    if i[1:-1] in list(compilable_tree_dictionary(admunit_cz).keys()):
        for j in apply_function(compilable_tree_dictionary(admunit_cz),i[1:-1]):
            lpis_cz.append_subgeoconcept(SubGeoConcept(str(j),'LPIS in Czech administrative territorial unit %s ' % str(j),'FeatureWithID',lpis_cz.get_attributes(),data_source=DataSource(lpis_cz.get_data_source().get_type(),lpis_cz.get_data_source().get_name(),(dict(lpis_cz.get_data_source().get_attributes(),**{'url':lpis_cz.get_data_source().get_attributes()['url'].replace(i,str(j))})),None,None),supergeoconcept=lpis_cz,table_inheritance=True,subgeoconcepts=[],type='spatial:admin',adm_graph_node=str(j)))

# %%
G=apply_function(compilable_node_dictionary(admunit_cz),'admunit__tree')

# %%
for s in lpis_cz.get_subgeoconcepts():
    municipality=[i for i in G.neighbors(str(s.get_adm_graph_node()))][0]
    s.create_table(dbs_lpis_cz,name='ob_'+str(municipality),scheme='data',conflict='append',adm_graph_node=municipality)
    s.set_table(View('ku_'+s.get_name(), s.get_attributes(), s.get_table(), "dictionary_select5((data->'PREKRYVKATUZE')::json, 'KUKOD == \"%s\" AND PLATNOSTDO>\"[aktualni_cas]\"')::jsonb not in ('[]'::jsonb,'{}'::jsonb)" % s.get_name(), dbs=dbs_lpis_cz, scheme='data',type='usual'))
    dbs_lpis_cz.execute(s.get_table().create_script())

# %%
for sub in ruian_parcely_cz.get_subgeoconcepts():
    sub.create_table(dbs_ruian_cz,name='ob_'+sub.get_name(),scheme='data',conflict='append',adm_graph_node=sub.get_adm_graph_node())

# %%
feature_gen=admunit_cz.read_features_from_table_by_sqlcondition("level=1 and data->>'Nazev'='Karlovarský kraj'",1)
feature=next(feature_gen)[0]
feature.get_data()

# %%
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
     'object_properties':{'function':(lambda x,y,z: {**{'fid':x},**{'dataset_id':1},**{'z_value':100000},**{'admunit_id':y},**{'valid_from':z.get_data()['PLATNOSTOD'],'valid_to':'NULL'} }),'parameters':['object_id','admunit_id','feature']},
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
     'object_properties':{'function':(lambda x,y,z: {**{'fid':x},**{'dataset_id':2},**{'z_value':10000},**{'admunit_id':y},**{'valid_from':z.get_data()['PlatiOd'],'valid_to':( ('\''+z.get_data()['PlatiDo']+'\'') if z.get_data()['PlatiDo'] is not None else 'NULL')}}),'parameters':['object_id','admunit_id','feature']},
     'attributes_properties':{'function':(lambda x,y: {**{'fid':x},**{'hilucs_value':ruian_zpv_hilucs[str(y.get_data()['ZpusobyVyuzitiPozemku'])] if y.get_data()['ZpusobyVyuzitiPozemku'] is not None else ruian_dp_hilucs[str(y.get_data()['DruhPozemkuKod'])]},**{'clc_value':ruian_zpv_clc[str(y.get_data()['ZpusobyVyuzitiPozemku'])] if y.get_data()['ZpusobyVyuzitiPozemku'] is not None else ruian_dp_clc[str(y.get_data()['DruhPozemkuKod'])]},**{'atts':y.get_data()},**{'dataset_id':2}}),'parameters':['attributes_id','feature']},
     'object_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,dataset_fid,z_value,geom,valid_from,valid_to) VALUES (%s,%s,%s,ST_SetSRID(ST_Multi(ST_GeomFromText('%s')),4326),'%s',%s)" % (w,x,z['dataset_id'],z['z_value'],y,z['valid_from'], z['valid_to'] ) ),'parameters':['object_table_name','object_id','object_geom','object_properties']},\
     'attributes_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,hilucs_id,atts,dataset_fid,clc_id) VALUES (%s,%s,('%s'::json),%s,%s)" % (w,x,z['hilucs_value'],json.dumps(y.get_data()),z['dataset_id'],z['clc_value']) ),'parameters':['attributes_table_name','attributes_id','feature','attributes_properties']},\
     'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,1)" % (z,x,y) ),'parameters':['object_id','attributes_id','attributes2object_table_name']},\
     'object2admunit_insert_statement':{'function':(lambda x,y: "INSERT INTO olu2.olu_object_to_admin_unit (object_fid,unit_fid) VALUES (%s,'%s')" % (x,y) ),'parameters':['object_id','admunit_id']},\
     'three_insert_statements':{'function':(lambda x,y,z: [x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement']},\
     'four_insert_statements':{'function':(lambda w,x,y,z: [w,x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement','object2admunit_insert_statement']},\
    }
    return o_dict


def esdr_transformations(ogr_olu_object, attributes_table, attributes2object_table, attributes_fid,  wrbfu_im):
    o_dict=\
    {
    'feature':{'object':ogr_olu_object},\
    'feature_json':{'function':(lambda y: json.loads(y.ExportToJson())),'parameters':['feature']},\
    'feature_geometry':{'function':(lambda y: y.GetGeometryRef().ExportToWkt() ),'parameters':['feature']},\
    'feature_id':{'function':(lambda y: int(y['id'])),'parameters':['feature_json']},\
    'attributes_table':{'object':attributes_table},\
    'attributes2object_table':{'object':attributes2object_table},\
    'attributes_fid':{'object':attributes_fid},\
    'wrbfu_image':{'object':wrbfu_im},\
    'attribute_properties':{'function':(lambda u, v, w: {**{'fid':u},**{'dataset_id':5},**{'atts':{'soil type':wrbfu_dict[Imagee(*w.clip_by_shape(v)).get_mode_value()]} } }),'parameters':['attributes_fid','feature_geometry','wrbfu_image']},
    'attributes_insert_statement':{'function':(lambda x, y,z: "INSERT INTO %s (fid,atts,dataset_fid) VALUES (%s,('%s'::json),%s)" % (x,y,json.dumps(z['atts']),z['dataset_id']) ),'parameters':['attributes_table','attributes_fid','attribute_properties']},\
    'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,2)" % (z,x,y) ),'parameters':['feature_id','attributes_fid','attributes2object_table']},\
    'two_insert_statements':{'function':(lambda y,z: [y,z]),'parameters':['attributes_insert_statement','attributes2object_insert_statement']},\
    }
    return o_dict

def esdr_full_transformations(ogr_olu_object, attributes_table, attributes2object_table, attributes_fid, aglim1_im, aglim2_im, il_im, parmado_im,  roo_im,  txdepchg_im, txsrfdo_im,  txsubdo_im,  wrbfu_im):
    o_dict=\
    {
    'feature':{'object':ogr_olu_object},\
    'feature_json':{'function':(lambda y: json.loads(y.ExportToJson())),'parameters':['feature']},\
    'feature_geometry':{'function':(lambda y: y.GetGeometryRef().ExportToWkt() ),'parameters':['feature']},\
    'feature_id':{'function':(lambda y: int(y['id'])),'parameters':['feature_json']},\
    'attributes_table':{'object':attributes_table},\
    'attributes2object_table':{'object':attributes2object_table},\
    'attributes_fid':{'object':attributes_fid},\
    'aglim1_image':{'object':aglim1_im},\
    'aglim2_image':{'object':aglim2_im},\
    'il_image':{'object':il_im},\
    'parmado_image':{'object':parmado_im},\
    'roo_image':{'object':roo_im},\
    'txdepchg_image':{'object':txdepchg_im},\
    'txsrfdo_image':{'object':txsrfdo_im},\
    'txsubdo_image':{'object':txsubdo_im},\
    'wrbfu_image':{'object':wrbfu_im},\
    'attribute_properties':{'function':(lambda p,q,r,s,t,u,v,w,x,y,z: {**{'fid':p},**{'dataset_id':6},**{'atts':{'agricultural_limitations_primary':aglim1_dict[Imagee(*r.clip_by_shape(q)).get_mode_value()],  'agricultural_limitations_secondary':aglim2_dict[Imagee(*s.clip_by_shape(q)).get_mode_value()],  'soil_impermeable_layer_depth':il_dict[Imagee(*t.clip_by_shape(q)).get_mode_value()],  'soil_parent_material':parmado_dict[Imagee(*u.clip_by_shape(q)).get_mode_value()],  'obstacles_to_roots':roo_dict[Imagee(*v.clip_by_shape(q)).get_mode_value()],  'textural_change_depth':txdepchg_dict[Imagee(*w.clip_by_shape(q)).get_mode_value()],  'dominant_surface_textural_class':txsrfdo_dict[Imagee(*x.clip_by_shape(q)).get_mode_value()],  'dominant_sub_surface_textural_class':txsubdo_dict[Imagee(*y.clip_by_shape(q)).get_mode_value()], 'dominant_soil_typological_unit_stu':wrbfu_dict[Imagee(*z.clip_by_shape(q)).get_mode_value()]   } } }),'parameters':['attributes_fid','feature_geometry', 'aglim1_image', 'aglim2_image', 'il_image', 'parmado_image', 'roo_image', 'txdepchg_image', 'txsrfdo_image', 'txsubdo_image', 'wrbfu_image']},
    'attributes_insert_statement':{'function':(lambda x, y,z: "INSERT INTO %s (fid,atts,dataset_fid) VALUES (%s,('%s'::json),%s)" % (x,y,json.dumps(z['atts']),z['dataset_id']) ),'parameters':['attributes_table','attributes_fid','attribute_properties']},\
    'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,3)" % (z,x,y) ),'parameters':['feature_id','attributes_fid','attributes2object_table']},\
    'two_insert_statements':{'function':(lambda y,z: [y,z]),'parameters':['attributes_insert_statement','attributes2object_insert_statement']},\
    }
    return o_dict

def geomorphology_transformations(ogr_olu_object, attributes_table, attributes2object_table, attributes_fid,  dem_im,  slope_im,  aspect_im,  twi_im):
    o_dict=\
    {
    'feature':{'object':ogr_olu_object},\
    'feature_json':{'function':(lambda y: json.loads(y.ExportToJson())),'parameters':['feature']},\
    'feature_geometry':{'function':(lambda y: y.GetGeometryRef().ExportToWkt() ),'parameters':['feature']},\
    'feature_id':{'function':(lambda y: int(y['id'])),'parameters':['feature_json']},\
    'attributes_table':{'object':attributes_table},\
    'attributes2object_table':{'object':attributes2object_table},\
    'attributes_fid':{'object':attributes_fid},\
    'dem_image':{'object':dem_im},\
    'slope_image':{'object':slope_im},\
    'azimuth_image':{'object':aspect_im},\
    'twi_image':{'object':twi_im},\
    'attribute_properties':{'function':(lambda u, v, w, x,y,z: {**{'fid':u},**{'dataset_id':7},**{'atts':{'elevation':Imagee(*w.clip_by_shape(v)).get_statistics()['mean'], 'slope':Imagee(*x.clip_by_shape(v)).get_statistics()['mean'], 'azimuth':Imagee(*y.clip_by_shape(v)).get_statistics()['mean'], 'twi':Imagee(*z.clip_by_shape(v)).get_statistics()['mean']} } }),'parameters':['attributes_fid','feature_geometry','dem_image', 'slope_image', 'azimuth_image', 'twi_image']},
    'attributes_insert_statement':{'function':(lambda x, y,z: "INSERT INTO %s (fid,atts,dataset_fid) VALUES (%s,('%s'::json),%s)" % (x,y,json.dumps(z['atts']),z['dataset_id']) ),'parameters':['attributes_table','attributes_fid','attribute_properties']},\
    'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,3)" % (z,x,y) ),'parameters':['feature_id','attributes_fid','attributes2object_table']},\
    'two_insert_statements':{'function':(lambda y,z: [y,z]),'parameters':['attributes_insert_statement','attributes2object_insert_statement']},\
    }
    return o_dict

# %%
olu_object_table='olu2.olu_object'
olu_attribute_set_table='olu2.olu_attribute_set'
olu_atts_to_object_table='olu2.atts_to_object'
for ku in find_neighbors_level(G.reverse(),'51',4):
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

# %%
olu_object_table='olu2.olu_object'
olu_attribute_set_table='olu2.olu_attribute_set'
olu_atts_to_object_table='olu2.atts_to_object'
for obec in find_neighbors_level(G.reverse(),'51',3):
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

# %%
conn_olu=ogr.Open("PG: host=localhost dbname=oluv2 user=... password=...")

# %%
aglim1_dict={0: 'No information', 1: 'No limitation to agricultural use', 2: 'Gravelly (over 35% gravel diameter < 7.5 cm)', 3: 'Stony (presence of stones diameter > 7.5 cm,impracticable mechanisation)', 4: 'Lithic (coherent and hard rock within 50 cm)', 5: 'Concretionary (over 35% concretions diameter < 7.5 cm near the surface)', 6: 'Petrocalcic (cemented or indurated calcic horizon within 100 cm)', 7: 'Saline (electric conductivity > 4 mS.cm-1 within 100 cm)', 8: 'Sodic (Na/T > 6% within 100 cm)', 9: 'Glaciers and snow-caps', 10: 'Soils disturbed by man (i.e. landfills, paved surfaces, mine spoils)', 11: 'Fragipans', 12: 'Excessively drained', 13: 'Almost always flooded', 14: 'Eroded phase, erosion', 15: 'Phreatic phase (shallow water table)', 18: 'Permafrost', 255: 'NoValue'}
aglim2_dict=aglim1_dict
il_dict={0: 'No information', 1: 'No impermeable layer within 150 cm', 2: 'Impermeable layer between 80 and 150 cm', 3: 'Impermeable layer between 40 and 80 cm', 4: 'Impermeable layer within 40 cm', 255: 'NoValue'}
parmado_dict={0: 'No information', 1000: 'consolidated-clastic-sedimentary rocks', 1110: 'conglomerate', 1120: 'breccia', 1200: 'psammite or arenite', 1210: 'sandstone', 1211: 'calcareous sandstone', 1212: 'ferruginous sandstone', 1213: 'clayey sandstone', 1214: 'quartzitiic sandstone/orthoquartzite', 1220: 'arkose', 1300: 'pelite, lutite or argilite', 1310: 'claystone/mudstone', 1320: 'siltstone', 1400: 'facies bound rock', 1410: 'flysch', 1420: 'molasse', 2100: 'calcareous rocks', 2110: 'limestone', 2111: 'hard limestone', 2112: 'soft limestone', 2113: 'marly limestone', 2114: 'chalky limestone', 2115: 'detrital limestone', 2120: 'dolomite', 2140: 'marl', 2142: 'gypsiferous marl', 2150: 'chalk', 3100: 'acid to intermediate plutonic rocks', 3110: 'granite', 3130: 'diorite', 3200: 'basic plutonic rocks', 3210: 'gabbro', 3400: 'acid to intermediate volcanic rocks', 3410: 'rhyolite', 3430: 'andesite', 3500: 'basic to ultrabasic volcanic rocks', 3510: 'basalt', 3700: 'pyroclastic rocks (tephra)', 3710: 'tuff/tuffstone', 4110: '(meta-)shale/argilite', 4120: 'slate', 4200: 'acid regional metamorphic rocks', 4230: 'micaschist', 4240: 'gneiss', 4310: 'greenschist', 4410: 'serpentinite', 4500: 'calcareous regional metamorphic rocks', 4700: 'tectogenetic metamorphism rocks or cataclasmic metamorphism', 5000: 'unconsolidated deposits (alluvium, weathering residuum and slope deposits)', 5100: 'marine and estuarine sands', 5121: 'holocene coastal sand with shells', 5200: 'marine and estuarine clays and silts', 5210: 'pre-quaternary clay and silt', 5211: 'tertiary clay', 5220: 'quaternary clay and silt', 5300: 'fluvial sands and gravels', 5310: 'river terrace sand or gravel', 5311: 'river terrace sand', 5400: 'fluvial clays, silts and loams', 5411: 'terrace clay and silt', 5420: 'river loam', 5431: 'floodplain clay and silt', 5500: 'lake deposits', 5600: 'residual and redeposited loams from silicate rocks', 5610: 'residual loam', 5611: 'stony loam', 5612: 'clayey loam', 5700: 'residual and redeposited clays from calcareous rocks', 5710: 'residual clay', 5711: 'clay with flints', 5712: 'ferruginous residual clay', 5713: 'calcareous clay', 5721: 'stony clay', 5800: 'slope deposits', 5820: 'colluvial deposit', 6000: 'unconsolidated glacial deposits/glacial drift', 6100: 'morainic deposits', 6111: 'boulder clay', 6200: 'glaciofluvial deposits', 6210: 'outwash sand, glacial sand', 7000: 'eolian deposits', 7100: 'loess', 7110: 'loamy loess', 7120: 'sandy loess', 7200: 'eolian sands', 7210: 'dune sand', 7220: 'cover sand', 8000: 'organic materials', 8100: 'peat (mires)', 8200: 'slime and ooze deposits', 255: 'NoValue'}
roo_dict={0: 'No information', 1: 'No obstacle to roots between 0 and 80 cm', 2: 'Obstacle to roots between 60 and 80 cm depth', 3: 'Obstacle to roots between 40 and 60 cm depth', 4: 'Obstacle to roots between 20 and 40 cm depth', 5: 'Obstacle to roots between 0 and 80 cm depth', 6: 'Obstacle to roots between 0 and 20 cm depth', 255: 'NoValue'}
txdepchg_dict={0: 'No information', 1: 'Textural change between 20 and 40 cm depth', 2: 'Textural change between 40 and 60 cm depth', 3: 'Textural change between 60 and 80 cm depth', 4: 'Textural change between 80 and 120 cm depth', 5: 'No textural change between 20 and 120 cm depth', 6: 'Textural change between 20 and 60 cm depth', 7: 'Textural change between 60 and 120 cm depth', 255: 'NoValue'}
txsrfdo_dict={0: 'No information', 9: 'No mineral texture (Peat soils)', 1: 'Coarse (18% < clay and > 65% sand)', 2: 'Medium (18% < clay < 35% and >= 15% sand, or 18% <clay and 15% < sand < 65%)', 3: 'Medium fine (< 35% clay and < 15% sand)', 4: 'Fine (35% < clay < 60%)', 5: 'Very fine (clay > 60 %)', 255: 'NoValue'}
txsubdo_dict={0: 'No information', 9: 'No mineral texture (Peat soils)', 1: 'Coarse (18% < clay and > 65% sand)', 2: 'Medium (18% < clay < 35% and >= 15% sand, or 18% <clay and 15% < sand < 65%)', 3: 'Medium fine (< 35% clay and < 15% sand)', 4: 'Fine (35% < clay < 60%)', 5: 'Very fine (clay > 60 %)', 255: 'NoValue'}
wrbfu_dict={2: 'Water body', 6: 'Umbric Cryosol', 7: 'Gelic Histosol', 8: 'Rock outcrops', 9: 'Glacier', 10: 'Histic Cryosol', 12: 'Humic Gleysol', 13: 'Rustic Podzol', 14: 'Sapric Histosol', 15: 'Fibric Histosol', 16: 'Turbic Cryosol', 17: 'Entic Podzol', 18: 'Histic Gleysol', 19: 'Lithic Leptosol', 20: 'Haplic Cryosol', 21: 'Dystric Fluvisol', 22: 'Thionic Fluvisol', 23: 'Cryic Histosol', 24: 'Carbic Podzol', 26: 'Histic Fluvisol', 27: 'Eutric Fluvisol', 28: 'Dystric Gleysol', 29: 'Dystric Cambisol', 30: 'Haplic Podzol', 31: 'Rendzic Leptosol', 32: 'Umbric Fluvisol', 33: 'Histic Albeluvisol', 35: 'Eutric Cambisol', 36: 'Gleyic Albeluvisol', 37: 'Gleyic Cambisol', 38: 'Protic Arenosol', 39: 'Endoeutric Albeluvisol', 42: 'Umbric Albeluvisol', 43: 'Sodic Gleysol', 44: 'Sodic Phaeozem', 45: 'Gleyic Phaeozem', 47: 'Chernic Chernozem', 48: 'Haplic Phaeozem', 49: 'Gleyic Umbrisol', 50: 'Humic Leptosol', 51: 'Humic Cambisol', 52: 'Luvic Phaeozem', 53: 'Haplic Kastanozem', 54: 'Gleyic Solonchak', 55: 'Luvic Chernozem', 56: 'Mollic Planosol', 57: 'Salic Fluvisol', 58: 'Albic Phaeozem', 59: 'Haplic Arenosol', 60: 'Mollic Leptosol', 61: 'Gleyic Solonetz', 62: 'Haplic Albeluvisol', 63: 'Glossic Chernozem', 64: 'Stagnic Albeluvisol', 65: 'Pellic Vertisol', 66: 'Calcic Chernozem', 67: 'Calcaric Fluvisol', 68: 'Haplic Calcisol', 69: 'Haplic Solonchak', 70: 'Calcic Kastanozem', 71: 'Albic Planosol', 72: 'Haplic Leptosol', 73: 'Haplic Solonetz', 74: 'Dystric Histosol', 75: 'Town', 76: 'Eutric Histosol', 77: 'Dystric Regosol', 78: 'Dystric Leptosol', 79: 'No information', 80: 'Umbric Leptosol', 81: 'Luvic Kastanozem', 82: 'Vertic Cambisol', 83: 'Placic Podzol', 84: 'Eutric Gleysol', 85: 'Umbric Podzol', 86: 'Calcaric Regosol', 87: 'Calcaric Cambisol', 88: 'Soil disturbed by man', 89: 'Gleyic Luvisol', 90: 'Calcic Luvisol', 91: 'Eutric Planosol', 92: 'Haplic Luvisol', 93: 'Gleyic Podzol', 94: 'Albic Arenosol', 95: 'Mollic Gleysol', 96: 'Mollic Fluvisol', 97: 'Dystric Planosol', 98: 'Calcaric Leptosol', 99: 'Calcaric Gleysol', 100: 'Thionic Gleysol', 101: 'Leptic Podzol', 102: 'Gleyic Fluvisol', 103: 'Arenic Luvisol', 104: 'Endosalic Calcisol', 105: 'Chromic Luvisol', 106: 'Chromic Cambisol', 107: 'Eutric Regosol', 108: 'Salic Histosol', 109: 'Haplic Gleysol', 110: 'Haplic Chernozem', 111: 'Albic Luvisol', 112: 'Dystric Luvisol', 113: 'Haplic Vertisol', 114: 'Calcaric Phaeozem', 115: 'Humic Andosol', 116: 'Mollic Solonetz', 117: 'Mollic Cambisol', 118: 'Haplic Acrisol', 119: 'Marsh', 120: 'Dystric Andosol', 121: 'Eutric Leptosol', 122: 'Arenic Umbrisol', 123: 'Chromic Vertisol', 124: 'Aridic Gypsisol', 125: 'Ferric Luvisol', 126: 'Aridic Calcisol', 127: 'Vertic Luvisol', 128: 'Gleyic Acrisol', 129: 'Solonchak', 255: 'NoValue'}

# %%
wrbfu_eu__metadata=MetaData('Dominant Soil Typological Unit (STU)', {"local":"/data/publish/jupyterhub-dpz/wrbfu/w001001x.adf", "format":"AIG"},'raster data')
wrbfu_eu_ds=ds_from_metadata(wrbfu_eu__metadata)
wrbfu_eu=GeoConcept('Dominant Soil Typological Unit (STU)','Dominant Soil Typological Unit (STU).', 'Feature',json_feature_with_raster_structure, data_source=wrbfu_eu_ds,subgeoconcepts=[])
wrbfu_eu.set_raster_output_backend('',wrbfu_eu.get_data_source().get_attributes()['local'])

# %%
re.sub("[^0-9a-zA-Z]+", "_", wrbfu_eu.get_name().lower())[:-1]

# %%
aglim1_eu__metadata=MetaData('Agricultural limitations (primary)', {"local":"/data/publish/jupyterhub-dpz/aglim1/w001001x.adf", "format":"AIG"},'raster data')
aglim1_eu_ds=ds_from_metadata(aglim1_eu__metadata)
aglim1_eu=GeoConcept('Agricultural limitations (primary)','Agricultural limitations (primary).', 'Feature',json_feature_with_raster_structure, data_source=aglim1_eu_ds,subgeoconcepts=[])
aglim1_eu.set_raster_output_backend('',aglim1_eu.get_data_source().get_attributes()['local'])

# %%
re.sub("[^0-9a-zA-Z]+", "_", aglim1_eu.get_name().lower())[:-1]

# %%
aglim2_eu__metadata=MetaData('Agricultural limitations (secondary)', {"local":"/data/publish/jupyterhub-dpz/aglim2/w001001x.adf", "format":"AIG"},'raster data')
aglim2_eu_ds=ds_from_metadata(aglim2_eu__metadata)
aglim2_eu=GeoConcept('Agricultural limitations (secondary)','Agricultural limitations (secondary) in EU', 'Feature',json_feature_with_raster_structure, data_source=aglim2_eu_ds,subgeoconcepts=[])
aglim2_eu.set_raster_output_backend('',aglim2_eu.get_data_source().get_attributes()['local'])

# %%
re.sub("[^0-9a-zA-Z]+", "_", aglim2_eu.get_name().lower())[:-1]

# %%
il_eu__metadata=MetaData('Soil impermeable layer depth', {"local":"/data/publish/jupyterhub-dpz/il/w001001x.adf", "format":"AIG"},'raster data')
il_eu_ds=ds_from_metadata(il_eu__metadata)
il_eu=GeoConcept('Soil impermeable layer depth','Soil impermeable layer depth.', 'Feature',json_feature_with_raster_structure, data_source=il_eu_ds,subgeoconcepts=[])
il_eu.set_raster_output_backend('',il_eu.get_data_source().get_attributes()['local'])

# %%
re.sub("[^0-9a-zA-Z]+", "_", il_eu.get_name().lower())

# %%
parmado_eu__metadata=MetaData('Soil parent material', {"local":"/data/publish/jupyterhub-dpz/parmado/w001001x.adf", "format":"AIG"},'raster data')
parmado_eu_ds=ds_from_metadata(parmado_eu__metadata)
parmado_eu=GeoConcept('Soil parent material','Soil parent material.', 'Feature',json_feature_with_raster_structure, data_source=parmado_eu_ds,subgeoconcepts=[])
parmado_eu.set_raster_output_backend('',parmado_eu.get_data_source().get_attributes()['local'])

# %%
re.sub("[^0-9a-zA-Z]+", "_", parmado_eu.get_name().lower())

# %%
roo_eu__metadata=MetaData('Obstacles to roots', {"local":"/data/publish/jupyterhub-dpz/roo/w001001x.adf", "format":"AIG"},'raster data')
roo_eu_ds=ds_from_metadata(roo_eu__metadata)
roo_eu=GeoConcept('Obstacles to roots','Obstacles to roots.', 'Feature',json_feature_with_raster_structure, data_source=roo_eu_ds,subgeoconcepts=[])
roo_eu.set_raster_output_backend('',roo_eu.get_data_source().get_attributes()['local'])

# %%
re.sub("[^0-9a-zA-Z]+", "_", roo_eu.get_name().lower())

# %%
txdepchg_eu__metadata=MetaData('Textural change depth', {"local":"/data/publish/jupyterhub-dpz/txdepchg/w001001x.adf", "format":"AIG"},'raster data')
txdepchg_eu_ds=ds_from_metadata(txdepchg_eu__metadata)
txdepchg_eu=GeoConcept('Textural change depth','Textural change depth.', 'Feature',json_feature_with_raster_structure, data_source=txdepchg_eu_ds,subgeoconcepts=[])
txdepchg_eu.set_raster_output_backend('',txdepchg_eu.get_data_source().get_attributes()['local'])

# %%
re.sub("[^0-9a-zA-Z]+", "_", txdepchg_eu.get_name().lower())

# %%
txsrfdo_eu__metadata=MetaData('Dominant surface textural class', {"local":"/data/publish/jupyterhub-dpz/txsrfdo/w001001x.adf", "format":"AIG"},'raster data')
txsrfdo_eu_ds=ds_from_metadata(txsrfdo_eu__metadata)
txsrfdo_eu=GeoConcept('Dominant surface textural class','Dominant surface textural class.', 'Feature',json_feature_with_raster_structure, data_source=txsrfdo_eu_ds,subgeoconcepts=[])
txsrfdo_eu.set_raster_output_backend('',txsrfdo_eu.get_data_source().get_attributes()['local'])

# %%
re.sub("[^0-9a-zA-Z]+", "_", txsrfdo_eu.get_name().lower())

# %%
txsubdo_eu__metadata=MetaData('Dominant sub-surface textural class', {"local":"/data/publish/jupyterhub-dpz/txsubdo/w001001x.adf", "format":"AIG"},'raster data')
txsubdo_eu_ds=ds_from_metadata(txsubdo_eu__metadata)
txsubdo_eu=GeoConcept('Dominant sub-surface textural class','Dominant sub-surface textural class.', 'Feature',json_feature_with_raster_structure, data_source=txsubdo_eu_ds,subgeoconcepts=[])
txsubdo_eu.set_raster_output_backend('',txsubdo_eu.get_data_source().get_attributes()['local'])

# %%


# %%
eudem_cz__metadata=MetaData('EU DEM in Czech Republic', {"local":"/home/jupyter-dima/eu_dem_czat3035.tif", "format":"GTiff"},'raster data')
eudem_cz_ds=ds_from_metadata(eudem_cz__metadata)
eudem_cz=GeoConcept('EU DEM in Czech Republic','EU DEM in Czech Republic.', 'Feature',json_feature_with_raster_structure, data_source=eudem_cz_ds,subgeoconcepts=[])
eudem_cz.set_raster_output_backend('',eudem_cz.get_data_source().get_attributes()['local'])

# %%
re.sub("[^0-9a-zA-Z]+", "_", txsubdo_eu.get_name().lower())

# %%
#bbox_r=(4467286, 3048027, 4560407 , 2972053)
bbox_r=(4441152,3062184, 4568221, 2945396)

# %%
eudem_im=eudem_cz.read_raster_output_backend(1,bbox_r)

# %%
eudem_im.get_data().shape

# %%
eudem_im.export_as_tif('kk_dem.tif')

# %%
im_slope=Imagee(*eudem_im.calculate_slope())

# %%
im_azimuth=Imagee(*eudem_im.calculate_azimuth(exact=False))

# %%
im_twi=get_twi(eudem_im)

# %%
im_twi.get_data().shape

# %%
wrbfu_im=wrbfu_eu.read_raster_output_backend(1,bbox_r)

# %%
aglim1_im=aglim1_eu.read_raster_output_backend(1,bbox_r)

# %%
aglim2_im=aglim2_eu.read_raster_output_backend(1,bbox_r)

# %%
il_im=il_eu.read_raster_output_backend(1,bbox_r)

# %%
parmado_im=parmado_eu.read_raster_output_backend(1,bbox_r)

# %%
roo_im=roo_eu.read_raster_output_backend(1,bbox_r)

# %%
txdepchg_im=txdepchg_eu.read_raster_output_backend(1,bbox_r)

# %%
txsrfdo_im=txsrfdo_eu.read_raster_output_backend(1,bbox_r)

# %%
txsubdo_im=txsubdo_eu.read_raster_output_backend(1,bbox_r)

# %%
lyr=conn_olu.GetLayerByName('olu2.olu_object')

# %%
lyr.GetFeatureCount()

# %%
lyr.ResetReading()

# %%
olu_conn.rollback()

# %%
#radok 16
olu_cur.execute('SELECT max(fid) FROM olu2.olu_attribute_set')
max_value=olu_cur.fetchone()[0]
max_value=max_value if max_value is not None else 1
olu_cur.execute("select setval('olu2.olu_attributes_fid_seq', %s)" % max_value)
olu_conn.commit()
olu_cur.execute('SELECT last_value FROM olu2.olu_attributes_fid_seq')
olu_atts_id_gen=sequence_generator(olu_cur.fetchone()[0])

# %%
f=lyr.GetFeature(9851)

# %%
f_geom=f.GetGeometryRef()

# %%
f_geom.IsEmpty()

# %%
f_geom.Transform(wgs84_to_etrs3035)

# %%
parmado_dict[Imagee(*parmado_im.clip_by_shape(f_geom.ExportToWkt())).get_mode_value()]

# %%


# %%
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
        lyr_f_im=Imagee(*im.clip_by_shape(lyr_f.GetGeometryRef().ExportToWkt()))
        [olu_cur.execute(i) for i in apply_function(esdr_full_transformations(lyr_f,attributes_table,attributes2object_table,attributes_fid,aglim1_im, aglim2_im, il_im, parmado_im,  roo_im,  txdepchg_im, txsrfdo_im,  txsubdo_im,  wrbfu_im),'two_insert_statements')]
        count+=1
        if count%10000==0:
            olu_conn.commit()
            count=0
    except:
        bad_list.append(lyr_f.GetFID())
        print(lyr_f.GetFID())
        continue

# %%
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
        lyr_f_im=Imagee(*im.clip_by_shape(lyr_f.GetGeometryRef().ExportToWkt()))
        [olu_cur.execute(i) for i in apply_function(geomorphology_transformations(lyr_f,attributes_table,attributes2object_table,attributes_fid,eudem_im,im_slope,im_azimuth,im_twi),'two_insert_statements')]
        count+=1
        if count%10000==0:
            olu_conn.commit()
            count=0
    except:
        bad_list.append(lyr_f.GetFID())
        print(lyr_f.GetFID())
        continue

# %%
olu_conn.commit()

# %%
len(bad_list)

# %%
olu_conn.commit()

# %%
np.savetxt("bad_list_2.txt", np.array(bad_list), fmt="%s")

# %%
im_twi.export_as_tif('kk_twi.tif')

# %%
count=0
for c in range(lyr.GetFeatureCount()):
    lyr_f=lyr.GetNextFeature()
    lyr_f.GetGeometryRef().Transform(wgs84_to_etrs3035)
    attributes_fid=next(olu_atts_id_gen)
    attributes_table='olu2.olu_attribute_set'
    attributes2object_table='olu2.atts_to_object'
    try:
        lyr_f_im=Imagee(*im.clip_by_shape(lyr_f.GetGeometryRef().ExportToWkt()))
        [olu_cur.execute(i) for i in apply_function(wrbfu_transformations(lyr_f,attributes_table,attributes2object_table,attributes_fid,im),'two_insert_statements')]
        count+=1
        if count%1000==0:
            olu_conn.commit()
            count=0
    except:
        print(lyr_f.GetFID())
        continue

# %%
f=lyr.GetNextFeature()

# %%
f.GetGeometryRef().Transform(wgs84_to_etrs3035)

# %%
f_im=Imagee(*im.clip_by_shape(f.GetGeometryRef().ExportToWkt()))

# %%
f_im.get_mode_value()

# %%
from scipy.stats import mode

# %%
wrbfu_dict[mode(f_im.get_data())[0][0][0]]

# %%
ku=list(find_neighbors_level(G.reverse(),'51',4))[0]

# %%
admunit_id=list(find_neighbors_level(G,ku,3))[0]

# %%
sub=lpis_cz.get_subgeoconcept_by_adm_node(cislo_ku)

# %%
list(find_neighbors_level(G.reverse(),'51',4))[0]

# %%
lpis_cz.get_subgeoconcept_by_adm_node('672289')

# %%
olu_conn.commit()

# %%
json.dumps({str(k):int(v) for k,v in dict(zip(*np.histogram(lyr_f_im.get_data())[::-1])).items() if v>0})

# %%
np.histogram(lyr_f_im.get_data())[::-1]

# %%
np.nanstd(lyr_f_im.get_data())

# %%
im_slope.get_statistics(full=True)

# %%
np.histogram((aglim1_im.get_data()[np.where(aglim1_im.get_data()!=aglim1_im.get_metadata()['nodata'])]))

# %%
wrbfu_im.get_statistics(categoric=True,full=True)

# %%
im_azimuth.get_statistics(categoric=True,full=True)

# %%
im_slope.get_statistics(full=True)

# %%
values,counts=np.unique((aglim1_im.get_data()[np.where(aglim1_im.get_data()!=aglim1_im.get_metadata()['nodata'])]), return_counts=True)

# %%
{str(k):v for k,v in dict(zip(values.data,counts)).items() if k!=aglim1_im.get_metadata()['nodata']}

# %%
np.ma.masked

# %%
dict(zip)

# %%
def create_raster_atts(geoconcept,ogr_feature):
    dict={}
    for sub in geoconcept.get_subgeoconcepts():
        
        dict={**dict,{sub.get_name():sub.get_statistics()}}
        

# %%
full,categoric=False,False

# %%
if full is False and categoric is False:
    print(1)

# %%
