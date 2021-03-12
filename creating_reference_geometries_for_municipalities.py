# %%
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

# %%
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

dbs_urbanatlas_connection={**{'dbname':'ua_cz'},**connectionstring_localhost}
dbs_urbanatlas=DBStorage(dbs_urbanatlas_connection)
dbs_urbanatlas.connect()
dbs_urbanatlas.disconnect()
dbs_urbanatlas.connect()

dbs_corine_connection={**{'dbname':'corine_cz'},**connectionstring_localhost}
dbs_corine=DBStorage(dbs_corine_connection)
dbs_corine.connect()
dbs_corine.disconnect()
dbs_corine.connect()

dbs_olu_connection={**{'dbname':'oluv20'},**connectionstring_localhost}
dbs_olu=DBStorage(dbs_olu_connection)
dbs_olu.connect()
dbs_olu.disconnect()
dbs_olu.connect()

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

ua_cz__metadata=MetaData('Urban Atlas in Czech Republic',
                              {"url":"https://land.copernicus.eu/local/urban-atlas/urban-atlas-2012?tab=download",
                               "format":"GPKG", "compression":"zip", "downloadable":"on_registration"},'data')

corine_cz__metadata=MetaData('CORINE land cover in Czech Republic',
                              [{"url":"https://land.copernicus.eu/pan-european/corine-land-cover/",
                               "format":"GPKG", "compression":"zip", "downloadable":"on_registration"},{"local":"/home/jupyter-dima/corine_cz/corine_cz.shp",
                               "format":"ESRI Shapefile"}],'data')

# %%
admunit_cz__ds=ds_from_metadata(admunit_cz__metadata)

ruian_cz__ds=ds_from_metadata(ruian_cz__metadata)
ruian_cz__ds.set_attributes({**ruian_cz__ds.get_attributes(),**{'layer':'Parcely'}})

lpis_cz__ds_xml=ds_from_metadata(lpis_cz__metadata,format='XML')

ua_cz__ds=ds_from_metadata(ua_cz__metadata)
ua_cz__ds.set_attributes({**ua_cz__ds.get_attributes(),**{'layer':'{compilable:ua_gpkg_layer}'}})

corine_cz__ds=ds_from_metadata(corine_cz__metadata,format='ESRI Shapefile')

# %%
admunit_cz=GeoConcept('Administrative units in Czech Republic','Administrative units in Czech Republic. All levels.',
                      'AdmUnitFeature',json_admin_unit_structure, data_source=admunit_cz__ds, subgeoconcepts=[] )

ruian_parcely_cz=GeoConcept('Land parcels in Czech Republic','Digital land parcels (parcely) in Czech Republic.',
                      'FeatureWithID',json_feature_with_bigid_structure, data_source=ruian_cz__ds,subgeoconcepts=[],adm_graph_node='1')

lpis_cz=GeoConcept('LPIS in Czech Republic','LPIS in Czech Republic. All levels.',
                      'FeatureWithID',json_feature_structure, data_source=lpis_cz__ds_xml, subgeoconcepts=[], adm_graph_node='1')

corine_cz=GeoConcept('CORINE land cover in Czech Republic','CORINE land cover in Czech Republic in years 1990 - 2018.',
                      'FeatureWithID',json_feature_structure, subgeoconcepts=[], data_source=corine_cz__ds)

ua_cz=GeoConcept('Urban Atlas in Czech Republic','Urban Atlas in Czech Republic in 2012.',
                      'FeatureWithID',json_feature_structure, subgeoconcepts=[], data_source=ua_cz__ds)

# %%
if 'local' in ruian_parcely_cz.get_data_source().get_attributes():
    ruian_parcely_cz.get_data_source().set_data_file(ruian_parcely_cz.get_data_source().get_attributes()['local']) 

if 'local' in corine_cz.get_data_source().get_attributes():
    corine_cz.get_data_source().set_data_file(corine_cz.get_data_source().get_attributes()['local'])

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

ua_cz.create_table(dbs_urbanatlas,  name='default',scheme='public',conflict='append',adm_graph_node='1')

corine_cz.create_table(dbs_corine,  name='cz',scheme='public',conflict='append',adm_graph_node='1')

# %%
#admunit_cz.get_table().get_scheme()+'.'+admunit_cz.get_table().get_name()
#ruian_parcely_cz.get_table().get_scheme()+'.'+ruian_parcely_cz.get_table().get_name()
#lpis_cz.get_table().get_scheme()+'.'+lpis_cz.get_table().get_name()
#ua_cz.get_table().get_scheme()+'.'+ua_cz.get_table().get_name()
#corine_cz.get_table().get_scheme()+'.'+corine_cz.get_table().get_name()
#ruian_parcely_cz.get_subgeoconcepts()[0].get_table().get_scheme()+'.'+ruian_parcely_cz.get_subgeoconcepts()[0].get_table().get_name()

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
            ruian_parcely_cz.append_subgeoconcept(SubGeoConcept(str(j),'RUIAN land parcels in Czech administrative territorial unit %s ' % str(j),'FeatureWithID',ruian_parcely_cz.get_attributes(),data_source=DataSource(ruian_parcely_cz.get_data_source().get_type(),ruian_parcely_cz.get_data_source().get_name(),(dict(ruian_parcely_cz.get_data_source().get_attributes(),**{'url':ruian_parcely_cz.get_data_source().get_attributes()['url'].replace(i,str(j))})),None,None),supergeoconcept=ruian_parcely_cz,table_inheritance=True,subgeoconcepts=[]))


# %%
for i in re.findall('\{.*?\}',lpis_cz.get_data_source().get_attributes()['url']): 
    if i[1:-1] in list(compilable_tree_dictionary(admunit_cz).keys()):
        for j in apply_function(compilable_tree_dictionary(admunit_cz),i[1:-1]):
            lpis_cz.append_subgeoconcept(SubGeoConcept(str(j),'LPIS in Czech administrative territorial unit %s ' % str(j),'FeatureWithID',lpis_cz.get_attributes(),data_source=DataSource(lpis_cz.get_data_source().get_type(),lpis_cz.get_data_source().get_name(),(dict(lpis_cz.get_data_source().get_attributes(),**{'url':lpis_cz.get_data_source().get_attributes()['url'].replace(i,str(j))})),None,None),supergeoconcept=lpis_cz,table_inheritance=True,subgeoconcepts=[],type='spatial:admin',adm_graph_node=str(j)))

# %%
concept_list=[re.search('_.*_',file)[0][1:-1] for file in os.listdir('.') if file.endswith('.zip') and file.startswith('CZ')]
for concept in concept_list:
    for i in re.findall('\{.*?\}',ua_cz.get_data_source().get_attributes()['layer']): 
        if i.split(':')[1][:-1] in list((compilable_ua_dictionary('')).keys()):
            geoconcept=SubGeoConcept('%s' % concept,'Urban Atlas in agglomeration %s ' % str(concept),'FeatureWithID',ua_cz.get_attributes(),data_source=DataSource(ua_cz.get_data_source().get_type(), ua_cz.get_data_source().get_name(),(ua_cz.get_data_source().get_attributes()),None,None),supergeoconcept=ua_cz,table_inheritance=True,subgeoconcepts=[])
            files=unzip_file([file for file in os.listdir('.') if file.endswith('.zip') and file.startswith('CZ') and re.match('^.*%s.*$' % concept,file)][0],'all',os.getcwd())
            geoconcept.get_data_source().set_data_file([file for file in files if file.endswith('gpkg')][0])
            geoconcept.get_data_source().set_attribute({'layer':apply_function(compilable_ua_dictionary(geoconcept),i.split(':')[1][:-1])})
            ua_cz.append_subgeoconcept(geoconcept)

# %%
corine_cz__years=[1990,2000,2006,2012,2018]

for corine_year in corine_cz__years:
    data_source=copy(corine_cz.get_data_source())
    data_source.set_attribute({'attribute_filter':('year=\'%s\'' % corine_year)})
    corine_cz.append_subgeoconcept(SubGeoConcept(str(corine_year),str(corine_year),'FeatureWithID',corine_cz.get_attributes(),data_source=data_source,supergeoconcept=corine_cz,table_inheritance=True,type='semantic',subgeoconcepts=[]))

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
for concept in ua_cz.get_subgeoconcepts():
    concept.create_table(dbs_urbanatlas,name='default',scheme='data',conflict='append')

# %%
for sub in corine_cz.get_subgeoconcepts():
    print(sub.get_name())
    sub.create_table(dbs_corine,name=sub.get_name(),scheme='data',conflict='append',adm_graph_node='1')
    #s.set_table(View(transform_name_to_postgresql_format(s.get_name()), s.get_attributes(), s.get_supergeoconcept().get_table(), "dictionary_select5((data->'PREKRYVKATUZE')::json, 'KUKOD == \"%s\" AND PLATNOSTDO>\"[aktualni_cas]\"')::jsonb not in ('[]'::jsonb,'{}'::jsonb)" % s.get_name(), dbs=dbs_lpis_cz, scheme='data',type='usual'))
    #dbs_lpis_cz.execute(s.get_table().create_script())
    #sub.create_table(dbs_olu,name='default',scheme='corine',conflict='replace',adm_graph_node=sub.get_adm_graph_node())

# %%
#for sub in corine_cz.get_subgeoconcepts():
#    features=sub.get_data_source().read_features('feature', number=50)
#    dbs_corine.insert_many('insert into %s.%s (geom,data) ' % (sub.get_table().get_scheme(),sub.get_table().get_name()) ,features,50)

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
olu_id_gen=sequence_generator(dbs_olu.execute('SELECT last_value FROM olu2.olu_object_fid_seq')[0][0])

# %%
olu_atts_id_gen=sequence_generator(dbs_olu.execute('SELECT last_value FROM olu2.olu_attributes_fid_seq')[0][0])

# %%
dbs_olu.execute("delete from olu2.origin_metadata")
dbs_olu.execute("delete from olu2.origin_dataset")

# %%
dbs_olu.execute("delete from olu2.olu_attribute_set")
dbs_olu.execute("delete from olu2.atts_to_object")
dbs_olu.execute("delete from olu2.olu_object")

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
     'object_properties':{'function':(lambda x,y,z: {**{'fid':x},**{'dataset_id':1},**{'z_value':100000},**{'admunit_id':y},**{'valid_from':z.get_data()['PLATNOSTOD'],'valid_to':str(datetime.date(datetime.date.today().year+100,datetime.date.today().month,datetime.date.today().day))} }),'parameters':['object_id','admunit_id','feature']},
     'attributes_properties':{'function':(lambda x,y: {**{'fid':x},**{'hilucs_value':lpis_hilucs[y.get_data()['KULTURAID']]},**{'clc_value':lpis_clc[y.get_data()['KULTURAID']]},**{'atts':y.get_data()},**{'dataset_id':1}}),'parameters':['attributes_id','feature']},
     'object_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,dataset_fid,z_value,geom,valid_from,valid_to,admunit_id) VALUES (%s,%s,%s,ST_SetSRID(ST_Multi(ST_GeomFromText('%s')),4326),'%s','%s','%s')" % (w,x,z['dataset_id'],z['z_value'],y,z['valid_from'], z['valid_to'],z['admunit_id'] ) ),'parameters':['object_table_name','object_id','object_geom','object_properties']},\
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
     'object_properties':{'function':(lambda x,y,z: {**{'fid':x},**{'dataset_id':2},**{'z_value':10000},**{'admunit_id':y},**{'valid_from':z.get_data()['PlatiOd'],'valid_to':(z.get_data()['PlatiOd'] if z.get_data()['PlatiOd'] is not None else str(datetime.date(datetime.date.today().year+100,datetime.date.today().month,datetime.date.today().day)))}}),'parameters':['object_id','admunit_id','feature']},
     'attributes_properties':{'function':(lambda x,y: {**{'fid':x},**{'hilucs_value':ruian_zpv_hilucs[str(y.get_data()['ZpusobyVyuzitiPozemku'])] if y.get_data()['ZpusobyVyuzitiPozemku'] is not None else ruian_dp_hilucs[str(y.get_data()['DruhPozemkuKod'])]},**{'clc_value':ruian_zpv_clc[str(y.get_data()['ZpusobyVyuzitiPozemku'])] if y.get_data()['ZpusobyVyuzitiPozemku'] is not None else ruian_dp_clc[str(y.get_data()['DruhPozemkuKod'])]},**{'atts':y.get_data()},**{'dataset_id':2}}),'parameters':['attributes_id','feature']},
     'object_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,dataset_fid,z_value,geom,valid_from,valid_to,admunit_id) VALUES (%s,%s,%s,ST_SetSRID(ST_Multi(ST_GeomFromText('%s')),4326),'%s','%s','%s')" % (w,x,z['dataset_id'],z['z_value'],y,z['valid_from'], z['valid_to'],z['admunit_id'] ) ),'parameters':['object_table_name','object_id','object_geom','object_properties']},\
     'attributes_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,hilucs_id,atts,dataset_fid,clc_id) VALUES (%s,%s,('%s'::json),%s,%s)" % (w,x,z['hilucs_value'],json.dumps(y.get_data()),z['dataset_id'],z['clc_value']) ),'parameters':['attributes_table_name','attributes_id','feature','attributes_properties']},\
     'attribute2originvalue_insert_statement':{'function':(lambda x,y: "INSERT INTO olu2.attribute_origin_value (atts_origin_id,origin_name,description) VALUES (%s,%s,'%s')" % (x,y['dataset_id'],'original attributes') ),'parameters':['attributes_id','attributes_properties']},\
     'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,1)" % (z,x,y) ),'parameters':['object_id','attributes_id','attributes2object_table_name']},\
     'object2admunit_insert_statement':{'function':(lambda x,y: "INSERT INTO olu2.olu_object_to_admin_unit (object_fid,unit_fid) VALUES (%s,'%s')" % (x,y) ),'parameters':['object_id','admunit_id']},\
     'three_insert_statements':{'function':(lambda x,y,z: [x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement']},\
     'four_insert_statements':{'function':(lambda w,x,y,z: [w,x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement','object2admunit_insert_statement']},\
    }
    return o_dict

def ua_transformations(object,object_fid,attributes_fid,admunit_id,object_table_name,attributes_table_name,attributes2object_table_name): 
    o_dict=\
    {'feature':{'object':object},\
     'object_id':{'object':object_fid},\
     'attributes_id':{'object':attributes_fid},\
     'admunit_id':{'object':admunit_id},\
     'object_table_name':{'object':object_table_name},\
     'attributes_table_name':{'object':attributes_table_name},\
     'attributes2object_table_name':{'object':attributes2object_table_name},\
     'object_geom':{'function':(lambda y: y.get_geometry()),'parameters':['feature']},\
     'object_properties':{'function':(lambda x,y,z: {**{'fid':x},**{'dataset_id':3},**{'z_value':1000},**{'admunit_id':y},**{'valid_from':'2018-01-01','valid_to':'2024-01-01'} }),'parameters':['object_id','admunit_id','feature']},
     'attributes_properties':{'function':(lambda x,y: {**{'fid':x},**{'hilucs_value':ua_hilucs[y.get_data()['CODE2012']]},**{'clc_value':ua_clc[y.get_data()['CODE2012']]},**{'atts':y.get_data()},**{'dataset_id':3}}),'parameters':['attributes_id','feature']},
     'object_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,dataset_fid,z_value,geom,valid_from,valid_to,admunit_id) VALUES (%s,%s,%s,ST_SetSRID(ST_Multi(ST_GeomFromText('%s')),4326),'%s','%s','%s')" % (w,x,z['dataset_id'],z['z_value'],y,z['valid_from'], z['valid_to'],z['admunit_id'] ) ),'parameters':['object_table_name','object_id','object_geom','object_properties']},\
     'attributes_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,hilucs_id,atts,dataset_fid,clc_id) VALUES (%s,%s,('%s'::json),%s,%s)" % (w, x,z['hilucs_value'],json.dumps(y.get_data()),z['dataset_id'],z['clc_value']) ),'parameters':['attributes_table_name','attributes_id','feature','attributes_properties']},\
     'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,1)" % (z,x,y) ),'parameters':['object_id','attributes_id','attributes2object_table_name']},\
     'object2admunit_insert_statement':{'function':(lambda x,y: "INSERT INTO olu2.olu_object_to_admin_unit (object_fid,unit_fid) VALUES (%s,'%s')" % (x,y) ),'parameters':['object_id','admunit_id']},\
     'three_insert_statements':{'function':(lambda x,y,z: [x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement']},\
     'four_insert_statements':{'function':(lambda w,x,y,z: [w,x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement','object2admunit_insert_statement']},\
    }
    return o_dict

def corine_transformations(object,object_fid,attributes_fid,admunit_id,object_table_name,attributes_table_name,attributes2object_table_name): 
    o_dict=\
    {'feature':{'object':object},\
     'object_id':{'object':object_fid},\
     'attributes_id':{'object':attributes_fid},\
     'admunit_id':{'object':admunit_id},\
     'object_table_name':{'object':object_table_name},\
     'attributes_table_name':{'object':attributes_table_name},\
     'attributes2object_table_name':{'object':attributes2object_table_name},\
     'object_geom':{'function':(lambda y: y.get_geometry()),'parameters':['feature']},\
     'object_properties':{'function':(lambda x,y,z: {**{'fid':x},**{'dataset_id':4},**{'z_value':100},**{'admunit_id':y},**{'valid_from':'2018-01-01','valid_to':'2024-01-01'} }),'parameters':['object_id','admunit_id','feature']},
     'attributes_properties':{'function':(lambda x,y: {**{'fid':x},**{'hilucs_value':corine_hilucs[y.get_data()['clc_code']]},**{'clc_value':y.get_data()['clc_code']},**{'atts':y.get_data()},**{'dataset_id':4}}),'parameters':['attributes_id','feature']},
     'object_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,dataset_fid,z_value,geom,valid_from,valid_to,admunit_id) VALUES (%s,%s,%s,ST_SetSRID(ST_Multi(ST_GeomFromText('%s')),4326),'%s','%s','%s')" % (w,x,z['dataset_id'],z['z_value'],y,z['valid_from'], z['valid_to'],z['admunit_id'] ) ),'parameters':['object_table_name','object_id','object_geom','object_properties']},\
     'attributes_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,hilucs_id,atts,dataset_fid,clc_id) VALUES (%s,%s,('%s'::json),%s,%s)" % (w, x,z['hilucs_value'],json.dumps(y.get_data()),z['dataset_id'],z['clc_value']) ),'parameters':['attributes_table_name','attributes_id','feature','attributes_properties']},\
     'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,1)" % (z,x,y) ),'parameters':['object_id','attributes_id','attributes2object_table_name']},\
     'object2admunit_insert_statement':{'function':(lambda x,y: "INSERT INTO olu2.olu_object_to_admin_unit (object_fid,unit_fid) VALUES (%s,'%s')" % (x,y) ),'parameters':['object_id','admunit_id']},\
     'three_insert_statements':{'function':(lambda x,y,z: [x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement']},\
     'four_insert_statements':{'function':(lambda w,x,y,z: [w,x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement','object2admunit_insert_statement']},\
    }
    return o_dict

# %%
f_gen=lpis_cz.get_subgeoconcepts()[0].read_features_from_table(1)
f=next(f_gen)[0]

# %%
dbs_olu.execute("insert into olu2.origin_metadata (fid,metadata,origin_name,valid_from,valid_to,origin_type,column_names) VALUES (1,'metadata content','lpis','2021-02-04','2021-02-05','vector','%s'::json)" % (json.dumps({'column_names':list(f.get_data().keys())})))
dbs_olu.execute("insert into olu2.origin_dataset (fid, metadata_fid ,uri, dataset_type) VALUES (1,1,'lpis/dpb',1)")

# %%
f_gen=ruian_parcely_cz.get_subgeoconcepts()[0].read_features_from_table(1)
f=next(f_gen)[0]

# %%
dbs_olu.execute("insert into olu2.origin_metadata (fid,metadata,origin_name,valid_from,valid_to,origin_type,column_names) VALUES (2,'metadata content','ruian','2021-02-04','2021-02-05','vector','%s'::json)" % (json.dumps({'column_names':list(f.get_data().keys())})))
dbs_olu.execute("insert into olu2.origin_dataset (fid, metadata_fid ,uri, dataset_type) VALUES (2,2,'ruian/parcely',1)")

# %%
f_gen=ua_cz.get_subgeoconcepts()[0].read_features_from_table(1)
f=next(f_gen)[0]

# %%
dbs_olu.execute("insert into olu2.origin_metadata (fid,metadata,origin_name,valid_from,valid_to,origin_type,column_names) VALUES (3,'metadata content','urban atlas','2018-01-01','2024-01-01','vector','%s'::json)" % (json.dumps({'column_names':list(f.get_data().keys())})))
dbs_olu.execute("insert into olu2.origin_dataset (fid, metadata_fid ,uri, dataset_type) VALUES (3,3,'urban atlas',1)")

# %%
f_gen=corine_cz.get_subgeoconcept_by_name('2018').read_features_from_table(1)
f=next(f_gen)[0]

# %%
dbs_olu.execute("insert into olu2.origin_metadata (fid,metadata,origin_name,valid_from,valid_to,origin_type,column_names) VALUES (4,'metadata content','corine land cover 2018','2018-01-01','2024-01-01','vector','%s'::json)" % (json.dumps({'column_names':list(f.get_data().keys())})))
dbs_olu.execute("insert into olu2.origin_dataset (fid, metadata_fid ,uri, dataset_type) VALUES (4,4,'clc2018',1)")

del(f_gen,f)

# %%
def generate_olu_reference_geometries(kod_obce, theme, generate_inherited_tables=False):
    if generate_inherited_tables==True:
        dbs_olu.execute('create table if not exists olu_object.%s () inherits (olu2.olu_object)' % transform_name_to_postgresql_format(kod_obce))
        dbs_olu.execute('create table if not exists olu_attribute_set.%s () inherits (olu2.olu_attribute_set)' % transform_name_to_postgresql_format(kod_obce))
        dbs_olu.execute('create table if not exists atts_to_object.%s () inherits (olu2.atts_to_object)' % transform_name_to_postgresql_format(kod_obce))
        olu_object_table='olu_object.%s' % transform_name_to_postgresql_format(kod_obce)
        olu_attribute_set_table='olu_attribute_set.%s' % transform_name_to_postgresql_format(kod_obce)
        olu_atts_to_object_table='atts_to_object.%s' % transform_name_to_postgresql_format(kod_obce)
    olu_objects_union_cascaded=ogr.CreateGeometryFromWkt('MULTIPOLYGON EMPTY')
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
                        olu_id=next(olu_id_gen)
                        atts_id=next(olu_atts_id_gen)
                        [dbs_olu.execute(i) for i in apply_function(lpis_transformations(feature,olu_id,atts_id,kod_obce,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table),'three_insert_statements')]
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

kod_obce='593516'

generate_olu_reference_geometries(kod_obce,'agriculture',True)
