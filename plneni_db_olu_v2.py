import os
#import tridy
#from tridy import Theme,Table, View,  Feature, FeatureWithID,  AdmUnitFeature, FeatureWithRasterMap, OLUFeature, Grid, Imagee,  xml_lpis_cz_reader, get_listvalues_from_generator, find_neighbors_till, connection_parameters_to_pg, transform_name_to_postgresql_format, world_to_pixel 
from tridy import GeoConcept, SubGeoConcept, MetaData, DBStorage, DataSource, View,  ds_from_metadata, lpis_cz__posledni_aktualizace, apply_function, select_nodes_from_graph, unzip_file
#from importlib import reload
import requests
import datetime
import re
#from io import BytesIO

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

#from osgeo import ogr, osr, gdal
#import networkx as nx
#import numpy as np
import json
#import binascii
#import copy
#import time

#from lxml import etree

#from PIL import Image
#import matplotlib.pyplot as plt
#from matplotlib.pyplot import imshow

from credentials import connectionstring_localhost

#from importlib import reload

#from psycopg2.extensions import AsIs

#del(tridy)
#del(Theme,FeatureWithID,SubGeoConcept)
#import tridy
#from tridy import Theme,FeatureWithID,SubGeoConcept


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
    
#for the case when data has to be downloaded externally initialization of requests session variable with setting of number of retries
s = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
s.mount('http://', HTTPAdapter(max_retries=retries))

replacement_dictionary = {"[posledni_den_mesice]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d'),"[lpis_cz__posledni_aktualizace]":lpis_cz__posledni_aktualizace().strftime('%Y%m%d'), "[vcera]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d')} 
json_feature_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_feature_with_bigid_structure=[{"name":"id","type":"bigint primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_feature_with_bigid__without_pk_structure=[{"name":"id","type":"bigint"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_admin_unit_structure=[{"name":"id","type":"integer primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_admin_unit_structure_at=[{"name":"id","type":"text primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]

###########################################################################################################################################
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

dbs_olu_connection={**{'dbname':'olu'},**connectionstring_localhost}
dbs_olu=DBStorage(dbs_olu_connection)
dbs_olu.connect()
dbs_olu.disconnect()
dbs_olu.connect()
###########################################################################################################################################
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
                              [{"url":"https://land.copernicus.eu/pan-european/corine-land-cover/clc2018?tab=download",
                               "format":"GPKG", "compression":"zip", "downloadable":"on_registration"},{"local":"/home/jupyter-dima/corine_cz/corine_cz.shp",
                               "format":"ESRI Shapefile"}],'data')

###########################################################################################################################################
admunit_cz__ds=ds_from_metadata(admunit_cz__metadata)

ruian_cz__ds=ds_from_metadata(ruian_cz__metadata)
ruian_cz__ds.set_attributes({**ruian_cz__ds.get_attributes(),**{'layer':'Parcely'}})

lpis_cz__ds_xml=ds_from_metadata(lpis_cz__metadata,format='XML')

ua_cz__ds=ds_from_metadata(ua_cz__metadata)
ua_cz__ds.set_attributes({**ua_cz__ds.get_attributes(),**{'layer':'{compilable:ua_gpkg_layer}'}})

corine_cz__ds=ds_from_metadata(corine_cz__metadata,format="ESRI Shapefile")
###########################################################################################################################################
admunit_cz=GeoConcept('Administrative units in Czech Republic','Administrative units in Czech Republic. All levels.',
                      'AdmUnitFeature',json_admin_unit_structure, data_source=admunit_cz__ds, subgeoconcepts=[] )

ruian_parcely_cz=GeoConcept('Land parcels in Czech Republic','Digital land parcels (parcely) in Czech Republic.',
                      'FeatureWithID',json_feature_with_bigid_structure, data_source=ruian_cz__ds,subgeoconcepts=[],adm_graph_node='1')

lpis_cz=GeoConcept('LPIS in Czech Republic','LPIS in Czech Republic. All levels.',
                      'Feature',json_feature_structure, data_source=lpis_cz__ds_xml, subgeoconcepts=[], adm_graph_node='1')

corine_cz=GeoConcept('CORINE land cover in Czech Republic','CORINE land cover in Czech Republic in years 1990 - 2018.',
                      'Feature',json_feature_structure, subgeoconcepts=[], data_source=corine_cz__ds)

ua_cz=GeoConcept('Urban Atlas in Czech Republic','Urban Atlas in Czech Republic in 2012.',
                      'Feature',json_feature_structure, subgeoconcepts=[], data_source=ua_cz__ds)
                      
###########################################################################################################################################

if 'local' in ruian_parcely_cz.get_data_source().get_attributes():
    ruian_parcely_cz.get_data_source().set_data_file(ruian_parcely_cz.get_data_source().get_attributes()['local']) 

if 'local' in corine_cz.get_data_source().get_attributes():
    corine_cz.get_data_source().set_data_file(corine_cz.get_data_source().get_attributes()['local']) 
    
###########################################################################################################################################
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

###########################################################################################################################################
#admunit_cz__ds.download_data('archive.zip',s,'all',os.getcwd())
#admunit_cz__ds.set_data_file('20201031_ST_UKSG.xml')
###########################################################################################################################################

admunit_cz.create_table(dbs_admin, name='default',scheme='cz',conflict='append')

ruian_parcely_cz.create_table(dbs_ruian_cz, name='default',scheme='public',conflict='append',adm_graph_node='1')

lpis_cz.create_table(dbs_lpis_cz,name='default',scheme='public',conflict='append', adm_graph_node='1')

ua_cz.create_table(dbs_urbanatlas,  name='default',scheme='public',conflict='append',adm_graph_node='1')

corine_cz.create_table(dbs_corine,  name='default',scheme='public',conflict='append',adm_graph_node='1')

###########################################################################################################################################
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
            ruian_parcely_cz.append_subgeoconcept(SubGeoConcept(str(j),'RUIAN land parcels in Czech administrative territorial unit %s ' % str(j),'FeatureWithID',ruian_parcely_cz.get_attributes(),data_source=DataSource(ruian_parcely_cz.get_data_source().get_type(),ruian_parcely_cz.get_data_source().get_name(),(dict(ruian_parcely_cz.get_data_source().get_attributes(),**{'url':ruian_parcely_cz.get_data_source().get_attributes()['url'].replace(i,str(j))})),None,None),supergeoconcept=ruian_parcely_cz,table_inheritance=True,subgeoconcepts=[]))

for i in re.findall('\{.*?\}',lpis_cz.get_data_source().get_attributes()['url']): 
    if i[1:-1] in list(compilable_tree_dictionary(admunit_cz).keys()):
        for j in apply_function(compilable_tree_dictionary(admunit_cz),i[1:-1]):
            lpis_cz.append_subgeoconcept(SubGeoConcept(str(j),'LPIS in Czech administrative territorial unit %s ' % str(j),'Feature',lpis_cz.get_attributes(),data_source=DataSource(lpis_cz.get_data_source().get_type(),lpis_cz.get_data_source().get_name(),(dict(lpis_cz.get_data_source().get_attributes(),**{'url':lpis_cz.get_data_source().get_attributes()['url'].replace(i,str(j))})),None,None),supergeoconcept=lpis_cz,table_inheritance=True,subgeoconcepts=[],type='spatial:admin',adm_graph_node=str(j)))

concept_list=[re.search('_.*_',file)[0][1:-1] for file in os.listdir('.') if file.endswith('.zip') and file.startswith('CZ')]
for concept in concept_list:
    for i in re.findall('\{.*?\}',ua_cz.get_data_source().get_attributes()['layer']): 
        if i.split(':')[1][:-1] in list((compilable_ua_dictionary('')).keys()):
            geoconcept=SubGeoConcept('%s' % concept,'Urban Atlas in agglomeration %s ' % str(concept),'Feature',ua_cz.get_attributes(),data_source=DataSource(ua_cz.get_data_source().get_type(), ua_cz.get_data_source().get_name(),(ua_cz.get_data_source().get_attributes()),None,None),supergeoconcept=ua_cz,table_inheritance=True,subgeoconcepts=[])
            files=unzip_file([file for file in os.listdir('.') if file.endswith('.zip') and file.startswith('CZ') and re.match('^.*%s.*$' % concept,file)][0],'all',os.getcwd())
            geoconcept.get_data_source().set_data_file([file for file in files if file.endswith('gpkg')][0])
            geoconcept.get_data_source().set_attribute({'layer':apply_function(compilable_ua_dictionary(geoconcept),i.split(':')[1][:-1])})
            ua_cz.append_subgeoconcept(geoconcept)
###########################################################################################################################################
G=apply_function(compilable_node_dictionary(admunit_cz),'admunit__tree')
###########################################################################################################################################

for s in lpis_cz.get_subgeoconcepts():
    municipality=[i for i in G.neighbors(str(s.get_adm_graph_node()))][0]
    s.create_table(dbs_lpis_cz,name='ob_'+str(municipality),scheme='data',conflict='append',adm_graph_node=municipality)
    s.set_table(View('ku_'+s.get_name(), s.get_attributes(), s.get_table(), "dictionary_select5((data->'PREKRYVKATUZE')::json, 'KUKOD == \"%s\" AND PLATNOSTDO>\"[aktualni_cas]\"')::jsonb not in ('[]'::jsonb,'{}'::jsonb)" % s.get_name(), dbs=dbs_lpis_cz, scheme='data',type='usual'))
    dbs_lpis_cz.execute(s.get_table().create_script())
    
for sub in ruian_parcely_cz.get_subgeoconcepts():
    sub.create_table(dbs_ruian_cz,name='ob_'+sub.get_name(),scheme='data',conflict='append',adm_graph_node=sub.get_adm_graph_node())
    
for concept in ua_cz.get_subgeoconcepts():
    concept.create_table(dbs_urbanatlas,name='default',scheme='data',conflict='append')
    
###########################################################################################################################################
lpis_hilucs={'2':'111', '3':'111', '4':'111', '5':'111', '6':'110', '7':'110', '9':'110', '10':'110', '11':'110', '12':'631', '91':'120', '97':'142', '98':'120', '99':'120'} 
ruian_zpv_hilucs={'1':'112', '2':'120', '3':'121', '4':'120', '5':'120', '6':'632', '7':'632', '8':'632', '9':'632', '10':'632', '11':'632', '12':'530', '13':'530', '14':'411', '15':'411', '16':'411', '17':'411', '18':'415', '19':'344', '20':'343', '21':'335', '22':'341', '23':'411', '24':'130', '25':'433', '26':'650', '27':'630', '28':'632', '29':'244','30':'630'}  
ruian_dp_hilucs={'2':'111', '3':'111', '4':'111', '5':'500', '6':'110', '7':'110', '8':'110', '10':'120', '11':'632', '13':'510', '14':'660'} 
corine_hilucs={'111':'500', '112':'500', '121':'660', '122':'410', '123':'414', '124':'413', '131':'130', '132':'433', '133':'660', '141':'340', '142':'340', '211':'111', '212':'111', '213':'111', '221':'111', '222':'111', '223':'111', '231':'111', '241':'111', '242':'111', '243':'111', '244':'120', '311':'120', '312':'120', '313':'120', '321':'631', '322':'631', '323':'631', '324':'631', '331':'631', '332':'631', '333':'631', '334':'620', '335':'631', '411':'631', '412':'631', '421':'631', '422':'631', '423':'631', '511':'414', '512':'660', '521':'660', '522':'414', '523':'660'} 
ua_hilucs={'11100':'500', '11200':'500', '11210':'500', '11220':'500', '11230':'500', '11240':'500', '11300':'500', '12100':'300', '12200':'410', '12210':'411', '12220':'411', '12230':'412', '12300':'414', '12400':'413', '13100':'130', '13300':'600', '13400':'600', '14100':'344', '14200':'340', '21000':'110', '22000':'110', '23000':'110', '24000':'110', '25000':'110', '30000':'120', '31000':'120', '32000':'120', '33000':'660', '40000':'660', '50000':'660', '91000':'660', '92000':'660'} 
###########################################################################################################################################

def ruian_transformations(object,object_fid,attributes_fid): 
    o_dict=\
    {'feature':{'object':object},\
     'object_id':{'object':object_fid},\
     'attributes_id':{'object':attributes_fid},\
     'object_geom':{'function':(lambda y: y.get_geometry()),'parameters':['feature']},\
     'object_properties':{'function':(lambda x: {**{'fid':x},**{'dataset_id':1},**{'z_value':1}}),'parameters':['object_id']},
     'attributes_properties':{'function':(lambda x,y: {**{'fid':x},**{'hilucs_value':ruian_zpv_hilucs[str(y.get_data()['ZpusobyVyuzitiPozemku'])] if y.get_data()['ZpusobyVyuzitiPozemku'] is not None else ruian_dp_hilucs[str(y.get_data()['DruhPozemkuKod'])]},**{'atts':y.get_data()},**{'dataset_id':1}}),'parameters':['attributes_id','feature']},
     'object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO olu2.olu_object (fid,dataset_id,z_value,geom) VALUES (%s,%s,%s,ST_SetSRID(ST_Multi(ST_GeomFromText('%s')),4326))" % (x,z['dataset_id'],z['z_value'],y) ),'parameters':['object_id','object_geom','object_properties']},\
     'attributes_insert_statement':{'function':(lambda x,y,z: "INSERT INTO olu2.olu_attributes (fid,hilucs_value,atts,dataset_id) VALUES (%s,%s,('%s'::json),%s)" % (x,z['hilucs_value'],json.dumps(y.get_data()),z['dataset_id']) ),'parameters':['attributes_id','feature','attributes_properties']},\
     'attribute2originvalue_insert_statement':{'function':(lambda x,y: "INSERT INTO olu2.attribute_origin_value (atts_origin_id,origin_name,description) VALUES (%s,%s,'%s')" % (x,y['dataset_id'],'origin description') ),'parameters':['attributes_id','attributes_properties']},\
     'attributes2object_insert_statement':{'function':(lambda x,y: "INSERT INTO olu2.olu_atts_to_object (object_fid,atts_fid,atts_origin) VALUES (%s,%s,%s)" % (x,y,y) ),'parameters':['object_id','attributes_id']},\
     'four_insert_statements':{'function':(lambda w,x,y,z: [w,x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attribute2originvalue_insert_statement','attributes2object_insert_statement']},\
    }
    return o_dict
    
def corine_transformations(object,object_fid,attributes_fid): 
    o_dict=\
    {'feature':{'object':object},\
     'object_id':{'object':object_fid},\
     'attributes_id':{'object':attributes_fid},\
     'object_geom':{'function':(lambda y: y.get_geometry()),'parameters':['feature']},\
     'object_properties':{'function':(lambda x: {**{'fid':x},**{'dataset_id':2},**{'z_value':2}}),'parameters':['object_id']},
     'attributes_properties':{'function':(lambda x,y: {**{'fid':x},**{'hilucs_value':corine_hilucs[str(y.get_data()['clc_code'])]},**{'atts':y.get_data()},**{'dataset_id':2}}),'parameters':['attributes_id','feature']},
     'object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO olu2.olu_object (fid,dataset_id,z_value,geom) VALUES (%s,%s,%s,ST_SetSRID(ST_Multi(ST_GeomFromText('%s')),4326))" % (x,z['dataset_id'],z['z_value'],y) ),'parameters':['object_id','object_geom','object_properties']},\
     'attributes_insert_statement':{'function':(lambda x,y,z: "INSERT INTO olu2.olu_attributes (fid,hilucs_value,atts,dataset_id) VALUES (%s,%s,('%s'::json),%s)" % (x,z['hilucs_value'],json.dumps(y.get_data()),z['dataset_id']) ),'parameters':['attributes_id','feature','attributes_properties']},\
     'attribute2originvalue_insert_statement':{'function':(lambda x,y: "INSERT INTO olu2.attribute_origin_value (atts_origin_id,origin_name,description) VALUES (%s,%s,'%s')" % (x,y['dataset_id'],'origin description') ),'parameters':['attributes_id','attributes_properties']},\
     'attributes2object_insert_statement':{'function':(lambda x,y: "INSERT INTO olu2.olu_atts_to_object (object_fid,atts_fid,atts_origin) VALUES (%s,%s,%s)" % (x,y,y) ),'parameters':['object_id','attributes_id']},\
     'four_insert_statements':{'function':(lambda w,x,y,z: [w,x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attribute2originvalue_insert_statement','attributes2object_insert_statement']},\
    }
    return o_dict
    
    
