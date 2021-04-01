# %%
from tridy import GeoConcept, SubGeoConcept, MetaData, DBStorage, DataSource, ds_from_metadata, xml_lpis_cz_reader, lpis_cz__posledni_aktualizace, apply_function, select_nodes_from_graph, unzip_file, transform_name_to_postgresql_format
import requests
import datetime
import re
import os
import json
import sys

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from credentials import connectionstring_localhost

import numpy as np
np.set_printoptions(threshold=sys.maxsize)

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


replacement_dictionary = {"[posledni_den_mesice]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d'),"[lpis_cz__posledni_aktualizace]":lpis_cz__posledni_aktualizace().strftime('%Y%m%d'), "[vcera]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d')} 
json_feature_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_feature_with_bigid_structure=[{"name":"id","type":"bigint primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_feature_with_bigid__without_pk_structure=[{"name":"id","type":"bigint"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_admin_unit_structure=[{"name":"id","type":"integer primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_admin_unit_structure_at=[{"name":"id","type":"text primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_feature_with_raster_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"raster_maps","type":"raster"}]


session = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
session.mount('http://', HTTPAdapter(max_retries=retries))


dbs_admin_connection={**{'dbname':'olu_administrative_units'},**connectionstring_localhost}
dbs_admin=DBStorage(dbs_admin_connection)
dbs_admin.connect()
dbs_admin.disconnect()
dbs_admin.connect()

dbs_lpis_cz_connection={**{'dbname':'lpis_cz_ku'},**connectionstring_localhost}
dbs_lpis_cz=DBStorage(dbs_lpis_cz_connection)
dbs_lpis_cz.connect()
dbs_lpis_cz.disconnect()
dbs_lpis_cz.connect()


admunit_cz__metadata=MetaData('Administrative units in Czech Republic',
                              {"url":"https://vdp.cuzk.cz/vymenny_format/soucasna/[posledni_den_mesice]_ST_UKSG.xml.zip",
                               "format":"GML", "compression":"zip"},'data')


admunit_cz__ds=ds_from_metadata(admunit_cz__metadata)
admunit_cz=GeoConcept('Administrative units in Czech Republic','Administrative units in Czech Republic. All levels.',
                      'AdmUnitFeature',json_admin_unit_structure, data_source=admunit_cz__ds, subgeoconcepts=[] )


url_adresa=admunit_cz.get_data_source().get_attributes()['url']
for i in re.findall('\[.*?\]',url_adresa):
    if i in list(replacement_dictionary.keys()):
        url_adresa=url_adresa.replace(i,replacement_dictionary[i])

        
admunit_cz.get_data_source().set_attribute({'url':url_adresa})
del(url_adresa)

#admunit_cz.get_data_source().download_data('archive.zip',s,'all',os.getcwd())
admunit_cz.get_data_source().set_data_file('20201031_ST_UKSG.xml')

admunit_cz.create_table(dbs_admin, name='default',scheme='cz',conflict='append')

concept_list=['Staty','Vusc','Okresy','Obce','KatastralniUzemi']
concept_additional_attributes={'Staty':{'level_value':0,'parent_value':'null','id_attribute':'Kod'},
                               'Vusc':{'level_value':1,'parent_value':'1','id_attribute':'Kod'},
                               'Okresy':{'level_value':2,'parent_attribute':'VuscKod','id_attribute':'Kod'},
                               'Obce':{'level_value':3,'parent_attribute':'OkresKod','id_attribute':'Kod'},
                               'KatastralniUzemi':{'level_value':4,'parent_attribute':'ObecKod','id_attribute':'Kod'}}


for l in list(set(concept_list).intersection(set(admunit_cz.get_data_source().list_layers()))):
    admunit_cz.append_subgeoconcept(SubGeoConcept(l,l,'AdmUnitFeature',admunit_cz.get_attributes(),data_source=DataSource(admunit_cz.get_data_source().get_type(),admunit_cz.get_data_source().get_name(),({**admunit_cz.get_data_source().get_attributes(),**{'layer':l}}),None,admunit_cz.get_data_source().get_data_file()),supergeoconcept=admunit_cz,table_inheritance=False,type='semantic',subgeoconcepts=[]))

G=apply_function(compilable_node_dictionary(admunit_cz),'admunit__tree')

lpis_cz__metadata=MetaData('LPIS in Czech Republic',
                              [{"url":"http://eagri.cz/public/app/eagriapp/lpisdata/[lpis_cz__posledni_aktualizace]-{admunit__tree__level4}-DPB-SHP.zip",
                               "format":"SHP", "compression":"zip"},{"url":"http://eagri.cz/public/app/eagriapp/lpisdata/[lpis_cz__posledni_aktualizace]-{admunit__tree__level4}-DPB-XML-A.zip",
                               "format":"XML", "compression":"zip"}],'data')
lpis_cz__ds_xml=ds_from_metadata(lpis_cz__metadata,format='XML')
lpis_cz=GeoConcept('LPIS in Czech Republic','LPIS in Czech Republic. All levels.',
                      'FeatureWithID',json_feature_structure, data_source=lpis_cz__ds_xml, subgeoconcepts=[], adm_graph_node='1')


url_adresa=lpis_cz.get_data_source().get_attributes()['url']
for i in re.findall('\[.*?\]',url_adresa):
    if i in list(replacement_dictionary.keys()):
        url_adresa=url_adresa.replace(i,replacement_dictionary[i])
lpis_cz.get_data_source().set_attribute({'url':url_adresa})
del(url_adresa)

if dbs_lpis_cz.execute('select datum from metadata order by datum desc limit 1')[0][0]<lpis_cz__posledni_aktualizace().date():
    
    lpis_cz.create_table(dbs_lpis_cz,name='default',scheme='public',conflict='append', adm_graph_node='1')
    
    for i in re.findall('\{.*?\}',lpis_cz.get_data_source().get_attributes()['url']): 
        if i[1:-1] in list(compilable_tree_dictionary(admunit_cz).keys()):
            for j in apply_function(compilable_tree_dictionary(admunit_cz),i[1:-1]):
                lpis_cz.append_subgeoconcept(SubGeoConcept(str(j),'LPIS in Czech administrative territorial unit %s ' % str(j),'FeatureWithID',lpis_cz.get_attributes(),data_source=DataSource(lpis_cz.get_data_source().get_type(),lpis_cz.get_data_source().get_name(),(dict(lpis_cz.get_data_source().get_attributes(),**{'url':lpis_cz.get_data_source().get_attributes()['url'].replace(i,str(j))})),None,None),supergeoconcept=lpis_cz,table_inheritance=True,subgeoconcepts=[],type='spatial:admin',adm_graph_node=str(j)))
    
    for s in lpis_cz.get_subgeoconcepts():
        s.get_data_source().download_data('archive.zip',session,'all',os.getcwd()+'/')
        s.create_table(dbs_lpis_cz,name=transform_name_to_postgresql_format(s.get_adm_graph_node()),scheme='data',conflict='replace',adm_graph_node=s.get_adm_graph_node())
        features=s.get_data_source().read_features('feature',number=10,reader=xml_lpis_cz_reader)
        dbs_lpis_cz.insert_many('insert into %s (geom,data) ' % (s.get_table().get_scheme()+'.'+s.get_table().get_name()) ,features,20)
        os.remove(s.get_data_source().get_data_file())


dbs_lpis_cz.execute("insert into metadata (attributes, datum) values ('%s'::json, '%s')" % (json.dumps(lpis_cz.get_data_source().get_attributes()), datetime.datetime.strptime(re.findall('[0-9]{8}',lpis_cz.get_data_source().get_attributes()['url'])[0],'%Y%m%d').strftime('%Y-%m-%d') ) ) 
