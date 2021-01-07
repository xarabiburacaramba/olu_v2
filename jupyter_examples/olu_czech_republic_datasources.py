# %%
import os
import tridy
from tridy import GeoConcept, SubGeoConcept, MetaData, Table, View, DBStorage, DataSource, Feature, FeatureWithID,  AdmUnitFeature, OLUFeature, Grid, Imagee, ds_from_metadata, xml_lpis_cz_reader, lpis_cz__posledni_aktualizace, get_listvalues_from_generator, apply_function, select_nodes_from_graph, unzip_file, find_neighbors_till, connection_parameters_to_pg, transform_name_to_postgresql_format, world_to_pixel 
from importlib import reload
import requests
import datetime
import re
from io import BytesIO

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from osgeo import ogr, osr, gdal
import networkx as nx
import numpy as np
import json
import binascii
import copy
import time

from lxml import etree

# %%
del(GeoConcept, SubGeoConcept, MetaData, Table, View, DBStorage, DataSource, Feature, FeatureWithID, AdmUnitFeature, OLUFeature, Grid, Imagee, ds_from_metadata,xml_lpis_cz_reader,get_listvalues_from_generator,apply_function,select_nodes_from_graph,world_to_pixel)
reload(tridy)
from tridy import GeoConcept, SubGeoConcept, MetaData, Table, View, DBStorage, DataSource, Feature, FeatureWithID, AdmUnitFeature, OLUFeature, Grid, Imagee, ds_from_metadata, xml_lpis_cz_reader, get_listvalues_from_generator, apply_function, select_nodes_from_graph,world_to_pixel

# %%
class Theme():
    ''''popis pasportu objektu'''
    def __init__(self, name, description,  attributes):
        self._name=name
        self._description=description
        self._attributes=attributes
    def get_name(self):
        return self._name
    def get_description(self):
        return self._description
    def get_attributes(self):
        return self._attributes
    def set_geometry_order(self, geoconcepts):
        self._geometry_order=geoconcepts
    def get_geometry_order(self):
        return self._geometry_order
    def set_attribute_order(self, geoconcepts):
        self._attribute_order=geoconcepts
    def get_attribute_order(self):
        return self._attribute_order
    def set_dictionary(self, dictionary):
        self._dictionary=dictionary
    def get_dictionary(self):
        return self._dictionary
    def transform(self,dictionary_key):
        concept=apply_function(self._dictionary,dictionary_key)
        return concept
    '''def transform(self, transformation_dictionary, level=None, fill_all=False):
        if level is not None:
            if self._geometry_order[0].get_type()=='spatial:admin':
                level_objects=level.read_features()# precist vsechny prvky, napriklad, z urovne Obce anebo KatastralniUzemi
                for level_object in level_objects:
                    features=self._geometry_order[0].find_subconcept_by_name(level_object.get_name()).read_features()
                    transform_features # mozna najit zpusob a aplikovat transformaci za cteni
                    write_features_to_the_table
            geometry_subgeoconcepts=self._geometry_order[0].get_subgeoconcepts()
            attributes_subgeoconcepts=self._attribute._order[0].get_subgeoconcepts()
            if geometry_subgeoconcepts.
        for level_object in level_objects :
            
            geoconcept with the highest priority select features from db from this level
            for features from selected geoconcept:
                transform selected features according to the provided transformation_dictionary
                add transformed feature to the return list
            yield(subgeoconcept, list)'''

# %%
def compilable_tree_dictionary(object): 
    g_dict=\
    {'admunit':{'object':object},\
    'admunit__tree':{'object':'admunit','function':'return_graph_representation'},\
    'admunit__tree__reverse':{'object':'admunit__tree','function':'reverse'},\
    'admunit__tree__level3':{'function':select_nodes_from_graph,'parameters':['admunit__tree','level',3]},\
    'admunit__tree__level4':{'function':select_nodes_from_graph,'parameters':['admunit__tree','level',4]}}
    return g_dict

# %%
def compilable_node_dictionary(object,node__level=0,node__name='1'): 
    g_dict=\
    {'admunit':{'object':object},\
    'admunit__tree':{'object':'admunit','function':'return_graph_representation'},\
    'admunit__tree__reverse':{'object':'admunit__tree','function':'reverse'},\
    'node__level':{'object':node__level},\
    'node__name':{'object':node__name},\
    'admunit__tree__level':{'function':select_nodes_from_graph,'parameters':['admunit__tree','level','node__level']},\
    'admunit__tree__neighbors':{'function':find_neighbors_level,'parameters':['admunit__tree','node__name','node__level']},\
    'admunit__tree__neighbors__43__4':{'function':find_neighbors_level,'parameters':['admunit__tree__reverse','43',4]},\
    'admunit__tree__neighbors__43__3':{'function':find_neighbors_level,'parameters':['admunit__tree__reverse','43',3]},\
    }
    return g_dict

# %%
def find_neighbors_level(graph,start_node,level):
    if graph.nodes()[start_node]['level']==level:
        yield start_node
    else:
        for n in graph.neighbors(start_node):
            yield from find_neighbors_level(graph,n,level) 

# %%
def raster_value(point,transformation,raster):
    point.Transform(transformation)
    return raster.get_data()[tuple(np.flip(world_to_pixel(raster.get_metadata()['affine_transformation'],point.GetX(),point.GetY())))]

# %%
def get_ruian_au_feature_geometry_from_wfs(gml_id):
    url='https://services.cuzk.cz/wfs/inspire-au-wfs.asp?service=WFS&request=GetFeature&typeName=au:AdministrativeUnit&maxFeatures=1&featureID=%s&version=2.0.0' %gml_id
    r=requests.get(url,stream=False)
    if r.status_code==200:
        tree=etree.parse(BytesIO(r.content))
        root=tree.getroot()
        geom=root.find('.//{http://www.opengis.net/gml/3.2}MultiSurface')
        geom_ogr=ogr.CreateGeometryFromGML(etree.tostring(geom).decode())
        return geom_ogr.ExportToWkt()
    else:
        return 'WFS no works'

# %%
#for the case when data has to be downloaded externally initialization of requests session variable with setting of number of retries
s = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
s.mount('http://', HTTPAdapter(max_retries=retries))

# %%
replacement_dictionary = {"[posledni_den_mesice]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d'),"[lpis_cz__posledni_aktualizace]":lpis_cz__posledni_aktualizace().strftime('%Y%m%d'), "[vcera]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d')} 
json_feature_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_feature_with_bigid_structure=[{"name":"id","type":"bigint primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_admin_unit_structure=[{"name":"id","type":"integer primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_admin_unit_structure_at=[{"name":"id","type":"text primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_feature_with_raster_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"raster_maps","type":"raster"}]

# %%
admunit_cz__metadata=MetaData('Administrative units in Czech Republic',
                              {"url":"https://vdp.cuzk.cz/vymenny_format/soucasna/[posledni_den_mesice]_ST_UKSG.xml.zip",
                               "format":"GML", "compression":"zip"},'data')

# %%
admunit_cz__ds=ds_from_metadata(admunit_cz__metadata)
admunit_cz=GeoConcept('Administrative units in Czech Republic','Administrative units in Czech Republic. All levels.',
                      'AdmUnitFeature',json_admin_unit_structure, data_source=admunit_cz__ds, subgeoconcepts=[] )

# %%
url_adresa=admunit_cz.get_data_source().get_attributes()['url']
for i in re.findall('\[.*?\]',url_adresa):
    if i in list(replacement_dictionary.keys()):
        url_adresa=url_adresa.replace(i,replacement_dictionary[i])
        
admunit_cz.get_data_source().set_attribute({'url':url_adresa})
del(url_adresa)

# %%
#admunit_cz.get_data_source().download_data('archive.zip',s,'all',os.getcwd())
admunit_cz.get_data_source().set_data_file('20201031_ST_UKSG.xml')

# %%
concept_list=['Staty','Vusc','Okresy','Obce','KatastralniUzemi']
concept_additional_attributes={'Staty':{'level_value':0,'parent_value':'null','id_attribute':'Kod'},
                               'Vusc':{'level_value':1,'parent_value':'1','id_attribute':'Kod'},
                               'Okresy':{'level_value':2,'parent_attribute':'VuscKod','id_attribute':'Kod'},
                               'Obce':{'level_value':3,'parent_attribute':'OkresKod','id_attribute':'Kod'},
                               'KatastralniUzemi':{'level_value':4,'parent_attribute':'ObecKod','id_attribute':'Kod'}}

# %%
for l in list(set(concept_list).intersection(set(admunit_cz.get_data_source().list_layers()))):
    admunit_cz.append_subgeoconcept(SubGeoConcept(l,l,'AdmUnitFeature',admunit_cz.get_attributes(),data_source=DataSource(admunit_cz.get_data_source().get_type(),admunit_cz.get_data_source().get_name(),({**admunit_cz.get_data_source().get_attributes(),**{'layer':l}}),None,admunit_cz.get_data_source().get_data_file()),supergeoconcept=admunit_cz,table_inheritance=False,type='semantic',subgeoconcepts=[]))

# %%
#administrative territorial units
dbs_admin_connection={'dbname':'olu_administrative_units','user':'euxdat_admin','host':'euxdat-db-svc','port':'5432','password':'Euxdat12345'}
dbs_admin=DBStorage(dbs_admin_connection)
dbs_admin.connect()
dbs_admin.disconnect()
dbs_admin.connect()

#lpis in czech republic
dbs_lpis_cz_connection={'dbname':'lpis_cz','user':'euxdat_admin','host':'euxdat-db-svc','port':'5432','password':'Euxdat12345'}
dbs_lpis_cz=DBStorage(dbs_lpis_cz_connection)
dbs_lpis_cz.connect()
dbs_lpis_cz.disconnect()
dbs_lpis_cz.connect()

#ruian cadastral parcels in czech republic
dbs_ruian_parcely_cz_connection={'dbname':'ruian_parcely_cz','user':'euxdat_admin','host':'euxdat-db-svc','port':'5432','password':'Euxdat12345'}
dbs_ruian_parcely_cz=DBStorage(dbs_ruian_parcely_cz_connection)
dbs_ruian_parcely_cz.connect()
dbs_ruian_parcely_cz.disconnect()
dbs_ruian_parcely_cz.connect()

#ruian urban atlas in czech republic
dbs_ua_cz_connection={'dbname':'ua_cz','user':'euxdat_admin','host':'euxdat-db-svc','port':'5432','password':'Euxdat12345'}
dbs_ua_cz=DBStorage(dbs_ua_cz_connection)
dbs_ua_cz.connect()
dbs_ua_cz.disconnect()
dbs_ua_cz.connect()

#ruian corine land cover in czech republic
dbs_clc_cz_connection={'dbname':'clc_cz','user':'euxdat_admin','host':'euxdat-db-svc','port':'5432','password':'Euxdat12345'}
dbs_clc_cz=DBStorage(dbs_clc_cz_connection)
dbs_clc_cz.connect()
dbs_clc_cz.disconnect()
dbs_clc_cz.connect()

# %%
#to get statistics
#dbs_admin.execute('SELECT pg_database.datname, pg_size_pretty(pg_database_size(pg_database.datname)) AS size FROM pg_database;')

# %%
#dbs_admin.execute('create schema cz')
#dbs_admin.execute('create extension postgis')
#dbs_lpis_cz.execute('create schema data')
#dbs_lpis_cz.execute('create extension postgis')
#dbs_ruian_parcely_cz.execute('create schema data')
#dbs_ruian_parcely_cz.execute('create extension postgis')

# %%
admunit_cz.create_table(dbs_admin, name='default',scheme='cz',conflict='append')

# %%
#for sub in admunit_cz.get_subgeoconcepts():
#    features=sub.get_data_source().read_features('admunitfeature',concept_additional_attributes[sub.get_data_source().get_attributes()['layer']],number=10)
#    dbs_admin.insert_many('insert into %s.%s (geom,data,id,level,parent_id) ' % (admunit_cz.get_table().get_scheme(),admunit_cz.get_table().get_name()) ,features,20)

# %%
for sub in admunit_cz.get_subgeoconcepts():
    sub.set_table(View(sub.get_name(),sub.get_attributes(), sub.get_supergeoconcept().get_table(),"level=%s" % (concept_additional_attributes[sub.get_name()]['level_value']), dbs=dbs_admin, scheme='public', type='usual'))
    dbs_admin.execute(sub.get_table().create_script())

# %%
'''for i in admunit_cz.read_features_from_table(number=100):
    if len(i)>0:
        for j in i:
            if j.get_level()==3:
                try:
                    dbs_admin.execute("update %s.%s set geom=st_geomfromtext('%s') where data->>'gml_id'='%s'" % (admunit_cz.get_table().get_scheme(),admunit_cz.get_table().get_name(),get_ruian_au_feature_geometry_from_wfs(j.get_data()['gml_id']),j.get_data()['gml_id']) )
                except:
                    dbs_admin.disconnect()
                    dbs_admin.connect()
                    print(j.get_data()['gml_id'])
    else:
        break'''

# %%
lpis_cz__metadata=MetaData('LPIS in Czech Republic',
                              [{"url":"http://eagri.cz/public/app/eagriapp/lpisdata/[lpis_cz__posledni_aktualizace]-{admunit__tree__level4}-DPB-SHP.zip",
                               "format":"SHP", "compression":"zip"},{"url":"http://eagri.cz/public/app/eagriapp/lpisdata/[lpis_cz__posledni_aktualizace]-{admunit__tree__level4}-DPB-XML-A.zip",
                               "format":"XML", "compression":"zip"}],'data')
lpis_cz__ds_xml=ds_from_metadata(lpis_cz__metadata,format='XML')
lpis_cz=GeoConcept('LPIS in Czech Republic','LPIS in Czech Republic. All levels.',
                      'Feature',json_feature_structure, data_source=lpis_cz__ds_xml, subgeoconcepts=[], adm_graph_node='1')

# %%
lpis_cz.create_table(dbs_lpis_cz,name='default',scheme='public',conflict='append')

# %%
url_adresa=lpis_cz.get_data_source().get_attributes()['url']
for i in re.findall('\[.*?\]',url_adresa):
    if i in list(replacement_dictionary.keys()):
        url_adresa=url_adresa.replace(i,replacement_dictionary[i])
lpis_cz.get_data_source().set_attribute({'url':url_adresa})
del(url_adresa)

# %%
for i in re.findall('\{.*?\}',lpis_cz.get_data_source().get_attributes()['url']): 
    if i[1:-1] in list(compilable_tree_dictionary(admunit_cz).keys()):
        for j in apply_function(compilable_tree_dictionary(admunit_cz),i[1:-1]):
            lpis_cz.append_subgeoconcept(SubGeoConcept(str(j),'LPIS in Czech administrative territorial unit %s ' % str(j),'Feature',lpis_cz.get_attributes(),data_source=DataSource(lpis_cz.get_data_source().get_type(),lpis_cz.get_data_source().get_name(),(dict(lpis_cz.get_data_source().get_attributes(),**{'url':lpis_cz.get_data_source().get_attributes()['url'].replace(i,str(j))})),None,None),supergeoconcept=lpis_cz,table_inheritance=True,subgeoconcepts=[],type='spatial:admin',adm_graph_node=str(j)))

# %%
for sub in lpis_cz.get_subgeoconcepts():
    #sub.get_data_source().download_data('archive.zip',s,'all',os.getcwd())
    #sub.create_table(dbs_lpis_cz,name=sub.get_name(),scheme='data',conflict='replace')
    #features=sub.get_data_source().read_features('feature',number=10,reader=xml_lpis_cz_reader)
    #dbs_lpis_cz.insert_many('insert into %s (geom,data) ' % (transform_name_to_postgresql_format(sub.get_table().get_scheme())+'.'+transform_name_to_postgresql_format(sub.get_table().get_name())) ,features,20)
    #os.remove(sub.get_data_source().get_data_file())
    sub.create_table(dbs_lpis_cz,name=sub.get_name(),scheme='data',conflict='append',adm_graph_node=sub.get_adm_graph_node())

# %%
ruian_cz__metadata=MetaData('RUIAN in Czech Republic',
                              {"url":"https://vdp.cuzk.cz/vymenny_format/soucasna/[vcera]_OB_{admunit__tree__level3}_UKSH.xml.zip",
                               "format":"GML", "compression":"zip"},'data')
ruian_parcely_cz__ds=ds_from_metadata(ruian_cz__metadata)
ruian_parcely_cz__ds.set_attributes({**ruian_parcely_cz__ds.get_attributes(),**{'layer':'Parcely'}})
ruian_parcely_cz=GeoConcept('Land parcels in Czech Republic','Digital land parcels (parcely) in Czech Republic.',
                      'FeatureWithID',json_feature_with_bigid_structure, data_source=ruian_parcely_cz__ds,subgeoconcepts=[],adm_graph_node='1')

# %%
url_adresa=ruian_parcely_cz.get_data_source().get_attributes()['url']
for i in re.findall('\[.*?\]',url_adresa):
    if i in list(replacement_dictionary.keys()):
        url_adresa=url_adresa.replace(i,replacement_dictionary[i])
ruian_parcely_cz.get_data_source().set_attribute({'url':url_adresa})
del(url_adresa)

# %%
ruian_parcely_cz.create_table(dbs_ruian_parcely_cz, name='default',scheme='public',conflict='append',adm_graph_node='1')

# %%
concept_list=['Parcely']
concept_additional_attributes={'Parcely':{'id_attribute':'Id'}}

for i in re.findall('\{.*?\}',ruian_parcely_cz.get_data_source().get_attributes()['url']): 
    if i[1:-1] in list(compilable_tree_dictionary(admunit_cz).keys()):
        for j in apply_function(compilable_tree_dictionary(admunit_cz),i[1:-1]):
            ruian_parcely_cz.append_subgeoconcept(SubGeoConcept('ob_%s' % str(j),'RUIAN land parcels in Czech administrative territorial unit %s ' % str(j),'FeatureWithID',ruian_parcely_cz.get_attributes(),data_source=DataSource(ruian_parcely_cz.get_data_source().get_type(),ruian_parcely_cz.get_data_source().get_name(),(dict(ruian_parcely_cz.get_data_source().get_attributes(),**{'url':ruian_parcely_cz.get_data_source().get_attributes()['url'].replace(i,str(j))})),None,None),supergeoconcept=ruian_parcely_cz,table_inheritance=True,subgeoconcepts=[],adm_graph_node=str(j)))

# %%
len(ruian_parcely_cz.get_subgeoconcepts())

# %%
for sub in ruian_parcely_cz.get_subgeoconcepts():
    #sub.get_data_source().download_data('archive.zip',s,'all',os.getcwd())
    #sub.create_table(dbs_ruian_parcely_cz,name='default',scheme='data',conflict='replace')
    #features=sub.get_data_source().read_features('featurewithid', concept_additional_attributes[sub.get_data_source().get_attributes()['layer']], number=100, gfs_template='sablona.gfs')
    #dbs_ruian_parcely_cz.insert_many('insert into %s (geom,data,id) ' % (transform_name_to_postgresql_format(sub.get_table().get_scheme())+'.'+transform_name_to_postgresql_format(sub.get_table().get_name())) ,features,100)
    #os.remove(sub.get_data_source().get_data_file())
    sub.create_table(dbs_ruian_parcely_cz,name='default',scheme='data',conflict='append',adm_graph_node=sub.get_adm_graph_node())

# %%
#dbs_ruian_parcely_cz.execute('select count(*) from %s' % ruian_parcely_cz.get_table().get_name() )

# %%
def ua_cz_gpkg_layer(data_file):
    return re.search('CZ.*_.*_',data_file.split('/')[-1])[0]+'UA2012'

def compilable_functions_dictionary(object): 
    o_dict=\
    {'geoconcept':{'object':object},\
     'geoconcept_name':{'object':'geoconcept','function':'get_name'},\
     'geoconcept_data_source':{'object':'geoconcept','function':'get_data_source'},\
     'geoconcept_data_source_file':{'object':'geoconcept_data_source','function':'get_data_file'},\
     'ua_gpkg_layer':{'function':ua_cz_gpkg_layer, 'parameters':['geoconcept_data_source_file']},\
    }
    return o_dict

# %%
ua_cz__metadata=MetaData('Urban Atlas in Czech Republic',
                              {"url":"https://land.copernicus.eu/local/urban-atlas/urban-atlas-2012?tab=download",
                               "format":"GPKG", "compression":"zip", "downloadable":"on_registration"},'data')
ua_cz__ds=ds_from_metadata(ua_cz__metadata)
ua_cz__ds.set_attributes({**ua_cz__ds.get_attributes(),**{'layer':'{compilable:ua_gpkg_layer}'}})
ua_cz=GeoConcept('Urban Atlas in Czech Republic','Urban Atlas in Czech Republic in 2012.',
                      'Feature',json_feature_structure, subgeoconcepts=[], data_source=ua_cz__ds)

# %%
concept_list=[re.search('_.*_',file)[0][1:-1] for file in os.listdir('urbanatlas_cz') if file.endswith('.zip') and file.startswith('CZ')]

# %%
for concept in concept_list:
    for i in re.findall('\{.*?\}',ua_cz__ds.get_attributes()['layer']): 
        if i.split(':')[1][:-1] in list((compilable_functions_dictionary('')).keys()):
            geoconcept=SubGeoConcept('%s' % concept,'Urban Atlas in agglomeration %s ' % str(concept),'Feature',ua_cz.get_attributes(),data_source=DataSource(ua_cz.get_data_source().get_type(), ua_cz.get_data_source().get_name(),(ua_cz.get_data_source().get_attributes()),None,None),supergeoconcept=ua_cz,table_inheritance=True,subgeoconcepts=[])
            files=unzip_file(['urbanatlas_cz/'+file for file in os.listdir('urbanatlas_cz') if file.endswith('.zip') and file.startswith('CZ') and re.match('^.*%s.*$' % concept,file)][0],'all',os.getcwd()+'/')
            geoconcept.get_data_source().set_data_file([file for file in files if file.endswith('gpkg')][0])
            geoconcept.get_data_source().set_attribute({'layer':apply_function(compilable_functions_dictionary(geoconcept),i.split(':')[1][:-1])})
            ua_cz.append_subgeoconcept(geoconcept)

# %%
ua_cz.create_table(dbs_ua_cz, name='default',scheme='public',conflict='replace')
#ua_cz.create_table(dbs_ua_cz, name='default',scheme='public',conflict='append')

# %%
for concept in ua_cz.get_subgeoconcepts():
    #concept.create_table(dbs_ua_cz,name='default',scheme='data',conflict='append')
    concept.create_table(dbs_ua_cz,name='default',scheme='data',conflict='replace')
    features=concept.get_data_source().read_features('feature', number=100)
    dbs_ua_cz.insert_many('insert into %s (geom,data) ' % (concept.get_table().get_scheme()+'.'+concept.get_table().get_name()) ,features,100)

# %%
features=concept.read_features_from_table(number=1)

# %%
next(features)[0].get_data()

# %%
del(features)

# %%
corine_cz__metadata=MetaData('CORINE land cover in Czech Republic',
                              [{"url":"https://land.copernicus.eu/pan-european/corine-land-cover/clc2018?tab=download",
                               "format":"GPKG", "compression":"zip", "downloadable":"on_registration"},{"local":"corine_cz/corine_cz.shp",
                               "format":"ESRI Shapefile"}],'data')
corine_cz__ds=ds_from_metadata(corine_cz__metadata,format="ESRI Shapefile")
corine_cz=GeoConcept('CORINE land cover in Czech Republic','CORINE land cover in Czech Republic in 2018.',
                      'Feature',json_feature_structure, subgeoconcepts=[], data_source=corine_cz__ds)

# %%
if 'local' in corine_cz__ds.get_attributes():
    corine_cz__ds.set_data_file(corine_cz__ds.get_attributes()['local']) 

# %%
corine_cz.create_table(dbs_clc_cz, name='default',scheme='public',conflict='replace')

# %%
features=corine_cz.get_data_source().read_features('feature', number=20)
dbs_clc_cz.insert_many('insert into %s (geom,data) ' % (corine_cz.get_table().get_scheme()+'.'+corine_cz.get_table().get_name()) ,features,10)

# %%
features=corine_cz.read_features_from_table(number=1)

# %%
next(features)[0].get_data()

# %%
del(features)