import os
import classes.tridy as tridy
from classes.tridy import GeoConcept, SubGeoConcept, MetaData, Table, View, DBStorage, DataSource, Feature, FeatureWithID,  AdmUnitFeature, OLUFeature, Grid, Imagee, ds_from_metadata, xml_lpis_cz_reader, lpis_cz__posledni_aktualizace, get_listvalues_from_generator, apply_function, select_nodes_from_graph, unzip_file, find_neighbors_till, connection_parameters_to_pg, world_to_pixel 
#from importlib import reload
import requests
import datetime
import re
#from io import BytesIO

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from osgeo import ogr, osr, gdal
#import networkx as nx
#import numpy as np
import json
#import binascii
#import copy
#import time

#from lxml import etree

#del(GeoConcept, SubGeoConcept, MetaData, Table, View, DBStorage, DataSource, Feature, FeatureWithID, AdmUnitFeature, OLUFeature, Grid, Imagee, ds_from_metadata,xml_lpis_cz_reader,get_listvalues_from_generator,apply_function,select_nodes_from_graph,world_to_pixel)
#reload(tridy)
#from classes.tridy import GeoConcept, SubGeoConcept, MetaData, Table, View, DBStorage, DataSource, Feature, FeatureWithID, AdmUnitFeature, OLUFeature, Grid, Imagee, ds_from_metadata, xml_lpis_cz_reader, get_listvalues_from_generator, apply_function, select_nodes_from_graph,world_to_pixel

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
    'admunit__tree__neighbors__43__4':{'function':find_neighbors_level,'parameters':['admunit__tree__reverse','43',4]},\
    'admunit__tree__neighbors__43__3':{'function':find_neighbors_level,'parameters':['admunit__tree__reverse','43',3]},\
    }
    return g_dict
    
def find_neighbors_level(graph,start_node,level):
    if graph.nodes()[start_node]['level']==level:
        yield start_node
    else:
        for n in graph.neighbors(start_node):
            yield from find_neighbors_level(graph,n,level) 

'''def get_ruian_au_feature_geometry_from_wfs(gml_id):
    url='https://services.cuzk.cz/wfs/inspire-au-wfs.asp?service=WFS&request=GetFeature&typeName=au:AdministrativeUnit&maxFeatures=1&featureID=%s&version=2.0.0' %gml_id
    r=requests.get(url,stream=False)
    if r.status_code==200:
        tree=etree.parse(BytesIO(r.content))
        root=tree.getroot()
        geom=root.find('.//{http://www.opengis.net/gml/3.2}MultiSurface')
        geom_ogr=ogr.CreateGeometryFromGML(etree.tostring(geom).decode())
        return geom_ogr.ExportToWkt()
    else:
        return 'WFS no works' '''
        
#inicializace promenne typu Session , pouziva se pri stahovani
s = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
s.mount('http://', HTTPAdapter(max_retries=retries))

#slovnik parametru, ktere muzou byt pouzite v URL odkazech na datove sady
replacement_dictionary = {"[posledni_den_mesice]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d'),"[lpis_cz__posledni_aktualizace]":lpis_cz__posledni_aktualizace().strftime('%Y%m%d'), "[vcera]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d')} 

#datove struktury; jsou hlavne treba pro tvorbu tabulek v Postgresu
json_feature_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_feature_with_bigid_structure=[{"name":"id","type":"bigint primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_admin_unit_structure=[{"name":"id","type":"integer primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_admin_unit_structure_at=[{"name":"id","type":"text primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_feature_with_raster_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"raster_maps","type":"raster"}]

#definice metadat datove sady uzemni celky z RUIANu
admunit_cz__metadata=MetaData('Administrative units in Czech Republic',
                              {"url":"https://vdp.cuzk.cz/vymenny_format/soucasna/[posledni_den_mesice]_ST_UKSG.xml.zip",
                               "format":"GML", "compression":"zip"},'data')
                               
#tvorba promenne druhu DataSource na zaklade metadat
admunit_cz__ds=ds_from_metadata(admunit_cz__metadata)

#tvorba zastresujici  (zastresuje metadata, data, zprostredkovava praci s daty atd.) promenne pro praci s uzemnimi celky
admunit_cz=GeoConcept('Administrative units in Czech Republic','Administrative units in Czech Republic. All levels.',
                      'AdmUnitFeature',json_admin_unit_structure, data_source=admunit_cz__ds, subgeoconcepts=[] )

#vymena parametru pouzitych v URL odkazu metadat
url_adresa=admunit_cz.get_data_source().get_attributes()['url']
for i in re.findall('\[.*?\]',url_adresa):
    if i in list(replacement_dictionary.keys()):
        url_adresa=url_adresa.replace(i,replacement_dictionary[i])
        
admunit_cz.get_data_source().set_attribute({'url':url_adresa})
del(url_adresa)

#stazeni dat
admunit_cz.get_data_source().download_data('archive.zip',s,'all',os.getcwd())

#v pripade data uz jsou stazena lze nastavit cestu k souboru
#admunit_cz.get_data_source().set_data_file('20201031_ST_UKSG.xml')

#seznam potrebnych vrstev pro vytvareni stromu uzemnich celku
concept_list=['Staty','Vusc','Okresy','Obce','KatastralniUzemi']
#definice atributu idcka, idcka rodice, a radovostni urovni ; je treba pro tvorbu stromu
concept_additional_attributes={'Staty':{'level_value':0,'parent_value':'null','id_attribute':'Kod'},
                               'Vusc':{'level_value':1,'parent_value':'1','id_attribute':'Kod'},
                               'Okresy':{'level_value':2,'parent_attribute':'VuscKod','id_attribute':'Kod'},
                               'Obce':{'level_value':3,'parent_attribute':'OkresKod','id_attribute':'Kod'},
                               'KatastralniUzemi':{'level_value':4,'parent_attribute':'ObecKod','id_attribute':'Kod'}}
#pridani tematickych vrstev zastresujici promenne admunit_cz                               
for l in list(set(concept_list).intersection(set(admunit_cz.get_data_source().list_layers()))):
    admunit_cz.append_subgeoconcept(SubGeoConcept(l,l,'AdmUnitFeature',admunit_cz.get_attributes(),data_source=DataSource(admunit_cz.get_data_source().get_type(),admunit_cz.get_data_source().get_name(),({**admunit_cz.get_data_source().get_attributes(),**{'layer':l}}),None,admunit_cz.get_data_source().get_data_file()),supergeoconcept=admunit_cz,table_inheritance=False,type='semantic',subgeoconcepts=[]))
    
#pro kazdou tematickou vrstvu, odpovidajici urcite radovostni urovni (Stat,Kraj,Okres atd...) vyber dat a ukladani do formatu geojson
for sub in admunit_cz.get_subgeoconcepts():
    sub.set_geojson_output_backend(os.getcwd()+'/',sub.get_name()+'.geojson')
    with open(sub.get_geojson_output_backend(), 'w', encoding='utf-8') as file:
        geojson={"type": "FeatureCollection", "features": [] }
        features=sub.get_data_source().read_features('admunitfeature',concept_additional_attributes[sub.get_data_source().get_attributes()['layer']],number=10)
        for f in features:
                if len(f)>0:
                    for feature in f:
                        geojson["features"].append(feature.export_to_geojson())
        json.dump(geojson, file, ensure_ascii=False, indent=4)
        
#vytvareni stromu uzemnich celku
G=apply_function(compilable_tree_dictionary(admunit_cz),'admunit__tree')

#to same s LPISem, akurat tady omezene na uzemi Plzenskeho kraje
lpis_cz__metadata=MetaData('LPIS in Plzeňský kraj',
                              [{"url":"http://eagri.cz/public/app/eagriapp/lpisdata/[lpis_cz__posledni_aktualizace]-{admunit__tree__neighbors__43__4}-DPB-SHP.zip",
                               "format":"SHP", "compression":"zip"},{"url":"http://eagri.cz/public/app/eagriapp/lpisdata/[lpis_cz__posledni_aktualizace]-{admunit__tree__neighbors__43__4}-DPB-XML-A.zip",
                               "format":"XML", "compression":"zip"}],'data')
lpis_cz__ds_xml=ds_from_metadata(lpis_cz__metadata,format='XML')
lpis_cz=GeoConcept('LPIS in Plzeňský kraj','LPIS in Plzeňský kraj. All levels.',
                      'Feature',json_feature_structure, data_source=lpis_cz__ds_xml, subgeoconcepts=[], adm_graph_node='43')
                      
#vymena parametru url adresy
url_adresa=lpis_cz.get_data_source().get_attributes()['url']
for i in re.findall('\[.*?\]',url_adresa):
    if i in list(replacement_dictionary.keys()):
        url_adresa=url_adresa.replace(i,replacement_dictionary[i])
lpis_cz.get_data_source().set_attribute({'url':url_adresa})
del(url_adresa)

#pridani zemepisnych podvrstev dle parametru url {admunit__tree__neighbors__43__4}
for i in re.findall('\{.*?\}',lpis_cz.get_data_source().get_attributes()['url']): 
    if i[1:-1] in list(compilable_node_dictionary(admunit_cz).keys()):
        for j in apply_function(compilable_node_dictionary(admunit_cz),i[1:-1]):
                lpis_cz.append_subgeoconcept(SubGeoConcept(str(j),'LPIS in Czech administrative territorial unit %s ' % str(j),'Feature',lpis_cz.get_attributes(),data_source=DataSource(lpis_cz.get_data_source().get_type(),lpis_cz.get_data_source().get_name(),(dict(lpis_cz.get_data_source().get_attributes(),**{'url':lpis_cz.get_data_source().get_attributes()['url'].replace(i,str(j))})),None,None),supergeoconcept=lpis_cz,table_inheritance=True,subgeoconcepts=[],type='spatial:admin',adm_graph_node=str(j)))
                
#stahovani dat pro zemepisne podvrstvy a ukladani do formatu geojson
for sub in lpis_cz.get_subgeoconcepts():
    sub.get_data_source().download_data('archive.zip',s,'all',os.getcwd())
    sub.set_geojson_output_backend(os.getcwd()+'/',sub.get_name()+'.geojson')
    with open(sub.get_geojson_output_backend(), 'w', encoding='utf-8') as file:
        geojson={"type": "FeatureCollection", "features": [] }
        features=sub.get_data_source().read_features('feature',number=10,reader=xml_lpis_cz_reader)
        for f in features:
                if len(f)>0:
                    for feature in f:
                        geojson["features"].append(feature.export_to_geojson())
        json.dump(geojson, file, ensure_ascii=False, indent=4)
    os.remove(sub.get_data_source().get_data_file())
        
