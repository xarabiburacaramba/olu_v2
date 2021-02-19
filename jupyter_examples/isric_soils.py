# %%
import os
import tridy
from tridy import GeoConcept, SubGeoConcept, MetaData, Table, View, DBStorage, DataSource, Feature, FeatureWithID,  AdmUnitFeature, OLUFeature, Grid, Imagee, ds_from_metadata, xml_lpis_cz_reader, lpis_cz__posledni_aktualizace, get_listvalues_from_generator, apply_function, select_nodes_from_graph, unzip_file, find_neighbors_till, connection_parameters_to_pg, world_to_pixel 
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
replacement_dictionary = {"[posledni_den_mesice]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d'),"[lpis_cz__posledni_aktualizace]":lpis_cz__posledni_aktualizace().strftime('%Y%m%d'), "[vcera]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d')} 
json_feature_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_feature_with_bigid_structure=[{"name":"id","type":"bigint primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_admin_unit_structure=[{"name":"id","type":"integer primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_admin_unit_structure_at=[{"name":"id","type":"text primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]

# %%
admunit_cz__metadata=MetaData('Administrative units in Czech Republic',
                              {"url":"https://vdp.cuzk.cz/vymenny_format/soucasna/[posledni_den_mesice]_ST_UKSG.xml.zip",
                               "format":"GML", "compression":"zip"},'data')

# %%
admunit_cz__ds=ds_from_metadata(admunit_cz__metadata)
admunit_cz=GeoConcept('Administrative units in Czech Republic','Administrative units in Czech Republic. All levels.',
                      'AdmUnitFeature',json_admin_unit_structure, data_source=admunit_cz__ds, subgeoconcepts=[] )

# %%
url_adresa=admunit_cz__ds.get_attributes()['url']
for i in re.findall('\[.*?\]',url_adresa):
    if i in list(replacement_dictionary.keys()):
        url_adresa=url_adresa.replace(i,replacement_dictionary[i])
        
admunit_cz__ds.set_attribute({'url':url_adresa})
del(url_adresa)

# %%
admunit_cz__ds.set_data_file('20201031_ST_UKSG.xml')

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
#uzemni celky
dbs_admin_connection={'dbname':'olu_administrative_units','user':'p4b','host':'10.0.0.26','port':'5432','password':'p4b'}
dbs_admin=DBStorage(dbs_admin_connection)
dbs_admin.connect()
dbs_admin.disconnect()
dbs_admin.connect()

#zdrojova data
dbs_lpis_cz_connection={'dbname':'lpis_cz','user':'p4b','host':'10.0.0.26','port':'5432','password':'p4b'}
dbs_lpis_cz=DBStorage(dbs_lpis_cz_connection)
dbs_lpis_cz.connect()
dbs_lpis_cz.disconnect()
dbs_lpis_cz.connect()

dbs_ruian_cz_connection={'dbname':'ruian_cz','user':'p4b','host':'10.0.0.26','port':'5432','password':'p4b'}
dbs_ruian_cz=DBStorage(dbs_ruian_cz_connection)
dbs_ruian_cz.connect()
dbs_ruian_cz.disconnect()
dbs_ruian_cz.connect()

dbs_ua_cz_connection={'dbname':'ua_cz','user':'p4b','host':'10.0.0.26','port':'5432','password':'p4b'}
dbs_ua_cz=DBStorage(dbs_ua_cz_connection)
dbs_ua_cz.connect()
dbs_ua_cz.disconnect()
dbs_ua_cz.connect()

dbs_corine_cz_connection={'dbname':'corine_cz','user':'p4b','host':'10.0.0.26','port':'5432','password':'p4b'}
dbs_corine_cz=DBStorage(dbs_corine_cz_connection)
dbs_corine_cz.connect()
dbs_corine_cz.disconnect()
dbs_corine_cz.connect()

#pripojeni na vyslednou databazi
dbs_olu_connection={'dbname':'lpis_soils','user':'p4b','host':'10.0.0.26','port':'5432','password':'p4b'}
dbs_olu=DBStorage(dbs_olu_connection)
dbs_olu.connect()
dbs_olu.disconnect()
dbs_olu.connect()

# %%
admunit_cz.create_table(dbs_admin, name='default',scheme='cz',conflict='append')

# %%
for sub in admunit_cz.get_subgeoconcepts():
    sub.set_table(View(sub.get_name(),sub.get_attributes(), sub.get_supergeoconcept().get_table(),"level=%s" % (concept_additional_attributes[sub.get_name()]['level_value']), dbs=dbs_admin, scheme='public', type='usual'))
    dbs_admin.execute(sub.get_table().create_script())

# %%
def compilable_tree_dictionary(object): 
    g_dict=\
    {'admunit':{'object':object},\
    'admunit__tree':{'object':'admunit','function':'return_graph_representation'},\
    'admunit__tree__level3':{'function':select_nodes_from_graph,'parameters':['admunit__tree','level',3]},\
    'admunit__tree__level4':{'function':select_nodes_from_graph,'parameters':['admunit__tree','level',4]}}
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
'''for i in admunit_cz.read_features_from_table(number=100):
    if len(i)>0:
        for j in i:
            try:
                dbs_admin.execute("update %s.%s set geom=st_geomfromtext('%s') where data->>'gml_id'='%s'" % (admunit_cz.get_table().get_scheme(),admunit_cz.get_table().get_name(),get_ruian_au_feature_geometry_from_wfs(j.get_data()['gml_id']),j.get_data()['gml_id']) )
            except:
                dbs_admin.disconnect()
                dbs_admin.connect()
                print(j.get_data()['gml_id'])
    else:
        break
'''
#obnoveno vsechno az na katastralni uzemi

# %%
G=apply_function(compilable_tree_dictionary(admunit_cz),'admunit__tree')

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
url_adresa=lpis_cz__ds_xml.get_attributes()['url']
for i in re.findall('\[.*?\]',url_adresa):
    if i in list(replacement_dictionary.keys()):
        url_adresa=url_adresa.replace(i,replacement_dictionary[i])
lpis_cz__ds_xml.set_attribute({'url':url_adresa})
del(url_adresa)

# %%
for i in re.findall('\{.*?\}',lpis_cz__ds_xml.get_attributes()['url']): 
    if i[1:-1] in list(compilable_tree_dictionary(admunit_cz).keys()):
        for j in apply_function(compilable_tree_dictionary(admunit_cz),i[1:-1]):
            lpis_cz.append_subgeoconcept(SubGeoConcept(str(j),'LPIS in Czech administrative territorial unit %s ' % str(j),'Feature',lpis_cz.get_attributes(),data_source=DataSource(lpis_cz.get_data_source().get_type(),lpis_cz.get_data_source().get_name(),(dict(lpis_cz.get_data_source().get_attributes(),**{'url':lpis_cz.get_data_source().get_attributes()['url'].replace(i,str(j))})),None,None),supergeoconcept=lpis_cz,table_inheritance=True,subgeoconcepts=[],type='spatial:admin',adm_graph_node=str(j)))

# %%
for s in lpis_cz.get_subgeoconcepts():
    municipality=[i for i in G.neighbors(str(s.get_adm_graph_node()))][0]
    s.create_table(dbs_lpis_cz,name='ob_'+str(municipality),scheme='data',conflict='append',adm_graph_node=municipality)

# %%
lpis_mnichovice=lpis_cz.get_subgeoconcept_by_table_adm_node('538493')

# %%
f=admunit_cz.get_subgeoconcept_by_name('Obce').read_features_from_table_by_sqlcondition('id=538493',1)

# %%
admunit_cz.get_subgeoconcept_by_name('Obce').get_table().get_scheme()

# %%
f=next(f)[0]

# %%
ogr.CreateGeometryFromWkb(binascii.unhexlify(f.get_geometry())).GetEnvelope()

# %%
wgs84_sr=osr.SpatialReference()
wgs84_sr.ImportFromProj4('+proj=longlat +datum=WGS84 +no_defs')

homolosine_sr=osr.SpatialReference()
homolosine_sr.ImportFromProj4('+proj=igh +lat_0=0 +lon_0=0 +datum=WGS84 +units=m +no_defs')

sjtsk5514_sr=osr.SpatialReference()
sjtsk5514_sr.ImportFromProj4('+proj=krovak +lat_0=49.5 +lon_0=24.83333333333333 +alpha=30.28813975277778 +k=0.9999 +x_0=0 +y_0=0 +ellps=bessel +units=m +towgs84=570.8,85.7,462.8,4.998,1.587,5.261,3.56 +no_defs')

sjtsk5514_to_wgs84=osr.CoordinateTransformation(sjtsk5514_sr,wgs84_sr)
wgs84_to_homolosine=osr.CoordinateTransformation(wgs84_sr,homolosine_sr)
homolosine_to_wgs84=osr.CoordinateTransformation(homolosine_sr,wgs84_sr)

# %%
f.transform_geometry(sjtsk5514_to_wgs84)

# %%
bbox=ogr.CreateGeometryFromWkt(f.get_geometry()).GetEnvelope()

# %%
mnichovice_bbox=(bbox[0]-0.1,bbox[3]+0.1,bbox[1]+0.1,bbox[2]-0.1)

# %%
mnichovice_bbox_homolosine=(*wgs84_to_homolosine.TransformPoint(*mnichovice_bbox[0:2])[0:2],*wgs84_to_homolosine.TransformPoint(*mnichovice_bbox[2:4])[0:2])

# %%
res = 250 

# %%
location = "https://files.isric.org/soilgrids/latest/data/"

# %%
sg_url = f"/vsicurl?max_retry=3&retry_delay=1&list_dir=no&url={location}"

# %%
kwargs = {'format': 'GTiff', 'projWin': mnichovice_bbox_homolosine, 'projWinSRS': homolosine_sr.ExportToProj4(), 'xRes': res, 'yRes': res, 'creationOptions': ["TILED=YES", "COMPRESS=DEFLATE", "PREDICTOR=2", "BIGTIFF=YES"]}


# %%
ds = gdal.Translate('ocd_0_5cm_mean_mnichovice.tif', 
                    sg_url + 'ocd/ocd_0-5cm_mean.vrt', 
                    **kwargs)

# %%
del(ds)

# %%
statisticka_hodnota='mean'

hloubky=[0,5,15,30,60,100]
j_hloubky='cm'

intervaly1=[(hloubky[i],hloubky[i+1]) for i in range(len(hloubky)-1)]
intervaly2=[(0,30)]

slovnik_pudnich_parametru={'ocd':{'nazev':'organic carbon density','hloubky':intervaly1},
'ocs': {'nazev':'organic carbon stocks','hloubky':intervaly2},
'phh2o':{'nazev':'pH water','hloubky':intervaly1},'nitrogen':{'nazev':'nitrogen','hloubky':intervaly1}}

# %%
for key, value in slovnik_pudnich_parametru.items():
    for hloubka in value['hloubky']:
        ds = gdal.Translate('mnichovice_'+key+'_'+str(hloubka[0])+'-'+str(hloubka[1])+j_hloubky+'_'+statisticka_hodnota+'.tif', 
                    sg_url + key+'/'+key+'_'+str(hloubka[0])+'-'+str(hloubka[1])+j_hloubky+'_'+statisticka_hodnota+'.vrt', 
                    **kwargs)
        del(ds)
        time.sleep(5)