from tridy import GeoConcept, SubGeoConcept, MetaData, DBStorage, DataSource, Imagee, Grid, ds_from_metadata, lpis_cz__posledni_aktualizace, apply_function, select_nodes_from_graph, unzip_file, transform_name_to_postgresql_format
import datetime
import re
import sys
import json

from osgeo import gdal,  ogr, osr

from credentials import connectionstring_localhost

import numpy as np
from pygeoprocessing import routing
from tempfile import TemporaryDirectory

np.set_printoptions(threshold=sys.maxsize)

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

def find_neighbors_level(graph,start_node,level):
    if graph.nodes()[start_node]['level']==level:
        yield start_node
    else:
        for n in graph.neighbors(start_node):
            yield from find_neighbors_level(graph,n,level) 
    
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

def geomorphology_transformations(ogr_olu_object, attributes_table, attributes2object_table, attributes_fid,  dem_im,  slope_im,  aspect_im,  twi_im):
    o_dict=\
    {
    'feature':{'object':ogr_olu_object},\
    'feature_json':{'function':(lambda y: json.loads(y.ExportToJson())),'parameters':['feature']},\
    'feature_geometry':{'function':(lambda y: y.GetGeometryRef().ExportToWkt() ),'parameters':['feature']},\
    'feature_id':{'function':(lambda y: int(y['properties']['fid'])),'parameters':['feature_json']},\
    'attributes_table':{'object':attributes_table},\
    'attributes2object_table':{'object':attributes2object_table},\
    'attributes_fid':{'object':attributes_fid},\
    'dem_image':{'object':dem_im},\
    'slope_image':{'object':slope_im},\
    'azimuth_image':{'object':aspect_im},\
    'twi_image':{'object':twi_im},\
    'attribute_properties':{'function':(lambda u, v, w, x,y,z: {**{'fid':u},**{'dataset_id':5},**{'atts':{'elevation':Imagee(*w.clip_by_shape(v)).get_statistics()['mean'], 'slope':Imagee(*x.clip_by_shape(v)).get_statistics()['mean'], 'azimuth':Imagee(*y.clip_by_shape(v)).get_statistics()['mean'], 'twi':Imagee(*z.clip_by_shape(v)).get_statistics()['mean']} } }),'parameters':['attributes_fid','feature_geometry','dem_image', 'slope_image', 'azimuth_image', 'twi_image']},
    'attributes_insert_statement':{'function':(lambda x, y,z: "INSERT INTO %s (fid,atts,dataset_fid) VALUES (%s,('%s'::json),%s)" % (x,y,json.dumps(z['atts']),z['dataset_id']) ),'parameters':['attributes_table','attributes_fid','attribute_properties']},\
    'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,4)" % (z,x,y) ),'parameters':['feature_id','attributes_fid','attributes2object_table']},\
    'two_insert_statements':{'function':(lambda y,z: [y,z]),'parameters':['attributes_insert_statement','attributes2object_insert_statement']},\
    }
    return o_dict
    

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

dbs_admin_connection={**{'dbname':'olu_administrative_units'},**connectionstring_localhost}
dbs_admin=DBStorage(dbs_admin_connection)
dbs_admin.connect()
dbs_admin.disconnect()
dbs_admin.connect()

dbs_olu_connection={**{'dbname':'oluv20'},**connectionstring_localhost}
dbs_olu=DBStorage(dbs_olu_connection)
dbs_olu.connect()
dbs_olu.disconnect()
dbs_olu.connect()

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

admunit_cz.create_table(dbs_admin, name='default',scheme='cz',conflict='append')

admunit_cz__ds.set_data_file('20201031_ST_UKSG.xml')

concept_list=['Staty','Vusc','Okresy','Obce','KatastralniUzemi']
concept_additional_attributes={'Staty':{'level_value':0,'parent_value':'null','id_attribute':'Kod'},
                               'Vusc':{'level_value':1,'parent_value':'1','id_attribute':'Kod'},
                               'Okresy':{'level_value':2,'parent_attribute':'VuscKod','id_attribute':'Kod'},
                               'Obce':{'level_value':3,'parent_attribute':'OkresKod','id_attribute':'Kod'},
                               'KatastralniUzemi':{'level_value':4,'parent_attribute':'ObecKod','id_attribute':'Kod'}}
                               
for l in list(set(concept_list).intersection(set(admunit_cz.get_data_source().list_layers()))):
    admunit_cz.append_subgeoconcept(SubGeoConcept(l,l,'AdmUnitFeature',admunit_cz.get_attributes(),data_source=DataSource(admunit_cz.get_data_source().get_type(),admunit_cz.get_data_source().get_name(),({**admunit_cz.get_data_source().get_attributes(),**{'layer':l}}),None,admunit_cz.get_data_source().get_data_file()),supergeoconcept=admunit_cz,table_inheritance=False,type='semantic',subgeoconcepts=[]))


G=apply_function(compilable_node_dictionary(admunit_cz),'admunit__tree')


eudem_cz__metadata=MetaData('EU DEM in Czech Republic', {"local":"/home/jupyter-dima/eu_dem_czat3035.tif", "format":"GTiff"},'raster data')
eudem_cz_ds=ds_from_metadata(eudem_cz__metadata)
eudem_cz=GeoConcept('EU DEM in Czech Republic','EU DEM in Czech Republic.', 'Feature',json_feature_with_raster_structure, data_source=eudem_cz_ds,subgeoconcepts=[])
eudem_cz.set_raster_output_backend('',eudem_cz.get_data_source().get_attributes()['local'])

f=admunit_cz.read_features_from_table_by_sqlcondition('id=538493',1)
ff=next(f)[0]
ff.transform_geometry(sjtsk5514_to_wgs84)
ff.transform_geometry(wgs84_to_etrs3035)

bbox=ogr.CreateGeometryFromWkt(ff.get_geometry()).Buffer(10).GetEnvelope()

im=eudem_cz.read_raster_output_backend(1,(bbox[0],bbox[3],bbox[1],bbox[2]))
im_slope=Imagee(*im.calculate_slope())
im_azimuth=Imagee(*im.calculate_azimuth())
im_twi=get_twi(im)

olu_atts_fid_maxvalue=dbs_olu.execute('SELECT max(fid) FROM olu2.olu_attribute_set')[0][0]
dbs_olu.execute("select setval('olu2.olu_attributes_fid_seq', %s)" % olu_atts_fid_maxvalue)
olu_atts_id_gen=sequence_generator(dbs_olu.execute('SELECT last_value FROM olu2.olu_attributes_fid_seq')[0][0])

dbs_olu_ogr_conn=dbs_olu.create_ogr_connection()

lyr=dbs_olu_ogr_conn.GetLayerByName('olu_object.538493')

for c in range(lyr.GetFeatureCount()):
    lyr_f=lyr.GetNextFeature()
    lyr_f.GetGeometryRef().Transform(wgs84_to_etrs3035)
    attributes_fid=next(olu_atts_id_gen)
    attributes_table='olu_attribute_set."538493"'
    attributes2object_table='atts_to_object."538493"'
    try:
        lyr_f_im=Imagee(*im.clip_by_shape(lyr_f.GetGeometryRef().ExportToWkt()))
        [dbs_olu.execute(i) for i in apply_function(geomorphology_transformations(lyr_f,attributes_table,attributes2object_table,attributes_fid,im,im_slope,im_azimuth,im_twi),'two_insert_statements')]
    except:
        print(lyr_f.GetFID())
        continue

dbs_olu.execute("SELECT setval('olu2.olu_attributes_fid_seq', %s)" % next(olu_atts_id_gen))
