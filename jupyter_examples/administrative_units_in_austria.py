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

import random
from ipyleaflet import Map, GeoJSON

# %%
del(GeoConcept, SubGeoConcept, MetaData, Table, View, DBStorage, DataSource, Feature, FeatureWithID, AdmUnitFeature, OLUFeature, Grid, Imagee, ds_from_metadata,xml_lpis_cz_reader,get_listvalues_from_generator,apply_function,select_nodes_from_graph,world_to_pixel)
reload(tridy)
from tridy import GeoConcept, SubGeoConcept, MetaData, Table, View, DBStorage, DataSource, Feature, FeatureWithID, AdmUnitFeature, OLUFeature, Grid, Imagee, ds_from_metadata, xml_lpis_cz_reader, get_listvalues_from_generator, apply_function, select_nodes_from_graph,world_to_pixel

# %%
def random_color(feature):
    return {
        'color': 'black',
        'fillColor': random.choice(['red', 'yellow', 'green', 'orange']),
    }

# %%
#administrative territorial units
dbs_admin_connection={'dbname':'olu_administrative_units','user':'euxdat_admin','host':'euxdat-db-svc','port':'5432','password':'Euxdat12345'}
dbs_admin=DBStorage(dbs_admin_connection)
dbs_admin.connect()
dbs_admin.disconnect()
dbs_admin.connect()

# %%
replacement_dictionary = {"[posledni_den_mesice]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d'),"[lpis_cz__posledni_aktualizace]":lpis_cz__posledni_aktualizace().strftime('%Y%m%d'), "[vcera]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d')} 
json_feature_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_feature_with_bigid_structure=[{"name":"id","type":"bigint primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_admin_unit_structure=[{"name":"id","type":"integer primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_admin_unit_structure_at=[{"name":"id","type":"text primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_feature_with_raster_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"raster_maps","type":"raster"}]

# %%
admunit_at__metadata=MetaData('Administrative units in Austria',
                             {"url":"http://gis.lesprojekt.cz/admunit_at.zip", "format":"ESRI Shapefile","compression":"zip"},
                             'data')

admunit_at__ds=ds_from_metadata(admunit_at__metadata)

admunit_at=GeoConcept('Administrative units in Austria','Administrative units in Austria. All levels.',
                      'AdmUnitFeature',json_admin_unit_structure_at, data_source=admunit_at__ds, subgeoconcepts=[] )

# %%
#admunit_at.get_data_source().download_data('admunit_at.zip', s, 'all', folder='admunit_at/')
#admunit_at.get_data_source().set_data_file([file for file in admunit_at.get_data_source().get_data_file() if file.endswith('shp')][0])
admunit_at.get_data_source().set_data_file('admunit_at/AU_AdministrativeUnit_extended.shp')

# %%
concept_list=['1stOrder','2ndOrder','3rdOrder','4thOrder','5thOrder']
concept_additional_attributes={'1stOrder':{'level_value':0,'parent_value':'null','id_attribute':'inspireId'},
                               '2ndOrder':{'level_value':1,'parent_attribute':'AU_Attri_6','id_attribute':'inspireId'},
                               '3rdOrder':{'level_value':2,'parent_attribute':'AU_Attri_6','id_attribute':'inspireId'},
                               '4thOrder':{'level_value':3,'parent_attribute':'AU_Attri_6','id_attribute':'inspireId'},
                               '5thOrder':{'level_value':4,'parent_attribute':'AU_Attri_6','id_attribute':'inspireId'}}

# %%
for l in concept_list:
    admunit_at.append_subgeoconcept(SubGeoConcept(l,l,'AdmUnitFeature',admunit_at.get_attributes(),data_source=DataSource(admunit_at.get_data_source().get_type(),admunit_at.get_data_source().get_name(),({**admunit_at.get_data_source().get_attributes(),**{'attribute_filter':"AU_Attri_2='%s'"%l}}),None,admunit_at.get_data_source().get_data_file()),supergeoconcept=admunit_at,table_inheritance=False,subgeoconcepts=[]))

# %%
admunit_at.create_table(dbs_admin, name='default',scheme='at',conflict='append')

# %%
#for sub in admunit_at.get_subgeoconcepts():
#    features=sub.get_data_source().read_features('admunitfeature',concept_additional_attributes[sub.get_name()],number=10)
#    dbs_admin.insert_many('insert into %s.%s (geom,data,id,level,parent_id) ' % (admunit_at.get_table().get_scheme(),admunit_at.get_table().get_name()) ,features,20)

# %%
for sub in admunit_at.get_subgeoconcepts():
    sub.set_table(View(transform_name_to_postgresql_format(sub.get_name()),sub.get_attributes(), sub.get_supergeoconcept().get_table(),"level=%s" % (concept_additional_attributes[sub.get_name()]['level_value']), dbs=dbs_admin, scheme='public', type='usual'))
    dbs_admin.execute(sub.get_table().create_script())

# %%
sub=admunit_at.get_subgeoconcept_by_name('2ndOrder')

# %%
with open('2ndOrder.geojson', 'w', encoding='utf-8') as file:
        geojson={"type": "FeatureCollection", "features": [] }
        features=sub.read_features_from_table(100)
        for f in features:
                if len(f)>0:
                    for feature in f:
                        geojson["features"].append(feature.export_to_geojson())
                else:
                    break
        json.dump(geojson, file, ensure_ascii=False, indent=4)

# %%
with open('2ndOrder.geojson', 'r') as f:
    data = json.load(f)
    
m = Map(center=(47.8,13), zoom=7)
geo_json = GeoJSON(
    data=data,
    style={
        'opacity': 1, 'dashArray': '9', 'fillOpacity': 0.1, 'weight': 1
    },
    hover_style={
        'color': 'white', 'dashArray': '0', 'fillOpacity': 0.5
    },
    style_callback=random_color
)

m.add_layer(geo_json)

m

# %%
