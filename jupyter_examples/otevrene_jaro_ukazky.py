# %%
import os
import tridy
from tridy import GeoConcept, SubGeoConcept, MetaData, Table, View, DBStorage, DataSource, Feature, FeatureWithID,  AdmUnitFeature, OLUFeature, Grid, Imagee, ds_from_metadata, xml_lpis_cz_reader, lpis_cz__posledni_aktualizace, get_listvalues_from_generator, apply_function, select_nodes_from_graph, unzip_file, find_neighbors_till, connection_parameters_to_pg, world_to_pixel 
from importlib import reload
import requests
import datetime
import re

# %%
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from osgeo import ogr, osr, gdal
import numpy as np
import json

from ipyleaflet import Map, GeoJSON, Marker
from ipywidgets import HTML

from PIL import Image
import matplotlib.pyplot as plt
from matplotlib.pyplot import imshow

from pygeoprocessing import routing

import tempfile
from tempfile import TemporaryDirectory

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
    'admunit__tree__neighbors__3201__4':{'function':find_neighbors_level,'parameters':['admunit__tree__reverse','3201',4]},\
    'admunit__tree__neighbors__3201__3':{'function':find_neighbors_level,'parameters':['admunit__tree__reverse','3201',3]},\
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
s = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[ 502, 503, 504 ])
s.mount('http://', HTTPAdapter(max_retries=retries))

# %%
replacement_dictionary = {"[posledni_den_mesice]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d'),"[lpis_cz__posledni_aktualizace]":lpis_cz__posledni_aktualizace().strftime('%Y%m%d'), "[vcera]":(datetime.datetime.today().replace(day=1)-datetime.timedelta(days=1)).strftime('%Y%m%d')} 

# %%
json_feature_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_feature_with_bigid_structure=[{"name":"id","type":"bigint primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_admin_unit_structure=[{"name":"id","type":"integer primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_admin_unit_structure_at=[{"name":"id","type":"text primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"level","type":"integer"},{"name":"parent_id","type":"text"}]
json_feature_with_raster_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"raster_maps","type":"raster"}]

# %%
admunit_cz__metadata=MetaData('Uzemni celky v CR',
                              {"url":"https://vdp.cuzk.cz/vymenny_format/soucasna/[posledni_den_mesice]_ST_UKSG.xml.zip",
                               "format":"GML", "compression":"zip"},'data')

# %%
admunit_cz__ds=ds_from_metadata(admunit_cz__metadata)

# %%
admunit_cz=GeoConcept('Uzemni celky v CR','Uzemni celky v CR. Vsechny urovne.',
                      'AdmUnitFeature',json_admin_unit_structure, data_source=admunit_cz__ds, subgeoconcepts=[] )

# %%
url_adresa=admunit_cz.get_data_source().get_attributes()['url']
for i in re.findall('\[.*?\]',url_adresa):
    if i in list(replacement_dictionary.keys()):
        url_adresa=url_adresa.replace(i,replacement_dictionary[i])

# %%
admunit_cz.get_data_source().set_attribute({'url':url_adresa})
del(url_adresa)

# %%
#admunit_cz.get_data_source().download_data('archive.zip',s,'all',(os.getcwd()+'/'))
admunit_cz.get_data_source().set_data_file('20210131_ST_UKSG.xml')

# %%
concept_list=['Staty','Vusc','Okresy','Obce','KatastralniUzemi']

# %%
concept_additional_attributes={'Staty':{'level_value':0,'parent_value':'null','id_attribute':'Kod'},
                               'Vusc':{'level_value':1,'parent_value':'1','id_attribute':'Kod'},
                               'Okresy':{'level_value':2,'parent_attribute':'VuscKod','id_attribute':'Kod'},
                               'Obce':{'level_value':3,'parent_attribute':'OkresKod','id_attribute':'Kod'},
                               'KatastralniUzemi':{'level_value':4,'parent_attribute':'ObecKod','id_attribute':'Kod'}}

# %%
for l in list(set(concept_list).intersection(set(admunit_cz.get_data_source().list_layers()))):
    admunit_cz.append_subgeoconcept(SubGeoConcept(l,l,'AdmUnitFeature',admunit_cz.get_attributes(),data_source=DataSource(admunit_cz.get_data_source().get_type(),admunit_cz.get_data_source().get_name(),({**admunit_cz.get_data_source().get_attributes(),**{'layer':l}}),None,admunit_cz.get_data_source().get_data_file()),supergeoconcept=admunit_cz,table_inheritance=False,type='semantic',subgeoconcepts=[]))

# %%
wgs84_sr=osr.SpatialReference()
wgs84_sr.ImportFromProj4('+proj=longlat +datum=WGS84 +no_defs')

sjtsk5514_sr=osr.SpatialReference()
sjtsk5514_sr.ImportFromProj4('+proj=krovak +lat_0=49.5 +lon_0=24.83333333333333 +alpha=30.28813975277778 +k=0.9999 +x_0=0 +y_0=0 +ellps=bessel +units=m +towgs84=570.8,85.7,462.8,4.998,1.587,5.261,3.56 +no_defs')

sjtsk5514_to_wgs84=osr.CoordinateTransformation(sjtsk5514_sr,wgs84_sr)

# %%
for sub in admunit_cz.get_subgeoconcepts():
    sub.set_geojson_output_backend(os.getcwd()+'/',sub.get_name()+'.geojson')
    #with open(sub.get_geojson_output_backend(), 'w', encoding='utf-8') as file:
    #    geojson={"type": "FeatureCollection", "features": [] }
    #    features=sub.get_data_source().read_features('admunitfeature',concept_additional_attributes[sub.get_data_source().get_attributes()['layer']],number=10)
    #    for f in features:
    #            if len(f)>0:
    #                for feature in f:
    #                    feature.transform_geometry(sjtsk5514_to_wgs84)
    #                    geojson["features"].append(feature.export_to_geojson())
    #    json.dump(geojson, file, ensure_ascii=False, indent=4)

# %%
admunit_cz.get_subgeoconcept_by_name('Okresy').get_geojson_output_backend()

# %%
m=Map(center=(49.98,14.73),zoom=10)

# %%
with open(admunit_cz.get_subgeoconcept_by_name('Okresy').get_geojson_output_backend(),'r', encoding='utf-8') as f:
    data = json.load(f)

# %%
features = data['features']
for i in range(len(features)):
    location=(features[i]['geometry']['coordinates'][1],features[i]['geometry']['coordinates'][0])
    nazvy = features[i]['properties']['Nazev']
    id = features[i]['properties']['gml_id']
    html = """
    <p>
      <h4><b>Okres</b>:        """ + " ".join(nazvy) + """</h4>
    </p>
    <p>
      <h5><b>ID</b>:        """ + " ".join(id) + """</h5>
    </p>
    """
    marker = Marker(location=location)
    # Popup associated to a layer
    marker.popup = HTML(html)
    m.add_layer(marker)

# %%
display(m)

# %%
G=apply_function(compilable_node_dictionary(admunit_cz),'admunit__tree')

# %%
list(find_neighbors_level(G.reverse(),'3201',4))

# %%
lpis_cz__metadata=MetaData('LPIS v okrese Benesov',
                              [{"url":"http://eagri.cz/public/app/eagriapp/lpisdata/[lpis_cz__posledni_aktualizace]-{admunit__tree__neighbors__3201__4}-DPB-SHP.zip",
                               "format":"SHP", "compression":"zip"},{"url":"http://eagri.cz/public/app/eagriapp/lpisdata/[lpis_cz__posledni_aktualizace]-{admunit__tree__neighbors__3201__4}-DPB-XML-A.zip",
                               "format":"XML", "compression":"zip"}],'data')
lpis_cz__ds_xml=ds_from_metadata(lpis_cz__metadata,format='XML')
lpis_cz=GeoConcept('LPIS v okrese Benesov','LPIS v okrese Benesov. Data ze vsech katastralnich uzemi v okrese.',
                      'Feature',json_feature_structure, data_source=lpis_cz__ds_xml, subgeoconcepts=[], adm_graph_node='3201')

# %%
url_adresa=lpis_cz.get_data_source().get_attributes()['url']
for i in re.findall('\[.*?\]',url_adresa):
    if i in list(replacement_dictionary.keys()):
        url_adresa=url_adresa.replace(i,replacement_dictionary[i])
lpis_cz.get_data_source().set_attribute({'url':url_adresa})
del(url_adresa)

# %%
for i in re.findall('\{.*?\}',lpis_cz.get_data_source().get_attributes()['url']): 
    if i[1:-1] in list(compilable_node_dictionary(admunit_cz).keys()):
        for j in apply_function(compilable_node_dictionary(admunit_cz),i[1:-1]):
                lpis_cz.append_subgeoconcept(SubGeoConcept(str(j),'LPIS in Czech administrative territorial unit %s ' % str(j),'Feature',lpis_cz.get_attributes(),data_source=DataSource(lpis_cz.get_data_source().get_type(),lpis_cz.get_data_source().get_name(),(dict(lpis_cz.get_data_source().get_attributes(),**{'url':lpis_cz.get_data_source().get_attributes()['url'].replace(i,str(j))})),None,None),supergeoconcept=lpis_cz,table_inheritance=True,subgeoconcepts=[],type='spatial:admin',adm_graph_node=str(j)))

# %%
len(lpis_cz.get_subgeoconcepts())

# %%
for sub in lpis_cz.get_subgeoconcepts():
    sub.set_geojson_output_backend(os.getcwd()+'/',sub.get_name()+'.geojson')
    #sub.get_data_source().download_data('archive.zip',s,'all',(os.getcwd()+'/'))
    #with open(sub.get_geojson_output_backend(), 'w', encoding='utf-8') as file:
    #    geojson={"type": "FeatureCollection", "features": [] }
    #    features=sub.get_data_source().read_features('feature',number=10,reader=xml_lpis_cz_reader)
    #    for f in features:
    #            if len(f)>0:
    #                for feature in f:
    #                    feature.transform_geometry(sjtsk5514_to_wgs84)
    #                    geojson["features"].append(feature.export_to_geojson())
    #    json.dump(geojson, file, ensure_ascii=False, indent=4)
    #os.remove(sub.get_data_source().get_data_file())

# %%
features_gen=lpis_cz.read_features_from_geojson_output_backend('feature',number=1)
feature_gen=next(features_gen)
feature=next(feature_gen)[0]
feature.get_data()

# %%
with open(lpis_cz.get_subgeoconcept_by_name('602191').get_geojson_output_backend(),'r', encoding='utf-8') as f:
    data = json.load(f)

# %%
geo_json = GeoJSON(data=data, style = {'color': 'green'})
m.add_layer(geo_json)

# %%
display(m)

# %%
for feature in data['features']:
    if feature['properties']['KULTURAID']!='99':
        data['features'].remove(feature)

# %%
geo_json = GeoJSON(data=data, style = {'color': 'red'})
m.add_layer(geo_json)

# %%
display(m)

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

# %%
features_gen=lpis_cz.read_features_from_geojson_output_backend('feature',number=1)
feature_gen=next(features_gen)
feature=next(feature_gen)[0]
feature.get_geometry()

# %%
dem30_fn='eu_dem_czat3035.tif'
dem30=gdal.Open(dem30_fn)

# %%
etrs3035_sr=osr.SpatialReference()

etrs3035_sr.ImportFromProj4('+proj=laea +lat_0=52 +lon_0=10 +x_0=4321000 +y_0=3210000 +ellps=GRS80 +towgs84=0,0,0,0,0,0,0 +units=m +no_defs')

wgs_84_to_etrs3035=osr.CoordinateTransformation(wgs84_sr,etrs3035_sr)

# %%
metadata_dict={}
metadata_dict['affine_transformation']=dem30.GetGeoTransform()
metadata_dict['nodata']=dem30.GetRasterBand(1).GetNoDataValue()
metadata_dict['proj_wkt']=etrs3035_sr.ExportToWkt()

# %%
grid=Grid((metadata_dict['affine_transformation'][0],metadata_dict['affine_transformation'][3]),(metadata_dict['affine_transformation'][1],metadata_dict['affine_transformation'][5]))

# %%
feature_geometry=ogr.CreateGeometryFromWkt(feature.get_geometry())

# %%
feature_geometry.Transform(wgs_84_to_etrs3035)

# %%
feature_geometry.ExportToWkt()

# %%
feature_geometry_buffer=feature_geometry.Buffer(500)

# %%
origin=tuple(grid.find_index((feature_geometry_buffer.GetEnvelope()[0],feature_geometry_buffer.GetEnvelope()[3])))
size=tuple(np.array(grid.find_index((feature_geometry_buffer.GetEnvelope()[1],feature_geometry_buffer.GetEnvelope()[2])))-np.array(grid.find_index((feature_geometry_buffer.GetEnvelope()[0],feature_geometry_buffer.GetEnvelope()[3]))))+np.array([1,1])
a=np.array(dem30.GetRasterBand(1).ReadAsArray(xoff=int(origin[0]),yoff=int(origin[1]),win_xsize=int(size[0]),win_ysize=int(size[1])))
grid_clip=Grid(np.array(grid.get_gridorigin())+np.array(tuple(grid.find_index((feature_geometry_buffer.GetEnvelope()[0],feature_geometry_buffer.GetEnvelope()[3]))))*np.array(grid.get_gridstepsize()),(metadata_dict['affine_transformation'][1],metadata_dict['affine_transformation'][5]))

# %%
clip_metadata_dict={}
clip_metadata_dict['affine_transformation']=grid_clip.get_affinetransformation()
clip_metadata_dict['nodata']=float(np.min(a)) if np.min(a)<0 else np.nan
clip_metadata_dict['proj_wkt']=etrs3035_sr.ExportToWkt()

# %%
im=Imagee(a,clip_metadata_dict)

# %%
im.export_as_tif('pole_s_bufrem500.tif')

# %%
im_twi=get_twi(im)

# %%
im_twi.export_as_tif('akumulace_vody_na_poli_s_bufrem500.tif')

# %%
cropped_im=Imagee(*im.clip_by_shape(feature_geometry.ExportToWkt()))

# %%
plt.imshow(cropped_im.get_data(), origin="upper", cmap='gray', interpolation='nearest')
plt.colorbar()
plt.show()

# %%
cropped_im.get_statistics()

# %%
plt.imshow(Imagee(*cropped_im.calculate_slope()).get_data(), origin="upper", cmap='gray', interpolation='nearest')
plt.colorbar()
plt.show()

# %%
plt.imshow(Imagee(*cropped_im.calculate_azimuth()).get_data(), origin="upper", cmap='gray', interpolation='nearest')
plt.colorbar()
plt.show()

# %%
cropped_im__twi=Imagee(*im_twi.clip_by_shape(feature_geometry.ExportToWkt()))

# %%
plt.imshow(cropped_im__twi.get_data(), origin="upper", cmap='gray', interpolation='nearest')
plt.colorbar()
plt.show()

# %%
cropped_im__twi.get_statistics()

# %%
cropped_im__twi.export_as_tif('akumulace_vody_na_poli.tif')

# %%
