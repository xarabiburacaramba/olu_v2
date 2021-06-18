import psycopg2
import psycopg2.extras
import requests
import zipfile
import os
import json
from osgeo import gdal,  gdalnumeric,  ogr,  osr
import math
import numpy as np
import pandas as pd
from numpy import ma
from PIL import Image, ImageDraw
import networkx as nx
from lxml import etree
from shutil import copyfile
import datetime
from io import BytesIO
import re
from scipy.stats import mode
from scipy.ndimage import morphological_laplace

def create_folder_if_not_exists(target):
    """Create target folder"""
    if not os.path.exists(target):
        os.makedirs(target)

def transform_wkt_geometry(geom_wkt, osr_coordinatetransformation):
    geom=ogr.CreateGeometryFromWkt(geom_wkt)
    geom.Transform(osr_coordinatetransformation)
    return geom.ExportToWkt()


def transform_name_to_postgresql_format(name):
    if name[0].isdigit():
        return '"{}"'.format(name)
    else:
        return name
        
def transform_column_name_to_postgresql_format(name):
    return re.sub('[^a-zA-Z0-9_]', '_', name)

def precision_and_scale(x):
 max_digits = 14
 int_part = int(abs(x))
 magnitude = 1 if int_part == 0 else int(math.log10(int_part)) + 1
 if magnitude >= max_digits:
  return (magnitude, 0)
 frac_part = abs(x) - int_part
 multiplier = 10 ** (max_digits - magnitude)
 frac_digits = multiplier + int(multiplier * frac_part + 0.5)
 while frac_digits % 10 == 0:
  frac_digits /= 10
 scale = int(math.log10(frac_digits))
 return (magnitude + scale, scale)

def world_to_pixel(geo_matrix, x, y):
 '''
 Uses a gdal geomatrix (gdal.GetGeoTransform()) to calculate
 the pixel location of a geospatial coordinate; from:
 http://pcjericks.github.io/py-gdalogr-cookbook/raster_layers.html#clip-a-geotiff-with-shapefile
 '''
 ulX = geo_matrix[0]
 ulY = geo_matrix[3]
 xDist = geo_matrix[1]
 yDist = geo_matrix[5]
 rtnX = geo_matrix[2]
 rtnY = geo_matrix[4]
 pixel = math.floor((x - ulX) / xDist)
 line = math.floor((y - ulY) / yDist)
 return (pixel, line)

def custom_round(x, base=.25, prec=2):
  return round(base * round(float(x)/base),prec)
  
def custom_ceiling(x, base=.25, prec=2):
  return round(base * math.ceil(float(x)/base),prec)
  
def custom_floor(x, base=.25, prec=2):
  return round(base * math.floor(float(x)/base),prec)

def connection_parameters_to_pg(connection_parameters):
    return "PG: host=%s dbname=%s user=%s password=%s" % (connection_parameters['host'],connection_parameters['dbname'],connection_parameters['user'],connection_parameters['password'])

def lpis_cz__posledni_aktualizace():
    url='http://eagri.cz/public/app/eagriapp/lpisdata/seznam_varek.txt'
    try:
        r=requests.get(url,stream=False)
    except:
        r=None
        return ('nelze precist seznam varek ze serveru eagri')
    if r.status_code!=200:
        return (str(r.status_code))
    else:
        return datetime.datetime.strptime(BytesIO(r.content).readlines()[-1].decode('utf-8').replace('\n',''),'%Y%m%d')


def select_nodes_from_graph(graph,attribute,value):
    return [x for x,y in graph.nodes(data=True) if (attribute in list(y.keys())) and (y[attribute]==value)]
    
def get_listvalues_from_generator(generator,function):
    l=[]
    for x in generator:
        if len(x)>0:
            [l.append(getattr(i,function)()) for i in x]
        else:
            break
    return l
    
def apply_function(funkce,k):
 if k not in list(funkce.keys()):
  return k 
 elif ('parameters' not in list(funkce[k].keys())) and ('function' not in list(funkce[k].keys())):
  return funkce[k]['object']
 elif 'parameters' not in list(funkce[k].keys()):
  return getattr(apply_function(funkce,funkce[k]['object']),funkce[k]['function'])()
 elif 'object' not in list(funkce[k].keys()):
  return funkce[k]['function'](*  tuple(apply_function(funkce,j) for j in funkce[k]['parameters'] ) )
 else:
  return getattr(apply_function(funkce,funkce[k]['object']),funkce[k]['function'])(*  tuple(apply_function(funkce,j) for j in funkce[k]['parameters'] ) )


def download_file(url,local_filename, requests_session):
    with requests_session.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return local_filename

#def single_insert(feature_prototype):
#   feature=Feature(data=feature_prototype['properties'], geometry=feature_prototype['geometry'], geometry_type='geojson')
#   return feature.convert_to_sql_insert()
    
def single_insert(feature):
   return feature.convert_to_sql_insert()

mass_insert=np.vectorize(single_insert)

#def unzip_file(file):
#	zipfile.ZipFile(file).extract(os.path.splitext(os.path.split(file)[1])[0],os.path.split(file)[0])

def image_to_array(i):
    '''
    Converts a Python Imaging Library (PIL) array to a gdalnumeric image.
    '''
    a = gdalnumeric.fromstring(i.tobytes(), 'b')
    a.shape = i.im.size[1], i.im.size[0]
    return a

def unzip_file(file, member, folder):
    if member=='all':
        zipfile.ZipFile(file).extractall(folder)
        return([(folder+i) for i in zipfile.ZipFile(file).namelist()])
    else:
        zipfile.ZipFile(file).extract(member, folder)
        return(folder+member)

def recursive_dict(element):
    if element.text == None and len(element.attrib):
        return etree.QName(element).localname, element.attrib
    return etree.QName(element).localname, \
        list(map(recursive_dict,element)) or element.text
        
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def ConvertB(b):
    if type(b)==tuple:
        return dict([b])
    elif type(b)==list:
        return dict(b)
    else:
        return b

def Convert(tup, di): 
    for a, b in tup:
        di.setdefault(a, []).append(ConvertB(b)) 
    return di 

def replace_list(di):
    for k,v in di.items():
        if type(v)==list and len(v)==1:
            di[k]=v[0]
    return di

def finditem_in_di(di, key):
    if key in di:
        return di[key]
    for k, v in di.items():
        if isinstance(v,dict):
            return finditem_in_di(v, key)

def find_neighbors_till(graph,start_node,concept):
    if concept.get_subgeoconcept_table_by_adm_node(start_node)!='no tables corresponding to this graph node was found':
        return concept.get_subgeoconcept_table_by_adm_node(start_node)
    elif len(list(graph.neighbors(start_node)))==0:
        return concept.get_table()
    else:
        for n in graph.neighbors(start_node):
            return find_neighbors_till(graph,n,concept) 
            
def find_neighbors_level(graph,start_node,level):
    if graph.nodes()[start_node]['level']==level:
        yield start_node
    else:
        for n in graph.neighbors(start_node):
            yield from find_neighbors_level(graph,n,level) 
#[i for i in find_neighbors_level(G.reverse(),'3408',4)]

def xml_lpis_cz_reader(xml_file,feature_type='feature'):
    '''read for XMLs from LPIS'''
    tree=etree.parse(xml_file)
    root=tree.getroot()
    result=[]
    if ('.//{http://sitewell.cz/lpis/schemas/LPI_GDP01A}Response')!=None:
        for i in root.find('{http://schemas.xmlsoap.org/soap/envelope/}Body').find('{http://www.mze.cz/ESBServer/1.0}ESBServerEnvelopeResponse').find('{http://www.mze.cz/ESBServer/1.0}Data').find('{http://sitewell.cz/lpis/schemas/LPI_GDP01A}Response').getchildren():
            if i.tag=='{http://sitewell.cz/lpis/schemas/LPI_GDP01A}DPB':
                a=recursive_dict(i)
                di={}
                b=replace_list(Convert(a[1],di))
                #geom=a[1]['GEOMETRIE']
                #del(a[1]['GEOMETRIE'])
                #result.append([geom,json.dumps(a[1])])
                if feature_type=='feature':
                    feature=Feature(**{'geom':b['GEOMETRIE'],'data':{x: b[x] for x in b if x!='GEOMETRIE'}})
                result.append(feature)
                del(a,b,di)
    #os.remove(out_file)
    return result


class GeoConcept():
    ''' zaklad. pouziva se hlavne na tvorbu tabulek pro ulozeni prvku v databazi. muze vychazet z datoveho zdroje, anebo muze se vytvaret umele. '''
    def __init__(self, name,  abstract,  feature_type,  attributes=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}],  subgeoconcepts=[],  data_source=None, adm_graph_node=None, wkt_geom=None) :
        self._name=name
        self._abstract=abstract
        self._feature_type=feature_type
        self._attributes=attributes
        self._subgeoconcepts=subgeoconcepts
        self._data_source=data_source
        self._adm_graph_node=adm_graph_node
        self._wkt_geom=wkt_geom
        self._table=None
        
    def get_name(self):
        return self._name
        
    def get_abstract(self):
        return self._abstract
        
    def get_feature_type(self):
        return self._feature_type
        
    def get_attributes(self):
        return self._attributes
        
    def get_attribute(self, key):
        return next(item for item in self._attributes if item['name'] == key)
        
    def set_attributes(self, attributes):
        self._attributes=attributes
        
    def set_attribute(self, attr_dict):
        self._attributes = self._attributes.append(attr_dict)
    
    def get_subgeoconcepts(self):
        return self._subgeoconcepts
        
    def set_subgeoconcepts(self, subgeoconcepts):
        self._subgeoconcepts=subgeoconcepts
        
    def get_data_source(self):
        return self._data_source
        
    def set_data_source(self, data_source):
        self._data_source=data_source
        
    def get_adm_graph_node(self):
       return self._adm_graph_node
       
    def set_adm_graph_node(self, adm_graph_node):
       self._adm_graph_node=adm_graph_node
       
    def get_wkt_geom(self):
       return self._wkt_geom
       
    def set_wkt_geom(self, wkt_geom):
       self._wkt_geom=wkt_geom
        
    def remove_attribute(self, key):
        self._attributes.remove(next(item for item in self._attributes if item['name'] == key))
        
    def append_subgeoconcept(self, subgeoconcept):
        self._subgeoconcepts.append(subgeoconcept)
        
    def set_raster_output_backend(self, folder, name):
        self._raster_backend=folder+name
        
    def get_raster_output_backend(self):
        return self._raster_backend

    def read_raster_output_backend(self, band_number=1, bbox=None):
        driver=gdal.GetDriverByName(self._data_source.get_attributes()['format'])
        dataset = gdal.Open(self._raster_backend)
        metadata_dict={}
        metadata_dict['affine_transformation']=dataset.GetGeoTransform()
        metadata_dict['nodata']=dataset.GetRasterBand(band_number).GetNoDataValue()
        metadata_dict['proj_wkt']=dataset.GetProjectionRef()
        grid=Grid((metadata_dict['affine_transformation'][0],metadata_dict['affine_transformation'][3]),(metadata_dict['affine_transformation'][1],metadata_dict['affine_transformation'][5]),grid_cell_origin='ul')
        if bbox is None:
            im=Imagee((dataset.GetRasterBand(band_number).ReadAsArray()),metadata_dict, grid)
        else:
            #ul_offset=grid.find_index([bbox[0],bbox[1]])
            #lr_offset=grid.find_index([bbox[2],bbox[3]])
            #im_data=dataset.GetRasterBand(band_number).ReadAsArray(xoff=ul_offset[0],yoff=ul_offset[1],win_xsize=(lr_offset[0]-ul_offset[0]) if (lr_offset[0]-ul_offset[0])>0 else 1 ,win_ysize=(ul_offset[1]-lr_offset[1]) if (ul_offset[1]-lr_offset[1])>0 else 1 )
            #im_metadata={**metadata_dict, **{'affine_transformation':(grid.get_gridorigin()[0]+ul_offset[0]*grid.get_gridstepsize()[0], grid.get_gridstepsize()[0], 0.0, grid.get_gridorigin()[1]+ul_offset[1]*grid.get_gridstepsize()[1], 0.0, grid.get_gridstepsize()[1])}}
            #im_grid=Grid((im_metadata['affine_transformation'][0],im_metadata['affine_transformation'][3]),(im_metadata['affine_transformation'][1],im_metadata['affine_transformation'][5]),grid_cell_origin='ul')
            #im=Imagee(im_data, im_metadata, im_grid)
            #del(im_data, im_metadata, im_grid, ul_offset, lr_offset)
            origin=tuple(grid.find_index([bbox[0],bbox[1]]))
            size=tuple(np.array(grid.find_index((bbox[2],bbox[3])))-np.array(grid.find_index((bbox[0],bbox[1]))))+np.array([1,1])
            a=np.array(dataset.GetRasterBand(band_number).ReadAsArray(xoff=int(origin[0]),yoff=int(origin[1]),win_xsize=int(size[0]),win_ysize=int(size[1])))
            im_grid=Grid(np.array(grid.get_gridorigin())+np.array(tuple(grid.find_index((bbox[0],bbox[1]))))*np.array(grid.get_gridstepsize()),(metadata_dict['affine_transformation'][1],metadata_dict['affine_transformation'][5]))
            im_metadata_dict={**metadata_dict,  **{'affine_transformation':im_grid.get_affinetransformation()}}
            im=Imagee(a,im_metadata_dict, im_grid)
        del(driver, dataset, grid)
        return im
        
    def get_imagee(self):
        try:
            return self._imagee
        except:
            return None 
            
    def set_imagee(self, im):
        self._imagee=im
      
    def set_geojson_output_backend(self,  folder,  name,  type='GeoJSON'):
        self._geojson_backend=folder+name
        
    def read_features_from_geojson_output_backend(self, feature_type,  feature_additional_attributes=None,  number=1000):
        if len(self._subgeoconcepts)==0:
            new_ds=DataSource('GeoJSON',self._name,{'format':'GeoJSON'})
            new_ds.set_data_file(self._geojson_backend)
            features=new_ds.read_features(feature_type,feature_additional_attributes, number=number)
            yield features
        else:
            for sub in self._subgeoconcepts:
                new_ds=DataSource('GeoJSON',sub.get_name(),{'format':'GeoJSON'})
                new_ds.set_data_file(sub.get_geojson_output_backend())
                features=new_ds.read_features(feature_type,feature_additional_attributes, number=number)
                yield features

    def get_geojson_output_backend(self):
        return self._geojson_backend

    def create_table(self, dbstorage_object, name='default', scheme='public', conflict='replace', adm_graph_node=None):
        if name=='default':
            name=self._name.replace(' ','_').lower()
        self._table=Table(name, self._attributes, dbstorage_object, scheme, adm_graph_node=adm_graph_node)
        if conflict=='replace':
            dbstorage_object.execute('DROP TABLE IF EXISTS %s CASCADE' % ( transform_name_to_postgresql_format(scheme)+'.'+ transform_name_to_postgresql_format(name) )) 
            dbstorage_object.execute(self._table.create_script())
        else:
            if  dbstorage_object.execute("SELECT EXISTS ( SELECT FROM information_schema.tables WHERE  table_schema = '%s' AND table_name = '%s')" % (scheme,name) )[0][0] == True:
                pass
            else:
                dbstorage_object.execute(self._table.create_script())
        
        
    #def sql_create_master_query(self,  dbstorage_object):
    #   print(self._attributes)
    #  dbstorage_object.execute('create table %s (%s) '  %  (self._table_name, ','.join([str(i[0])+' '+str(i[1]) for i in [[v for k,v  in i.items()] for i in self._attributes]])) )        
    '''
    def create_download_script(self):
        return 'download_script'
        
    def create_upload_script(self,  geoconcept_object,  dbstorage_object):
        return 'create upload to database script'
        
    def create_update_script(self,  geoconcept_object, dbstorage_object,  table_object):
        return 'create regular update script according to datasource metadata update frequency'
        
    def generate_metadata(self):
        return MetaData()
    '''
    def get_subgeoconcept_by_name(self, name):
        subgeoconcept=[]
        for sub in self._subgeoconcepts:
            if sub.get_name()==name:
                subgeoconcept.append(sub)
        if len(subgeoconcept)==0:
            return 'no subgeoconcept with such name was found'
        elif len(subgeoconcept)==1:
            return subgeoconcept[0]
        else:
            return subgeoconcept
            
    def get_subgeoconcept_by_adm_node(self, adm_node):
        subgeoconcept=[]
        for sub in self._subgeoconcepts:
            if sub.get_adm_graph_node()==adm_node:
                subgeoconcept.append(sub)
        if len(subgeoconcept)==0:
            return 'no subgeoconcept with such name was found'
        elif len(subgeoconcept)==1:
            return subgeoconcept[0]
        else:
            return subgeoconcept
            
    def get_subgeoconcept_by_table_adm_node(self, table_adm_node):
        subgeoconcept=[]
        for sub in self._subgeoconcepts:
            if sub.get_table().get_adm_graph_node()==table_adm_node:
                subgeoconcept.append(sub)
        if len(subgeoconcept)==0:
            return 'no subgeoconcept with such name was found'
        elif len(subgeoconcept)==1:
            return subgeoconcept[0]
        else:
            return subgeoconcept
            
    def get_subgeoconcept_table_by_adm_node(self, table_adm_node):
        tables=[]
        for sub in self._subgeoconcepts:
            if sub.get_table().get_adm_graph_node()==table_adm_node:
                if sub.get_table().get_name() not in [t.get_name() for t in tables]:
                    tables.append(sub.get_table())
        if len(tables)==0:
            return 'no tables corresponding to this graph node was found'
        elif len(tables)==1:
            return tables[0]
        else:
            return tables

    def set_table(self, table):
        self._table=table
        
    def set_default_table(self, dbstorage_object, scheme='public'):
        self._table=Table(self._name.replace(' ','_').lower(), self._attributes, dbstorage_object, scheme)
        
    def get_table(self):
        return self._table
        
    def set_feature_representation(self, feature):
        self._feature_representation=feature
        
    def get_feature_representation(self):
        return self._feature_representation
        
    def read_features_from_table(self, number):
        features=self._table.read_features(number, ','.join([i['name'] for i in self._attributes]).replace('geom','st_asbinary(geom) as geom'))
        if self._feature_type=='Feature':
            while True:
                yield [Feature(**{j:i[j] for j in dict(i) if j!='id'},geom_type='wkb') for i in next(features)]
        elif self._feature_type=='FeatureWithID':
            while True:
                yield [FeatureWithID(**i,geom_type='wkb') for i in next(features)]
        elif self._feature_type=='OLUFeature':
            while True:
                yield [OLUFeature(**i,geom_type='wkb') for i in next(features)]
        elif self._feature_type=='AdmUnitFeature':
            while True:
                yield [AdmUnitFeature(**i,geom_type='wkb') for i in next(features)]
                
    def read_features_from_table_by_sqlcondition(self, sqlcondition, number):
        features=self._table.read_features_by_condition(sqlcondition,  number,  ','.join([i['name'] for i in self._attributes]).replace('geom','st_asbinary(geom) as geom'))
        if self._feature_type=='Feature':
            while True:
                yield [Feature(**i,geom_type='wkb') for i in next(features)]
        elif self._feature_type=='FeatureWithID':
            while True:
                yield [FeatureWithID(**i,geom_type='wkb') for i in next(features)]
        elif self._feature_type=='OLUFeature':
            while True:
                yield [OLUFeature(**i,geom_type='wkb') for i in next(features)]
        elif self._feature_type=='AdmUnitFeature':
            while True:
                yield [AdmUnitFeature(**i,geom_type='wkb') for i in next(features)]
                
    def return_graph_representation(self):
        if self._feature_type=='AdmUnitFeature':
            if self._table!=None:
                G=nx.DiGraph()
                for i in self.read_features_from_table(number=5000):
                    if len(i)>0:
                        for j in i:
                            G.add_node(str(j.get_id()), level=j.get_level())
                    else:
                        break
                for i in self.read_features_from_table(number=5000):
                    if len(i)>0:
                        for j in i:
                            G.add_edge(str(j.get_id()), str(j.get_parentid()))
                    else:
                        break
                return G
            else:
                G=nx.DiGraph()
                for i in self.read_features_from_geojson_output_backend('admunitfeature',feature_additional_attributes={'level_attribute':'level','parent_attribute':'parent_id','id_attribute':'Kod'}):
                    for f in i:
                        if len(f)>0:
                            for j in f:
                                G.add_node(str(j.get_id()), level=j.get_level())
                        else:
                            break
                for i in self.read_features_from_geojson_output_backend('admunitfeature',feature_additional_attributes={'level_attribute':'level','parent_attribute':'parent_id','id_attribute':'Kod'}):
                    for f in i:
                        if len(f)>0:
                            for j in f:
                                G.add_edge(str(j.get_id()), str(j.get_parentid()))
                        else:
                            break
            return G
        else:
            return 'just concepts with AdmUnitFeature representation could be exported to networkx graph'
            
    def return_graph_representation_from_geojson_output_backend(self):
        if self._feature_type=='AdmUnitFeature':                
            G=nx.DiGraph()
            for i in self.read_features_from_geojson_output_backend('admunitfeature',feature_additional_attributes={'level_attribute':'level','parent_attribute':'parent_id','id_attribute':'Kod'}):
                for f in i:
                    if len(f)>0:
                        for j in f:
                            G.add_node(str(j.get_id()), level=j.get_level())
                    else:
                        break
            for i in self.read_features_from_geojson_output_backend('admunitfeature',feature_additional_attributes={'level_attribute':'level','parent_attribute':'parent_id','id_attribute':'Kod'}):
                for f in i:
                    if len(f)>0:
                        for j in f:
                            G.add_edge(str(j.get_id()), str(j.get_parentid()))
                    else:
                        break
            return G
        else:
            return 'just concepts with AdmUnitFeature representation could be exported to networkx graph'
            
    def convert_to_sql_insert(self):
        '''function to write self representation to the relational database'''
        

class SubGeoConcept(GeoConcept):
    "vektorovy prvek, reprezentuje administrativne uzemni celek"
    def __init__(self, name,  abstract,  feature_type,  attributes, data_source,  supergeoconcept, adm_graph_node=None, wkt_geom=None, table_inheritance=False,  type='semantic',  subgeoconcepts=[] ):
        super().__init__(name,  abstract,  feature_type,  attributes, subgeoconcepts,  data_source, adm_graph_node, wkt_geom)
        self._supergeoconcept=supergeoconcept
        self._table_inheritance=table_inheritance
        self._type=type
        
    def get_supergeoconcept(self):
        return self._supergeoconcept
        
    def set_supergeoconcept(self, supergeoconcept):
        self._supergeoconcept=supergeoconcept
        
    def get_table_inheritance(self):
        return self._table_inheritance
        
    def set_table_inheritance(self, table_inheritance):
        self._table_inheritance=table_inheritance
        
    def get_type(self):
        return self._type
        
    def create_table(self, dbstorage_object, name='default',  scheme='public', conflict='replace', adm_graph_node=None):
        if name=='default':
            name=self._name.replace(' ','_').lower()
        self._table=Table(name, self._attributes, dbstorage_object, scheme, adm_graph_node, table_inheritance=(None if self._table_inheritance==False else self._supergeoconcept.get_table().get_scheme()+'.'+self._supergeoconcept.get_table().get_name() ))
        if conflict=='replace':
            dbstorage_object.execute('DROP TABLE IF EXISTS %s CASCADE' % ( transform_name_to_postgresql_format(scheme)+'.'+ transform_name_to_postgresql_format(name) )) 
            dbstorage_object.execute(self._table.create_script())
        else:
            if  dbstorage_object.execute("SELECT EXISTS ( SELECT FROM information_schema.tables WHERE  table_schema = '%s' AND table_name = '%s')" % (scheme, name) )[0][0] == True:
                pass
            else:
                dbstorage_object.execute(self._table.create_script())

class AdmUnit(GeoConcept):
    '''administrative unit. dedi z tridy geoconcept . v podstate popisuje prvek administrativne uzemniho celku , ktery muze byt nacten / vlozen z / do databaze. ma byt umozneno jednodussi vytvaret hierarchicky graf na zaklade atributu prvku.  '''
    def __init__(self, name,  abstract,  attributes, other_attributes) :
        super( ).__init__(name,  abstract,  attributes)
        self._other_attributes=other_attributes
        
    def get_other_attributes(self):
        return self._other_attributes
        


class DataSource():
    ''' trida na definici datoveho zdroje pro OLU. '''
    def __init__(self,  type,  name,  attributes,  metadata=None, data_file=None,  olu_id=None,  metatable=None) :
        self._type=type
        self._name=name
        self._metadata=metadata
        self._attributes=attributes
        self._data_file=data_file
        self._olu_id=olu_id
        self._metatable=metatable
        
    def get_type(self):
        return self._type
        
    def set_type(self, type):
        self._type=type
        
    def get_name(self):
        return self._name
        
    def get_metadata(self):
        return self._metadata
        
    def set_metadata(self, metadata):
        self._metadata=metadata
        
    def get_attributes(self):
        return self._attributes
        
    def get_attribute(self, key):
        return next(item for item in self._attributes if item['name'] == key)
        
    def set_attributes(self, attributes):
        self._attributes=attributes
        
    def set_attribute(self, attribute):
        self._attributes={**self._attributes, **attribute}

    def download_data(self, file_name, requests_session, member=None, folder=None):
        '''download file with data and save to disk '''
        if self.get_attributes()['format'] in ('GPKG', 'ESRI Shapefile', 'GML', 'XML'):
            if self.get_attributes()['compression']=='zip':
                archive=download_file(self.get_attributes()['url'], file_name,  requests_session)    
                self._data_file=unzip_file(archive, member, folder)
                if len(self._data_file)==1:
                    self._data_file=self._data_file[0]
                #url_link.rstrip('.zip').split('/')[-1]
            else:
                download_file(self.get_attributes()['url'],  file_name,  requests_session)
                self._data_file=file_name
        else:
                download_file(self.get_attributes()['url'],  file_name,  requests_session)
    
    def get_data_file(self):
        return self._data_file
        
    def set_data_file(self,  data_file):
        self._data_file=data_file
        
    def list_layers(self):
        if self._data_file==None:
            print('Please, set datafile at first!')
        elif isinstance(self._data_file,str):
            driver=ogr.GetDriverByName(self.get_attributes()['format'])
            datasource=driver.Open(self._data_file, 0)
            layers=[ datasource.GetLayerByIndex(i).GetName() for i in range(datasource.GetLayerCount())]
            del(datasource,  driver)
            return layers
        else:
            print('Please, set single datafile from the list at first!')

    def get_layer_srid(self,layer_name):
        if self._data_file==None:
            print('Please, set datafile at first!')
        elif isinstance(self._data_file,str):
            driver=ogr.GetDriverByName(self.get_attributes()['format'])
            datasource=driver.Open(self._data_file, 0)
            layer=datasource.GetLayerByName(layer_name)
            srid=layer.GetSpatialRef().GetName()
            del(datasource,  driver,  layer)
            return srid
        else:
            print('Please, set single datafile from the list at first!')
        
    def read_feature(self, feature_type,feature_additional_attributes, layer_name=None):
        if self._data_file==None:
            print('Please, set datafile at first!')
        elif isinstance(self._data_file,str):
            driver=ogr.GetDriverByName(self.get_attributes()['format'])
            datasource=driver.Open(self._data_file, 0)
            if layer_name==None:
                if 'layer' in list(self._attributes.keys()):
                    layer=datasource.GetLayer([i.GetName() for i in datasource].index(self._attributes['layer']))
                else:
                    layer=datasource.GetLayer()
            else:
                layer=datasource.GetLayer([i.GetName() for i in datasource].index(layer_name))
            feature=layer.GetNextFeature()
            feature_json=json.loads(feature.ExportToJson())
            if feature_type=='feature':
               feature=Feature(data=feature_json['properties'], geom=('POLYGON EMPTY' if feature.GetGeometryRef() is None else feature.GetGeometryRef().ExportToWkt()), geom_type='wkt')
            elif  feature_type=='admunitfeature':
                feature=AdmUnitFeature(data=feature_json['properties'], geom=('POLYGON EMPTY' if feature.GetGeometryRef() is None else feature.GetGeometryRef().ExportToWkt()),  id=(feature_additional_attributes['id_value'] if 'id_value' in list(feature_additional_attributes.keys()) else feature_json['properties'][feature_additional_attributes['id_attribute']]),  level=(feature_additional_attributes['level_value'] if 'level_value' in list(feature_additional_attributes.keys()) else feature_json['properties'][feature_additional_attributes['level_attribute']]), parent_id=(feature_additional_attributes['parent_value'] if 'parent_value' in list(feature_additional_attributes.keys()) else feature_json['properties'][feature_additional_attributes['parent_attribute']]), geom_type='wkt')
            del(feature_json,  layer, datasource)
            return feature
        else:
            print('Please, set single datafile from the list at first!')
            
    def read_features(self,  feature_type,feature_additional_attributes=None,  layer_name=None, attribute_filter=None,  spatial_filter=None, number=1000,  reader=None,  gfs_template=None):
        if self._data_file==None:
            print('Please, set datafile at first!')
        elif isinstance(self._data_file,str):
            if ogr.GetDriverByName(self.get_attributes()['format'])!=None:
                driver=ogr.GetDriverByName(self.get_attributes()['format'])
                if self.get_attributes()['format']=='GML' and gfs_template is not None:
                    copyfile(gfs_template,'%s.gfs' % self._data_file.replace(('.'+self._data_file.split('.')[-1]),''))
                datasource=driver.Open(self._data_file, 0)
                if layer_name==None:
                    if 'layer' in list(self._attributes.keys()):
                        layer=datasource.GetLayer([i.GetName() for i in datasource].index(self._attributes['layer']))
                    else:
                        layer=datasource.GetLayer()
                else:
                    layer=datasource.GetLayer([i.GetName() for i in datasource].index(layer_name))
                if self.get_attributes()['format']=='GML' and gfs_template is not None:
                    os.remove('%s.gfs' % self._data_file.replace(('.'+self._data_file.split('.')[-1]),''))
                if attribute_filter==None:
                    if 'attribute_filter' in list(self._attributes.keys()):
                        layer.SetAttributeFilter(self._attributes['attribute_filter'])
                else:
                    layer.SetAttributeFilter(attribute_filter)
                if spatial_filter==None:
                    if 'spatial_filter' in list(self._attributes.keys()):
                        layer.SetSpatialFilter(self._attributes['spatial_filter'])
                else:
                    layer.SetSpatialFilter(spatial_filter)
                parts=layer.GetFeatureCount()
                i=1
                count=0
                j=math.ceil(parts/number)
                while i<j:
                    result=[]
                    for k in range(number):
                        feature=layer.GetNextFeature()
                        feature_json=json.loads(feature.ExportToJson())
                        if feature_type=='feature':
                           feature=Feature(data=feature_json['properties'], geom=('POLYGON EMPTY' if feature.GetGeometryRef() is None else feature.GetGeometryRef().ExportToWkt()), geom_type='wkt')
                        elif feature_type=='featurewithid':
                           feature=FeatureWithID(data=feature_json['properties'], geom=('POLYGON EMPTY' if feature.GetGeometryRef() is None else feature.GetGeometryRef().ExportToWkt()), id=(feature_additional_attributes['id_value'] if 'id_value' in list(feature_additional_attributes.keys()) else feature_json['properties'][feature_additional_attributes['id_attribute']]), geom_type='wkt')
                        elif  feature_type=='admunitfeature':
                            feature=AdmUnitFeature(data=feature_json['properties'], geom=('POLYGON EMPTY' if feature.GetGeometryRef() is None else feature.GetGeometryRef().ExportToWkt()), id=(feature_additional_attributes['id_value'] if 'id_value' in list(feature_additional_attributes.keys()) else feature_json['properties'][feature_additional_attributes['id_attribute']]), level=(feature_additional_attributes['level_value'] if 'level_value' in list(feature_additional_attributes.keys()) else feature_json['properties'][feature_additional_attributes['level_attribute']]), parent_id=(feature_additional_attributes['parent_value'] if 'parent_value' in list(feature_additional_attributes.keys()) else feature_json['properties'][feature_additional_attributes['parent_attribute']]), geom_type='wkt')
                        result.append(feature)
                        count+=1
                    i+=1
                    yield result
                result=[]
                for k in range(parts-count):
                    feature=layer.GetNextFeature()
                    feature_json=json.loads(feature.ExportToJson())
                    if feature_type=='feature':
                        feature=Feature(data=feature_json['properties'], geom=('POLYGON EMPTY' if feature.GetGeometryRef() is None else feature.GetGeometryRef().ExportToWkt()), geom_type='wkt')
                    elif feature_type=='featurewithid':
                           feature=FeatureWithID(data=feature_json['properties'], geom=('POLYGON EMPTY' if feature.GetGeometryRef() is None else feature.GetGeometryRef().ExportToWkt()), id=(feature_additional_attributes['id_value'] if 'id_value' in list(feature_additional_attributes.keys()) else feature_json['properties'][feature_additional_attributes['id_attribute']]), geom_type='wkt')
                    elif  feature_type=='admunitfeature':
                        feature=AdmUnitFeature(data=feature_json['properties'], geom=('POLYGON EMPTY' if feature.GetGeometryRef() is None else feature.GetGeometryRef().ExportToWkt()), id=(feature_additional_attributes['id_value'] if 'id_value' in list(feature_additional_attributes.keys()) else feature_json['properties'][feature_additional_attributes['id_attribute']]), level=(feature_additional_attributes['level_value'] if 'level_value' in list(feature_additional_attributes.keys()) else feature_json['properties'][feature_additional_attributes['level_attribute']]), parent_id=(feature_additional_attributes['parent_value'] if 'parent_value' in list(feature_additional_attributes.keys()) else feature_json['properties'][feature_additional_attributes['parent_attribute']]), geom_type='wkt')
                    result.append(feature)
                yield result
                del(feature,  layer, datasource, driver)
            elif reader!=None:
                features=reader(self._data_file, feature_type)
                for i in chunker(features,number):
                    yield(i)
        else:
            print('Please, set single datafile from the list at first!')
            
    def get_olu_id(self):
        return self._olu_id
    
    def set_olu_id(self, olu_id):
        self._olu_id=olu_id
        
    def get_metatable(self):
        return self._metatable
        
    def set_metatable(self, metatable):
        self._metatable=metatable
    
    def convert_to_sql_insert(self):
        sql_insert= "insert into "+self._metatable.get_scheme()+"."+self._metatable.get_name()+" (fid,  metadata_fid, uri, dataset_type) VALUES ("+str(self._olu_id)+","+str(self.get_metadata().get_olu_id())+","+("'{}'".format(self._name))+","+str(self._type)+")"
        return sql_insert

class DBStorage():
    '''trida ktera definuje databazi, pripojeni, pripadne nejake specificke dotazy, napriklad, nacist vsechny uzemne administrativni celky z databaze. '''
    def __init__(self, connection_parameters,  kind='postgresql'):
        self._connection_parameters=connection_parameters
        self._kind=kind
        self._connection=None
    def connect(self):
        if self._connection:
            pass
        else:
            self._connection = psycopg2.connect("dbname='%s' user='%s' host='%s' port=%s password='%s'" % (self._connection_parameters['dbname'],self._connection_parameters['user'] , self._connection_parameters['host'], self._connection_parameters['port'], self._connection_parameters['password']) )
    def disconnect(self):
        self._connection.close()
        self._connection=None
    def create_ogr_connection(self):
        conn=ogr.Open(connection_parameters_to_pg(self._connection_parameters))
        return conn
    def execute(self, query):
        cursor=self._connection.cursor()
        cursor.execute(query)
        self._connection.commit()
        try:
                results=cursor.fetchall()
                cursor.close()
        except:
                cursor.close()
                results='query executed'
        return results
    def execute_many(self, query, number=1):
        cursor=self._connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(query)
        self._connection.commit()
        try:
            while True:
                results=cursor.fetchmany(number)
                yield results
        except:
            results='query executed'
            return results
        cursor.close()

    def create_table(self, table, datastructure):
        self.execute( 'create table %s (%s) ;' % (table, ','.join(['%s %s' % (i['name'],i['type']) for i in datastructure ]) ))

    def insert_many(self, insert,  data,  commit_number):
        cursor=self._connection.cursor()
        count=0
        for i in data:
            if len(i)>0:
                args_str = ','.join(mass_insert(i))
                cursor.execute(insert+" VALUES " + args_str) 
                count+=1
                if (count%commit_number==0):
                    self._connection.commit()
                self._connection.commit()
            else:
                break
        cursor.close()
        return 'inserted!' 
    
    def get_last_value_of_seq(self, sequence_name,set_last_value=False,data_table=None,id_attribute=None):
        if set_last_value is True:
            max_value=self.execute('select max(%s) from %s' % (id_attribute, data_table) )[0][0]
            max_value=max_value if max_value is not None else 0
            self.execute("select setval('%s', %s)" % (sequence_name,max_value) )
        return self.execute('select last_value from %s' % sequence_name )[0][0]

    
class MetaData():
    '''trida pro tvorbu metadat na zaklade propojeni informaci z datoveho zdroje a prislusneho konceptu. '''
    def __init__(self, name, data, kind, metatable=None,  example_feature=None,  olu_id=None):
        self._name=name
        self._data=data
        self._kind=kind
        self._metatable=metatable
        self._example_feature=example_feature
        self._olu_id=olu_id
    def get_name(self):
        return self._name
    def get_data(self):
        return self._data
    def get_kind(self):
        return self._kind
    def set_kind(self, kind):
        self._kind=kind
    def get_metatable(self):
        return self._metatable
    def set_metatable(self, metatable):
        self._metatable=metatable
    def get_example_feature(self):
        return self._example_feature
    def set_example_feature(self, example_feature):
        self._example_feature=example_feature
    def get_olu_id(self):
        return self._olu_id
    def set_olu_id(self, olu_id):
        self._olu_id=olu_id
    def set_validitydates(self, dates):
        self._valid_from, self._valid_to=dates
    def get_validitydates(self):
        return(self._valid_from, self._valid_to)
    def convert_to_sql_insert(self):
        if self._example_feature is not None:
            sql_insert= "insert into "+self._metatable.get_scheme()+"."+self._metatable.get_name()+" (fid,metadata,origin_name,valid_from,valid_to,origin_type,column_names) VALUES ("+str(self._olu_id)+",'obsah metadat',"+("'{}'".format(self._name))+","+("'{}'".format(self._valid_from) if self._valid_from is not None else 'NULL')+","+("'{}'".format(self._valid_to)  if self._valid_to is not None else 'NULL')+","+("'{}'".format(self._kind))+",'"+(json.dumps({"column_names": list(self._example_feature.get_data().keys() )})) +"'::json)"
        else:
            sql_insert= "insert into "+self._metatable.get_scheme()+"."+self._metatable.get_name()+" (fid,metadata,origin_name,valid_from,valid_to,origin_type) VALUES ("+str(self._olu_id)+",'obsah metadat',"+("'{}'".format(self._name))+","+("'{}'".format(self._valid_from) if self._valid_from is not None else 'NULL')+","+("'{}'".format(self._valid_to)  if self._valid_to is not None else 'NULL')+","+("'{}'".format(self._kind))+")"
        return sql_insert
     
def ds_from_metadata(metadata_object, format=None):
    #metadata:name,data,kind
    if format==None:
        return DataSource(metadata_object.get_data()['format'], metadata_object.get_name(), metadata_object.get_data(), metadata_object)
    else:
        return DataSource(format, metadata_object.get_name(), [i for i in metadata_object.get_data() if i['format']==format][0], metadata_object)
        
class Table():
    '''trida na ukadani dat do tabulek. '''
    def __init__(self, name, data_structure, dbs=None, scheme='public', adm_graph_node=None, table_inheritance=None):
        self._name=name
        self._data_structure=data_structure
        self._dbs=dbs
        self._scheme=scheme
        self._adm_graph_node=adm_graph_node
        self._table_inheritance=table_inheritance
    def get_name(self):
        return self._name
    def get_scheme(self):
        return self._scheme
    def get_data_structure(self):
        return self._data_structure
    def get_dbs(self):
        return self._dbs
    def get_adm_graph_node(self):
       return self._adm_graph_node
    def set_adm_graph_node(self, adm_graph_node):
       self._adm_graph_node=adm_graph_node
    def create_script(self):
        if self._table_inheritance==None:
            return  'create table %s.%s (%s) '  %  (transform_name_to_postgresql_format(self._scheme), transform_name_to_postgresql_format(self._name), ','.join([str(i[0])+' '+str(i[1]) for i in [[v for k,v  in i.items()] for i in self._data_structure]]))
        else:
            return 'create table %s.%s () inherits (%s) '  %  (transform_name_to_postgresql_format(self._scheme), transform_name_to_postgresql_format(self._name), self._table_inheritance)
    def define_inheritance(self,  inheritance_dictionary):
        self._dbs.execute('create_inheritance')
    def read_feature(self):
        '''read first single feature '''
        feature=self._dbs.execute('select * from %s limit 1' % ( transform_name_to_postgresql_format(self._scheme)+'.'+ transform_name_to_postgresql_format(self._name) ))
        return feature
    def read_features(self, number=1000,  attributes='*'):
        '''features generator '''
        features=self._dbs.execute_many('select %s from %s' % (attributes,  transform_name_to_postgresql_format(self._scheme)+'.'+ transform_name_to_postgresql_format(self._name)) ,  number=number)
        return features
    def read_features_by_condition(self, condition, number=1000,  attributes='*'):
        '''features generator '''
        features=self._dbs.execute_many('select %s from %s where %s' % (attributes,  transform_name_to_postgresql_format(self._scheme)+'.'+ transform_name_to_postgresql_format(self._name), condition) ,  number=number)
        return features
    def create_index(self, column,  type,  unique=False):
        if type=='btree':
            if unique==True:
                self._dbs.execute('create unique index uidx_%s_%s ON %s (%s);' % ( self._name,   transform_column_name_to_postgresql_format(column),  ( transform_name_to_postgresql_format(self._scheme)+'.'+ transform_name_to_postgresql_format(self._name)), column ) )
            else:
                self._dbs.execute('create index idx_%s_%s on %s using btree(%s)' % ( self._name,  transform_column_name_to_postgresql_format(column),  ( transform_name_to_postgresql_format(self._scheme)+'.'+ transform_name_to_postgresql_format(self._name)), column ) )
        elif type=='gist':
            self._dbs.execute('create index sidx_%s_%s on %s using gist(%s)' % ( self._name,  transform_column_name_to_postgresql_format(column),  ( transform_name_to_postgresql_format(self._scheme)+'.'+ transform_name_to_postgresql_format(self._name)), column ) )
        else:
            print('this type of index is not implemented')
    def get_self_sequence_name(self, column_name):
        return (self._dbs.execute("select (regexp_matches(column_default, '^.*(\''.*\'').*$', 'g'))[1] from INFORMATION_SCHEMA.COLUMNS where table_name = '%s' and column_name='%s'" % (self._name, column_name))[0][0]).replace("'", '')
            
class View(Table):
    '''trida na cteni dat z pohledu. '''
    def __init__(self, name, data_structure, table, condition, dbs=None,  scheme='public',adm_graph_node=None, type='usual'):
        super().__init__(name, data_structure, dbs,  scheme,  adm_graph_node)
        self._condition=condition
        self._table=table
        self._type=type
    def get_condition(self):
        return self._condition
    def get_table(self):
        return self._table
    def get_type(self):
        return self._type
    def create_script(self):
        if self._type=='materialized':
            return  'create or replace materialized view %s.%s as select * from %s.%s where %s '  %  (self._scheme, self._name, self._table.get_scheme(), self._table.get_name(),self._condition)
        else:
            return  'create or replace view %s.%s as select * from %s.%s where %s '  %  (self._scheme, self._name, self._table.get_scheme(), self._table.get_name(),self._condition)
    def define_inheritance(self,  inheritance_dictionary):
       raise AttributeError( "'View' object has no 'inheritance'" )
    def read_feature(self):
        '''read first single feature '''
        feature=self._dbs.execute('select * from %s limit 1' % ( transform_name_to_postgresql_format(self._scheme)+'.'+ transform_name_to_postgresql_format(self._name)) )
        return feature
    #def read_features(self, number=1000):
    #   '''features generator '''
    #  features=self._dbs.execute_many('select * from %s' % ( transform_name_to_postgresql_format(self._scheme)+'.'+ transform_name_to_postgresql_format(self._name)) ,  number=number)
    # return features
    def read_features_by_condition(self, condition, number=1000):
        '''features generator '''
        features=self._dbs.execute_many('select * from %s where %s' % ( transform_name_to_postgresql_format(self._scheme)+'.'+ transform_name_to_postgresql_format(self._name),  condition) ,  number=number)
        return features
    def create_index(self, column,  type,  unique=False):
        if self._type=='materialized':
            if type=='btree':
                if unique==True:
                    self._dbs.execute('create unique index uidx_%s_%s ON %s (%s);' % ( self._name,  column,  ( transform_name_to_postgresql_format(self._scheme)+'.'+ transform_name_to_postgresql_format(self._name)), column ) )
                else:
                    self._dbs.execute('create index idx_%s_%s on %s using btree(%s)' % ( self._name,  column,  ( transform_name_to_postgresql_format(self._scheme)+'.'+ transform_name_to_postgresql_format(self._name)), column ) )
            elif type=='gist':
                self._dbs.execute('create index sidx_%s_%s on %s using gist(%s)' % ( self._name,  column,  ( transform_name_to_postgresql_format(self._scheme)+'.'+ transform_name_to_postgresql_format(self._name)), column ) )
            else:
                print('this type of index is not implemented')
        else:
            raise AttributeError( "Usual Views don't have indices, just materialized" )
    
class Feature():
    '''vektorovy prvek, jenz pochazi z jednoho z datovych zdroju (DataSource) . slouzi pro transformaci do podoby OLU prvku.  '''
    def __init__(self, data, geom,  geom_type='wkt'):
        self._data=data
        self._geom=geom
        self._geom_type=geom_type
    def get_data(self):
        return self._data
    def get_geometry(self):
        return self._geom
    def transform_geometry(self, osr_coordinatetransformation):
        if self._geom_type=='wkt':
            geom=ogr.CreateGeometryFromWkt(self._geom)
            geom.Transform(osr_coordinatetransformation)
            self._geom=geom.ExportToWkt()
            self._geom_type='wkt'
        elif self._geom_type=='geojson':
            geom=ogr.CreateGeometryFromJson(self._geom)
            geom.Transform(osr_coordinatetransformation)
            self._geom=geom.ExportToWkt()
            self._geom_type='wkt'
        elif self._geom_type=='wkb':
            geom=ogr.CreateGeometryFromWkb(self._geom)
            geom.Transform(osr_coordinatetransformation)
            self._geom=geom.ExportToWkt()
            self._geom_type='wkt'
        else:
            raise ValueError('the feature has unsupported geometry type')
    def get_geometry_type(self):
        return self._geom_type
    def convert_to_sql_insert(self):
        if self._geom_type=='wkt':
            return '(st_geomfromtext(\''+self._geom+'\'),\''+json.dumps(self._data).replace("'","''")+'\'::json)'
        elif self._geom_type=='wkb':
            return '((\''+self._geom+'\')::geometry,\''+json.dumps(self._data).replace("'","''")+'\'::json)'
        elif self._geom_type=='geojson':
            return '(st_geomfromgeojson(\''+json.dumps(self._geom)+'\'),\''+json.dumps(self._data).replace("'","''")+'\'::json)'
    def export_to_geojson(self):
        if self._geom_type=='wkt':
            geom=ogr.CreateGeometryFromWkt(self._geom)
            geom_json=json.loads(geom.ExportToJson())
        elif self._geom_type=='geojson':
            geom_json=self._geom
        elif self._geom_type=='wkb':
            geom=ogr.CreateGeometryFromWkb(self._geom)
            geom_json=json.loads(geom.ExportToJson())
        else:
            raise ValueError('the feature has unsupported geometry type')
        return {"type": "Feature", "geometry":geom_json, "properties":self._data}

class AdmUnitFeature(Feature):
    "vektorovy prvek, reprezentuje administrativne uzemni celek"
    def __init__(self, data, geom, id,  level, parent_id, geom_type='geojson'):
        super().__init__(data, geom,  geom_type)
        self._id=id
        self._level=level
        self._parent_id=parent_id
    def get_id(self):
        return self._id
    def get_level(self):
        return self._level
    def get_parentid(self):
        return self._parent_id
    def convert_to_sql_insert(self):
        if self._geom_type=='wkt':
            return '(st_geomfromtext(\''+self._geom+'\'),\''+json.dumps(self._data).replace("'","''")+'\'::json,'+(str(self._id) if type(self._id)==int else '\''+str(self._id)+'\'')+','+(str(self._level) if type(self._level)==int else '\''+str(self._level)+'\'')+','+(str(self._parent_id) if type(self._parent_id)==int else (('\''+str(self._parent_id)+'\'') if self._parent_id is not None else 'NULL') )+ ')'
        elif self._geom_type=='wkb':
            return '((\''+self._geom+'\')::geometry,\''+json.dumps(self._data).replace("'","''")+'\'::json,'+(str(self._id) if type(self._id)==int else '\''+str(self._id)+'\'')+','+(str(self._level) if type(self._level)==int else '\''+str(self._level)+'\'')+','+(str(self._parent_id) if type(self._parent_id)==int else (('\''+str(self._parent_id)+'\'') if self._parent_id is not None else 'NULL') )+ ')'
        elif self._geom_type=='geojson':
            return '(st_geomfromgeojson(\''+json.dumps(self._geom)+'\'),\''+json.dumps(self._data).replace("'","''")+'\'::json,'+(str(self._id) if type(self._id)==int else '\''+str(self._id)+'\'')+','+(str(self._level) if type(self._level)==int else '\''+str(self._level)+'\'')+','+(str(self._parent_id) if type(self._parent_id)==int else (('\''+str(self._parent_id)+'\'') if self._parent_id is not None else 'NULL') )+ ')'
    def export_to_geojson(self):
        if self._geom_type=='wkt':
            geom=ogr.CreateGeometryFromWkt(self._geom)
            geom_json=json.loads(geom.ExportToJson())
        elif self._geom_type=='geojson':
            geom_json=self._geom
        elif self._geom_type=='wkb':
            geom=ogr.CreateGeometryFromWkb(self._geom)
            geom_json=json.loads(geom.ExportToJson())
        else:
            raise ValueError('the feature has unsupported geometry type')
        data={**self._data, **{"parent_id":self._parent_id, "level":self._level}}
        return {"type": "Feature", "geometry":geom_json, "properties":data}
    
class FeatureWithID(Feature):
    "vektorovy prvek, reprezentuje prvek s urcitym IDckem"
    def __init__(self, data, geom, id,  geom_type='wkt'):
        super().__init__(data, geom,  geom_type)
        self._id=id
    def get_id(self):
        return self._id
    def convert_to_sql_insert(self):
        if self._geom_type=='wkt':
            return '(st_geomfromtext(\''+self._geom+'\'),\''+json.dumps(self._data).replace("'","''")+'\'::json,'+str(self._id)+ ')'
        elif self._geom_type=='wkb':
            return '((\''+self._geom+'\')::geometry,\''+json.dumps(self._data).replace("'","''")+'\'::json,'+str(self._id)+ ')'
        elif self._geom_type=='geojson':
            return '(st_geomfromgeojson(\''+json.dumps(self._geom)+'\'),\''+json.dumps(self._data).replace("'","''")+'\'::json,'+str(self._id)+')'
            
            
class FeatureWithRasterMap(FeatureWithID):
    "vektorovy prvek, reprezentuje prvek, jednim jehoz atributem je rastrova mapa"
    def __init__(self, data, geom, id,  raster_map,  geom_type='wkt'):
        super().__init__(data, geom, id,  geom_type)
        self._raster_map=raster_map
    def get_raster_map(self):
        return self._raster_map
    def set_raster_map(self, raster_map):
        self._raster_map=raster_map
    def convert_to_sql_insert(self):
        if self._geom_type=='wkt':
            return '(st_geomfromtext(\''+self._geom+'\'),\''+json.dumps(self._data).replace("'","''")+'\'::json,'+str(self._id)+ ')'
        elif self._geom_type=='wkb':
            return '((\''+self._geom+'\')::geometry,\''+json.dumps(self._data).replace("'","''")+'\'::json,'+str(self._id)+ ')'
        elif self._geom_type=='geojson':
            return '(st_geomfromgeojson(\''+json.dumps(self._geom)+'\'),\''+json.dumps(self._data).replace("'","''")+'\'::json,'+str(self._id)+')'
    
#prvek neni treba, zrusit!
class OLUFeature(FeatureWithID):
    '''vektorovy prvek, dosazeny transformaci z obycejneho vektoroveho prvku. je vytvaren dle modelu OLU.  '''
    def __init__(self, transformation_dictionary,  geom_type='wkt'):
        self._id=apply_function(transformation_dictionary,  'id')
        self._geom=apply_function(transformation_dictionary, 'geom')
        self._data=apply_function(transformation_dictionary, 'properties')
        self._geom_type=geom_type
    
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
    def convert_to_sql_insert(self):
        '''function to write self representation to the relational database'''

class InspireTheme(Theme):
    ''''popis pasportu objektu Inspire'''    

class Grid:
 '''
 Data describes grid class.
 The most important attributes are WGS-84 coordinates of the grid origin.
 Then stepsize which is a tuple of x and y stepsize.
 And griddata which is data matrix which needs to be fit into the grid.
 '''
 def __init__ (self, grid_origin, grid_stepsize, grid_size=None, grid_data=None, grid_cell_origin='ul'):
  '''
  Initialization of the grid object that takes in the described 3 parameters.
  '''
  self._grid_origin = grid_origin
  self._grid_stepsize = grid_stepsize
  self._grid_size=grid_size
  self._grid_data=grid_data
  self._grid_cellorigin=grid_cell_origin
  
 def get_gridorigin(self):
  '''
  Returns x,y coordinates of the grid origin.
  '''
  return self._grid_origin
  
 def get_gridstepsize(self):
  '''
  Returns tuple of x,y grid step size.
  '''
  return self._grid_stepsize
 
 def get_gridsize(self):
  '''
  Returns the number of rows and columns that are supposed to be inside the grid.
  '''
  return self._grid_size
  
 def get_griddata(self):
  '''
  Returns the ndarray with the data that is fit into the grid.
  '''
  return self._grid_data
  
 def get_affinetransformation(self):
  '''
  Returns gdal affine transformation matrix.
  '''
  return (self._grid_origin[0],self._grid_stepsize[0],0,self._grid_origin[1],0,self._grid_stepsize[1])

 def iterate_sm_grids(self, step):
  start=self._grid_origin
  stop=start+np.multiply(self._grid_size, step)
  i=start[0]
  while (i!=stop[0]):
    j=start[1]
    while (j!=stop[1]):
     yield [i,j]
     j+=step[1]
     #if j==stop[1]:
      #yield [i,j]
    i+=step[0]
    #if i==stop[0]:
     #j=start[1]
     #while (j!=stop[1]):
      #yield [i,j]
      #j+=step[1]
      #if j==stop[1]:
       #yield [i,j]
 def iterate_grid(self, step):
  start=self._grid_origin
  stop=self._grid_origin+np.multiply(self._grid_size, self._grid_stepsize)
  i=start[0]
  if self._grid_stepsize[0]>0:
    while (i<=stop[0]):
      j=start[1]
      if self._grid_stepsize[1]>0:
        while (j<=stop[1]):
         yield Grid((i, j), (self._grid_stepsize), (step)), slice(int((j-self._grid_origin[1])/self._grid_stepsize[1]), int((j-self._grid_origin[1])/self._grid_stepsize[1]+step[1]/self._grid_stepsize[1])), slice(int((i-self._grid_origin[0])/self._grid_stepsize[0]), int((i-self._grid_origin[0])/self._grid_stepsize[0]+step[0]/self._grid_stepsize[0]))
         j+=step[1]
        i+=step[0]
      elif self._grid_stepsize[1]<0:
        while (j>=stop[1]):
         yield Grid((i, j), (self._grid_stepsize), (step)), slice(int((j-self._grid_origin[1])/self._grid_stepsize[1]), int((j-self._grid_origin[1])/self._grid_stepsize[1]+step[1]/self._grid_stepsize[1])), slice(int((i-self._grid_origin[0])/self._grid_stepsize[0]), int((i-self._grid_origin[0])/self._grid_stepsize[0]+step[0]/self._grid_stepsize[0]))
         j+=step[1]
        i+=step[0]
      else:
        raise ValueError
  elif self._grid_stepsize[0]<0:
    while (i>=stop[0]):
      j=start[1]
      if self._grid_stepsize[1]>0:
        while (j<=stop[1]):
         yield Grid((i, j), (self._grid_stepsize), (step)), slice(int((j-self._grid_origin[1])/self._grid_stepsize[1]), int((j-self._grid_origin[1])/self._grid_stepsize[1]+step[1]/self._grid_stepsize[1])), slice(int((i-self._grid_origin[0])/self._grid_stepsize[0]), int((i-self._grid_origin[0])/self._grid_stepsize[0]+step[0]/self._grid_stepsize[0]))
         j+=step[1]
        i+=step[0]
      elif self._grid_stepsize[1]<0:
        while (j>=stop[1]):
         yield Grid((i, j), (self._grid_stepsize), (step)), slice(int((j-self._grid_origin[1])/self._grid_stepsize[1]), int((j-self._grid_origin[1])/self._grid_stepsize[1]+step[1]/self._grid_stepsize[1])), slice(int((i-self._grid_origin[0])/self._grid_stepsize[0]), int((i-self._grid_origin[0])/self._grid_stepsize[0]+step[0]/self._grid_stepsize[0]))
         j+=step[1]
        i+=step[0]
      else:
        raise ValueError
  else:
    raise ValueError

 
 def find_index(self,coordinate):
  '''
  Finds index of the point coordinate if it is within the grid.
  Otherwise returns error.
  '''
  if self._grid_cellorigin=='ul':
      xOffset = math.floor(round(coordinate[0]  - self._grid_origin[0], 2)/self._grid_stepsize[0])
      yOffset = math.ceil(round(coordinate[1] - self._grid_origin[1], 2)/self._grid_stepsize[1])
  elif self._grid_cellorigin=='cc':
      xOffset = np.int(round(round(coordinate[0]  - self._grid_origin[0], 2)/self._grid_stepsize[0]))
      yOffset = np.int(round(round(coordinate[1] - self._grid_origin[1], 2)/self._grid_stepsize[1]))
  else:
      xOffset = np.int(round(round(coordinate[0]  - self._grid_origin[0], 2)/self._grid_stepsize[0]))
      yOffset = np.int(round(round(coordinate[1] - self._grid_origin[1], 2)/self._grid_stepsize[1]))
  return(xOffset,yOffset)
  
 '''def split_grid(self, size, origin=self._grid_origin, folder_scheme=None):
  return size'''
  
"""
g=Grid((180,0),(6,6))
g.find_index((-175,0))
np.multiply(g.find_index((0,0)),g.get_gridstepsize())
"""

class Imagee():
    '''Class to read and work with raster data - DEM, Sentinel Imagery
    '''
    def __init__ (self, dataarray=None, metadata=None, grid=None):
        '''
        Initialize the Imagee object.
        It is needed to provide numpy array (values in 2D space) as well as metadata,
        where 'affine_transformation' and 'nodata' keys are important.
        '''
        if metadata!=None and list(dataarray)!=None:
            self._metadata = metadata
            if type(dataarray)==ma.core.MaskedArray:
                dataarray = dataarray.filled(self._metadata['nodata'])
                self._data =ma.array(dataarray,mask=[pd.isnull(dataarray)])  if pd.isnull(self._metadata['nodata']) else ma.array(dataarray,mask=[dataarray==self._metadata['nodata']])
            elif 'nodata' in self._metadata:
                    self._data =ma.array(dataarray,mask=[pd.isnull(dataarray)])  if pd.isnull(self._metadata['nodata']) else ma.array(dataarray,mask=[dataarray==self._metadata['nodata']])
            else:
                self._data = ma.array(dataarray,mask=[dataarray==-32767])
        else:
            self._metadata=metadata
            self._data=dataarray
        self._grid=grid

    def get_metadata(self):
        return self._metadata
        
    def set_metadata(self, metadata):
        self._metadata=metadata

    def get_data(self):
        return self._data  
    
    def set_data(self, data):
        self._data=data
        
    def get_grid(self):
        if self._grid is not None:
            grid=self._grid
        else:
            try:
                grid=Grid((self._metadata['affine_transformation'][0],self._metadata['affine_transformation'][3]),(self._metadata['affine_transformation'][1],self._metadata['affine_transformation'][5]),grid_cell_origin='ul')
            except:
                grid=None
        return grid
        
    def set_grid(self, grid):
        self._grid=grid

    def image_to_geo_coordinates(self, rownum, colnum):
        return( (self._metadata['affine_transformation'][0]+colnum*self._metadata['affine_transformation'][1]+0.5*self._metadata['affine_transformation'][1], self._metadata['affine_transformation'][3]+rownum*self._metadata['affine_transformation'][5]+0.5*self._metadata['affine_transformation'][5]) )

    def clip_by_shape(self, geom_wkt):
        '''
        Clip an Imagee by vector feature.
        '''
        rast = self._data
        gt=self._metadata['affine_transformation']
        poly=ogr.CreateGeometryFromWkt(geom_wkt)
        # Convert the layer extent to image pixel coordinates
        minX, maxX, minY, maxY = poly.GetEnvelope()
        #ulX, ulY = world_to_pixel(gt, minX, maxY)
        #lrX, lrY = world_to_pixel(gt, maxX, minY)
        ulX, ulY = math.floor((minX-gt[0])/gt[1]),math.floor((maxY-gt[3])/gt[5])
        lrX,lrY = math.ceil((maxX-gt[0])/gt[1]),math.ceil((minY-gt[3])/gt[5])
        # Calculate the pixel size of the new image
        pxWidth = int(lrX - ulX)
        pxHeight = int(lrY - ulY)
        # If the clipping features extend out-of-bounds and ABOVE the raster...
        if gt[3] < maxY:
            # In such a case... ulY ends up being negative--can't have that!
            iY = ulY
            ulY = 0
        clip = rast[ulY:lrY, ulX:lrX]
        # Create a new geomatrix for the image
        gt2 = list(gt)
        #gt2[0] = minX
        #gt2[3] = maxY
        gt2[0] = ulX*gt[1]+gt[0]
        gt2[3] = ulY*gt[5]+gt[3]
        # Map points to pixels for drawing the boundary on a blank 8-bit,
        #   black and white, mask image.
        raster_poly = Image.new('L', (pxWidth, pxHeight), 1)
        rasterize = ImageDraw.Draw(raster_poly)

        def rec(poly_geom):
            '''
            Recursive drawing of parts of multipolygons over initialized PIL Image object using ImageDraw.Draw method.
            '''
            if poly_geom.GetGeometryCount()==0:
                points=[]
                pixels=[]
                for p in range(poly_geom.GetPointCount()):
                    points.append((poly_geom.GetX(p), poly_geom.GetY(p)))
                for p in points:
                    #pixels.append(world_to_pixel(gt2, p[0], p[1]))
                    pixels.append((int((p[0]-gt2[0])/gt2[1]),int((p[1]-gt2[3])/gt2[5])))
                rasterize.polygon(pixels, 0)
            if poly_geom.GetGeometryCount()>=1:
                for j in range(poly_geom.GetGeometryCount()):
                    rec(poly_geom.GetGeometryRef(j))
        rec(poly)
        mask = image_to_array(raster_poly)
        # Clip the image using the mask
        try:
            #clip = gdalnumeric.choose(mask, (clip, nodata))
            clip=ma.array(clip,mask=mask)
            clip=clip.astype(float).filled(fill_value=self._metadata['nodata'])
        # If the clipping features extend out-of-bounds and BELOW the raster...
        except ValueError:
            # We have to cut the clipping features to the raster!
            rshp = list(mask.shape)
            if mask.shape[-2] != clip.shape[-2]:
                rshp[0] = clip.shape[-2]
            if mask.shape[-1] != clip.shape[-1]:
                rshp[1] = clip.shape[-1]
            mask.resize(*rshp, refcheck=False)
            #clip = gdalnumeric.choose(mask, (clip, nodata))
            clip=ma.array(clip,mask=mask)
            clip=clip.astype(float).filled(fill_value=self._metadata['nodata'])
        #self._data=clip
        #self._metadata['affine_transformation'],self._metadata['ul_x'],self._metadata['ul_y']=gt2,ulX,ulY
        d={}
        d['affine_transformation'],d['ul_x'],d['ul_y'], d['nodata'],d['proj_wkt']=gt2,ulX,ulY, self._metadata['nodata'], self._metadata['proj_wkt']
        #clip = ma.array(clip,mask=[clip==nodata])
        return (clip, d)
        
        
    def clip_by_shape_bb_buffer(self, envelope, buffer=0): 
        '''
        Clip an Imagee by bounding box of wkt geometry. Add buffer in pixels optionally.
        '''
        rast = self._data
        gt=self._metadata['affine_transformation']
        # Convert the layer extent to image pixel coordinates
        minX = custom_floor(envelope[0],gt[1],precision_and_scale(gt[1])[1])
        maxX = custom_ceiling(envelope[1],gt[1],precision_and_scale(gt[1])[1])
        minY = custom_floor(envelope[2],gt[1],precision_and_scale(gt[1])[1])
        maxY = custom_ceiling(envelope[3],gt[1],precision_and_scale(gt[1])[1])
        minX-=(buffer*gt[1])
        maxX+=(buffer*gt[1])
        minY+=(buffer*gt[5])
        maxY-=(buffer*gt[5])
        ulX, ulY = world_to_pixel(gt, minX, maxY)
        lrX, lrY = world_to_pixel(gt, maxX, minY)
        # Calculate the pixel size of the new image
        pxWidth = int(lrX - ulX)
        pxHeight = int(lrY - ulY)
        clip = rast[ulY:lrY, ulX:lrX]
        # Create a new geomatrix for the image
        gt2 = list(gt)
        gt2[0] = minX
        gt2[3] = maxY
        d={}
        d['affine_transformation'],d['ul_x'],d['ul_y'], d['nodata'],d['proj_wkt']=gt2,ulX,ulY, self._metadata['nodata'], self._metadata['proj_wkt']
        return (clip, d)
        
    def get_statistics(self, categoric=False, full=False, percentage=False):
        if full is False and categoric is False:
            dictionary={'mean':self.get_mean_value(),'max':self.get_max_value(),'min':self.get_min_value(), 'mode':self.get_mode_value(),  'std':self.get_std_value() }
        elif categoric is False and full is True:
            dictionary={'mean':self.get_mean_value(),'max':self.get_max_value(),'min':self.get_min_value(), 'mode':self.get_mode_value(),  'std':self.get_std_value(), 'histogram': self.get_histogram_binscounts(percentage=percentage) }
        elif categoric is True and full is False:
            dictionary={'max':self.get_max_value(),'min':self.get_min_value(), 'mode':self.get_mode_value() }
        else:
            dictionary={'max':self.get_max_value(),'min':self.get_min_value(), 'mode':self.get_mode_value(), 'frequencies': self.get_values_frequencies(percentage=percentage) }
        return dictionary

    def get_min_value(self):
        '''
        Get self min value excluding self nodata value.
        '''
        return np.nanmin(self._data[np.where(self._data!=self._metadata['nodata'])])

    def get_max_value(self):
        '''
        Get self max value excluding self nodata value.
        '''
        return np.nanmax(self._data[np.where(self._data!=self._metadata['nodata'])])

    def get_mean_value(self):
        '''
        Get self mean value excluding self nodata values.
        '''
        return np.nanmean(self._data[np.where(self._data!=self._metadata['nodata'])])

    def get_std_value(self):
        '''
        Get self standard deviation excluding self nodata values.
        '''
        return np.nanstd(self._data[np.where(self._data!=self._metadata['nodata'])])
        
    def get_median_value(self):
        '''
        Get self median value excluding self nodata values.
        '''
        return np.nanmedian(self._data[np.where(self._data!=self._metadata['nodata'])])
        
    def get_mode_value(self):
        '''
        Get self mode value excluding self nodata value.
        '''
        return mode(self._data[self._data.mask==False])[0][0]#mode(self._data[np.where(self._data!=self._metadata['nodata'])])[0][0]
    
    def get_histogram_binscounts(self, percentage=False):
        '''
        Get self histogram.
        '''
        if percentage is False:
            binscounts= {str(k):str(v) for k,v in dict(zip(*np.histogram(self._data[np.where(self._data!=self._metadata['nodata'])], range=(self.get_min_value(),  self.get_max_value()) )[::-1])).items() if v>0}
        else:
            data_size=self._data[self._data.mask==False].size
            binscounts= {str(k):'{:.2f}'.format((v/data_size)*100) for k,v in dict(zip(*np.histogram(self._data[np.where(self._data!=self._metadata['nodata'])], range=(self.get_min_value(),  self.get_max_value()))[::-1])).items() if v>0}
        return binscounts
        
    def get_values_frequencies(self, percentage=False):
        '''
        Get self values frequencies.
        '''
        values,counts=np.unique(self._data, return_counts=True)
        if percentage is False:
            values_frequencies={str(k):str(v) for k,v in dict(zip(values.data,counts)).items() if k!=self._metadata['nodata']}
        else:
            data_size=self._data[self._data.mask==False].size
            values_frequencies={str(k):'{:.2f}'.format((v/data_size)*100) for k,v in dict(zip(values.data,counts)).items() if k!=self._metadata['nodata']}
        return values_frequencies

    def calculate_slope(self):
        '''
        Calculate slope from self data of DEM image.
        '''
        x, y = np.gradient(self._data, abs(self._metadata['affine_transformation'][1]), abs(self._metadata['affine_transformation'][5]),  edge_order=1,axis=None) if 'affine_transformation' in self._metadata else np.gradient(self._data)
        #slope = np.pi/2. - np.arctan(np.sqrt(x*x + y*y))
        slope = np.degrees(np.arctan(np.sqrt(x*x + y*y)))
        return (slope,self._metadata)

    def calculate_azimuth(self, exact=True):
        '''
        Calculate azimuth from self data of DEM image.
        '''
        x, y = np.gradient(self._data, abs(self._metadata['affine_transformation'][1]), abs(self._metadata['affine_transformation'][5]),  edge_order=1,axis=None) if 'affine_transformation' in self._metadata else np.gradient(self._data)
        aspect = np.degrees(np.arctan2(-x, y))
        metadata=self._metadata
        if exact is False:
            bins=[-180,-135,-45,45,135,180]
            aspect=np.digitize(aspect,bins)
            aspect[aspect==5]=1
            aspect[aspect==len(bins)]=0
            metadata['nodata']=0
            aspect=ma.MaskedArray(data=aspect, mask=(self._data.mask))
            #1-east,2-north,3-west,4-south
        return (aspect,metadata)
        
    def calculate_laplace(self, size):
        '''
        Calculate morphological laplace from self data of DEM image.
        '''
        data=morphological_laplace(self._data, size=size)
        return (ma.MaskedArray(data=data, mask=(self._data.mask)), self._metadata)

    def export_as_tif(self,filename):
        '''
        Export self data as GeoTiff 1-band image. 
        Output filename should be provided as a parameter.
        '''
        nrows,ncols=self._data.shape
        geotransform = self._metadata['affine_transformation']
        output_raster = gdal.GetDriverByName('GTiff').Create(filename, ncols, nrows, 1, gdal.GDT_Float32)
        output_raster.SetGeoTransform(geotransform)
        srs = osr.SpatialReference()
        if 'proj_wkt' in list(self._metadata.keys()):
            srs.ImportFromWkt(self._metadata['proj_wkt'])
        #srs.ImportFromWkt('GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]')
        output_raster.SetProjection(srs.ExportToWkt())
        output_raster.GetRasterBand(1).WriteArray(self._data)
        if self._metadata['nodata']!=None:
            output_raster.GetRasterBand(1).SetNoDataValue(self._metadata['nodata'])
        output_raster.FlushCache()
        del output_raster

    def export_into_memory(self):
        nrows,ncols=self._data.shape
        geotransform = self._metadata['affine_transformation']
        output_raster = gdal.GetDriverByName('GTiff').Create('/vsimem/image.tif', ncols, nrows, 1, gdal.GDT_Float32)
        output_raster.SetGeoTransform(geotransform)
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(3857)
        output_raster.SetProjection(srs.ExportToWkt())
        output_raster.GetRasterBand(1).WriteArray(self._data)
        output_raster.GetRasterBand(1).SetNoDataValue(self._metadata['nodata'])
        output_raster.FlushCache()
        f = gdal.VSIFOpenL('/vsimem/image.tif', 'rb') 
        gdal.VSIFSeekL(f, 0, 2) # seek to end 
        size = gdal.VSIFTellL(f) 
        gdal.VSIFSeekL(f, 0, 0) # seek to beginning 
        data = gdal.VSIFReadL(1, size, f) 
        gdal.VSIFCloseL(f) 
        # Cleanup 
        gdal.Unlink('/vsimem/image.tif') 
        return data
        

class SuperImagee:
    """Multi-array image. Typically netCDF data format."""
    def __init__ (self, data=None, metadata=None, grid=None):
        """Initialize the image object.
        Typically should have attributes such as data and metadata.
        Data is a netCDF4 dataset object and metadata is a dictionary object."""
        self._data = data
        self._metadata = metadata
    
    def get_dimensions(self):
        return list(self._data.dimensions.keys())[::-1]
    
    def get_variables(self):
        return list(self._data.variables)
    
    def get_data(self):
        return self._data
    
    def set_data(self, data):
        self._data=data
    
    def find_index(self, dictionary):
        """Find slice indices given the dictionary with slice dimension name and its' range.
        For instance, dictionary {'latitude':[40,50]} would return index to make appropriate slice of dataframe"""
        variable=list(dictionary.keys())[0]
        if len(dictionary[variable])==2:
            return np.where(np.logical_and(self._data.variables[variable][:]>=np.sort(dictionary[variable])[0], self._data.variables[variable][:]<=np.sort(dictionary[variable])[1]))[0]
        elif len(dictionary[variable])==1:
            return np.array(np.where(self._data.variables[variable][:]==dictionary[variable][0])).flatten()
        else:
            raise ValueError('The dictionary should contain variable name with its value or range. ')
    
    def slice (self, attribute, dictionary):
        """Create subImage in a way of slicing original Image by dictionary of attributes"""
        dimensions=self._data.variables[attribute].dimensions
        indices=[]
        for dim in dimensions:
            if dim in list(dictionary.keys()):
                indices.append(self.find_index({dim:dictionary[dim]}))
            else:
                indices.append(None)
        return self._data.variables[attribute][[np.array([x]).flatten() if x is not None else slice(None) for x in np.array(indices).flatten()]].data
    
    def get_statistics(self, attribute, dictionary, kind):
        dimensions=self._data.variables[attribute].dimensions
        indices=[]
        for dim in dimensions:
            if dim in list(dictionary.keys()):
                indices.append(self.find_index({dim:dictionary[dim]}))
            else:
                indices.append(slice(None))
        if 'longitude' in list(dictionary.keys()):
            min_longitude=np.min(dictionary['longitude'])
        else:
            min_longitude=np.min(self._data.variables['longitude'])
        if 'latitude' in list(dictionary.keys()):
            max_latitude=np.max(dictionary['latitude'])
        else:
            max_latitude=np.max(self._data.variables['latitude'])
        if kind=='min':
            data=self._data.variables[attribute][indices].min(axis=0)
        elif kind=='max':
            data=self._data.variables[attribute][indices].max(axis=0)
        elif kind=='mean':
            data=self._data.variables[attribute][indices].mean(axis=0)
        elif kind=='sum':
            data=self._data.variables[attribute][indices].sum(axis=0)
        elif kind=='less_then_0_count':
            def less_then_zero(a):
                return (a<273.15).astype(int)
            pre_data=np.apply_along_axis(less_then_zero,0, self._data.variables[attribute][indices])
            data=pre_data.sum(axis=0)
        else:
            print('This kind of statistical measurement is not yet available. ')
        return Imagee(data,{'affine_transformation':(min_longitude,abs(self._data.variables['longitude'][1]-self._data.variables['longitude'][0]),0,max_latitude,0,-abs(self._data.variables['longitude'][1]-self._data.variables['longitude'][0]))})
    
'''
available_database=DBStorage(connection_parameters)
master_database=DBStorage(connection_parameters)

inspire_natural_hazards=InspireTheme(name,abstract,uri,specification)
inspire_natural_hazards.write_to_masterdb()

corine_1990_metadata=MetaData(inspire_xml)
corine_1990_ds=corine_1990_metadata.create__data_source()
corine_1990_ds.write_to_masterdb(master_database)
corine_land_cover=GeoConcept(data_model_dictionary, name, abstract)
corine_land_cover.set_datasource(corine_1990_ds)
corine_land_cover.set_working_directory(path)
corine_land_cover.set_table(available_database.create_master_table(corine_land_cover))
corine_land_cover.get_table().set_inheritance(adm_unit.create_hierarchy_graph())
corine_land_cover.generate_download_script(type, metadata.get_link() etc etc etc, self._working_directory)
corine_land_cover.generate_upload_script(self, available_database)
corine_land_cover.generate_update_script(self, available_database)
corine_land_cover.set_relevant_topics([inspire__land_use, inspire__land_cover])
adm_unit=GeoConcept(data_model_dictionary, name, abstract)
...
parcely....
lpis....
urban_atlas...
flood_zones...
....

inspire__geohazards_theme.set_base_order([parcels, lpis, urban_atlas, corine])
inspire_geohazards_theme.set_attribute_layers([flood_risk,forest_fire_risk], mode='aggregate')

inspire__geohazards_theme.generate_view(available_db, adm_unit.find(id='CZ0209538493') )
inspire__geohazars_theme.generate_db(....)


- GeoConcept - land cover simple model, geometry, attributes, 



available_database=DBStorage(connection_parameters)
master_database=DBStorage(connection_parameters)

inspire_soil=InspireTheme(name,abstract,uri,specification)
inspire_soil.write_to_masterdb()

lpis_metadata=MetaData(inspire_xml)
lpis_ds=lpis_metadata.create__data_source()
lpis_ds.write_to_db(master_database)
lpis=GeoConcept(data_model_dictionary, name, abstract)
lpis.set_datasource(lpis_ds)
lpis.set_working_directory(path)
lpis.set_table(available_database.create_geoconcept_table(lpis))
lpis.get_table().set_inheritance(adm_unit.create_hierarchy_graph())
lpis.generate_download_script(type, self._datasource.get_metadata().get_link(), self._working_directory)
lpis.generate_upload_script(self, available_database)
lpis.generate_update_script(self, available_database)
lpis.set_relevant_topics([inspire__land_use, inspire__land_cover, inspire__soil....])

adm_unit=GeoConcept(data_model_dictionary, name, abstract)
land_parcels=GeoConcept(data_model_dictionary, name, abstract)
soil=GeoConcept(data_model_dictionary, name, abstract)


inspire_soil.set_base_order([lpis,land_parcels])
inspire_soil.set_attribute_layers([soil], mode='simple;intersects')

inspire__soil.generate_view(available_db, adm_unit.find(id='CZ0209538493') )
inspire__soil.generate_db(....)
---------------------------------------------------------------------------------------------------------------------------------
url='https://geometadatensuche.inspire.gv.at/metadatensuche/srv/api/records/5a7739f5-866f-4f8a-91c6-8898a1ce52b2/formatters/xml'
metadata=BytesIO(r.content)
metadata_tree=etree.parse(metadata)
etree.tostring(metadata_tree)

import json
json_url='https://www.data.gv.at/katalog/api/3/action/package_show?id=e21a731f-9e08-4dd3-b9e5-cd460438a5d9'
r_json=requests.get(json_url)
metadata_json=json.loads(r_json.text)

metadata_json['result']['resources'][0]['url']

def download_file(url):
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter below
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)
    return local_filename
    
zipped_gpkg=download_file(metadata_json['result']['resources'][0]['url']
zipfile.ZipFile(zipped_gpkg).extractall()
fn='inspire_schlaege_2019.gpkg'
dataSource=driver.Open(fn)
[i.GetName() for i in dataSource]
--------['inspire_schlaege_2019']
layer=dataSource.GetLayer(0)
f=layer.GetNextFeature()
f_json=json.loads(f.ExportToJson())
f_json
--------{'type': 'Feature', 'geometry': {'type': 'Point', 'coordinates': [467203.74, 450385.12]}, 'properties': {'FS_KENNUNG': 73799472.0, 'SNAR_BEZEICHNUNG': 'LSE BÄUME / BÜSCHE', 'SL_FLAECHE_BRUTTO_HA': 0.0}, 'id': 1}
pro to aby spustit soubor:;.
exec(open("filename.py").read())

data_structure=[{'name':'id','type':'integer'},{'name':'speed','type':'integer'}]
table_name='car'
 'create table %s (%s) ;' % (table_name, ','.join(['%s %s' % (i['name'],i['type']) for i in data_structure ]) )

'''
