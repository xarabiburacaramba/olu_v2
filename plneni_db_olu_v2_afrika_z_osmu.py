import paramiko

from tridy import DBStorage, Table, Imagee,  ds_from_metadata,  MetaData,  GeoConcept
import requests
import os
import re
import subprocess

from osgeo import osr

import numpy as np
import pandas as pd

from credentials import connectionstring_localhost2, connectionstring_localhost3

relations_dict={'instance_of':'P31', 'part_of':'P361','has_part':'P527', 'country':'P17', 'continent':'P30' , 'located_in_the_administrative_territorial_entity':'P131', 'contains_administrative_territorial_entity':'P150'}
types_dict={'country':'Q6256', 'continent':'Q5107', 'first-level_administrative_country_subdivision':'Q10864048', 'geographic_region':'Q82794', 'inner_planet':'Q3504248', 'continent':'Q5107', 'sovereign_state':'Q3624078'}
statistics_dict={'life_expectancy':'P2250', 'official_language':'P37', 'marriageable_age':'P3000', 'language_used':'P2936', 'population':'P1082', 'nominal_GDP':'P2131', 'nominal_GDP_per_capita':'P2132', 'GDP_PPP':'P4010', 'PPP_GDP_per_capita':'P2299', 'total_reserves':'P2134', 'Human_Development_Index':'P1081', 'suicide_rate':'P3864', 'unemployment_rate':'P1198', 'inflation_rate':'P1279', 'shares_border_with':'P47', 'area':'P2046', 'driving_side':'P1622', 'mains_voltage':'P2884', 'number_of_outofschool_children':'P2573', 'total_fertility_rate':'P4841'}
registers_dict={'ISNI':'P213', 'GeoNames_ID':'P1566', 'OSM_relation_ID':'P402'}
class AdmUnit:
    def __init__(self, type,  name,  parent,  geomwkt,  properties,  wikiuri,  osmuri,  level) :
        self._name=name
        self._geomwkt=geomwkt
        self._properties=properties
        self._wikiuri=wikiuri
        self._osmuri=osmuri
        self._level=level
        self._type=type
        self._parent=parent
    def get_name(self):
        return self._name
    def get_geomwkt(self):
        return self._geomwkt
    def get_properties(self):
        return self._properties
    def get_wikiuri(self):
        return self._wikiuri
    def get_osmuri(self):
        return self._osmuri
    def get_level(self):
        return self._level
    def get_type(self):
        return self._type
    def get_parent(self):
        return self._parent
    def set_name(self, name):
        self._name=name
    def set_geomwkt(self, geomwkt):
        self._geomwkt=geomwkt
    def set_properties(self, properties):
        self._properties=properties
    def set_wikiuri(self, wikiuri):
        self._wikiuri=wikiuri
    def set_osmuri(self, osmuri):
        self._osmuri=osmuri
    def set_level(self, level):
        self._level=level
    def set_type(self, type):
        self._type=type
    def set_parent(self, parent):
        self._parent=parent
    def copy_from_record(self, source, dbs,  table_name, attributes_dictionary ):
        copied_attributes=','.join(['%s as %s' % (v,k) for k,v in {k:(v if k!='_geomwkt' else 'st_astext(st_union(%s))' % v) for k,v in attributes_dictionary.items()}.items()])
        if source=='osm':
            if self._osmuri is None:
                return({'eror':'object doesnt have osmuri attribute set. '})
            result_dict=dbs.execute('select %s from %s where osm_id=-%s group by %s' % (copied_attributes,  table_name,  self._osmuri.split('/')[-1], ','.join([i for i in attributes_dictionary.values() if i not in ('geom','way')])), format='json')
            if len(result_dict)==1:
                result_dict=result_dict[0]
                for key,value in result_dict.items():
                    setattr(self, key, value)
        elif source=='wiki':
            result_dict=dbs.execute('select %s from %s where wikiuri=%s' % (copied_attributes,  table_name,  self._wikiuri), format='json')
            if len(result_dict)==1:
                result_dict=result_dict[0]
                for key,value in result_dict.items():
                    setattr(self, key, value)
        else:
            return({'eror':'this type of id is not supported. '})
    def create_table(self, db,  table_name, unique_attribute=None):
        #db.connect()
        db.execute('create table %s (id serial primary key, type text,  name text,  parent text,  geomwkt text,  properties jsonb,  wikiuri text,  osmuri text,  level integer); ' % table_name)
        if unique_attribute:
            db.execute('create unique index idx_%s_%s on %s (%s)' % (unique_attribute,table_name,table_name,unique_attribute) )
        #db.disconnect()
        #del(db)
        return table_name  
    def insert_into_table(self, db, table_name):
        #db.connect()
        db.execute("delete from %s where wikiuri='%s'" % (table_name, self._wikiuri) )
        db.execute("insert into %s (type,name,parent,geomwkt,properties,wikiuri,osmuri,level) VALUES (%s,%s,%s,%s,'%s'::jsonb,%s,%s,%s)" %(table_name,  (("'"+self._type+"'") if self._type is not None else 'NULL'),  (("'"+self._name+"'") if self._name is not None else 'NULL'),  (("'"+self._parent+"'") if self._parent is not None else 'NULL'),  (("'"+self._geomwkt+"'") if self._geomwkt is not None else 'NULL'), (self._properties if self._properties is not None else '{}'),  (("'"+self._wikiuri+"'") if self._wikiuri is not None else 'NULL'),  (("'"+self._osmuri+"'") if self._osmuri is not None else 'NULL'),  (str(self._level) if self._level is not None else 'NULL')))
        #db.disconnect()
        #del(db)
        return self._wikiuri
        
def sequence_generator(starting_number):
    while starting_number<=10000000000000:
        starting_number+=1
        yield(starting_number)
        
        
cci_clc={1.0:'310',2.0:'320',3.0:'321',4.0:'200',5.0:'400',6.0:'322',7.0:'330',8.0:'100',9.0:'335',10.0:'500',200.0:'900'}

wgs84_sr=osr.SpatialReference()
wgs84_sr.ImportFromProj4('+proj=longlat +datum=WGS84 +no_defs')
mercator3857_sr=osr.SpatialReference()
mercator3857_sr.ImportFromProj4('+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs')
wgs84_to_mercator3857=osr.CoordinateTransformation(wgs84_sr,mercator3857_sr)
mercator3857_to_wgs84=osr.CoordinateTransformation(mercator3857_sr,wgs84_sr)

wikidata_url='https://www.wikidata.org/wiki/'
osm_relations_url='https://www.openstreetmap.org/relation/'
osm_ways_url='https://www.openstreetmap.org/way/'
url='https://sophox.org/sparql'

has_part_query='''
SELECT 
  ?part (SAMPLE(?label_en) as ?label_en) (SAMPLE(?osmid) as ?osmid)
WHERE {
  VALUES ?entity { <http://www.wikidata.org/entity/[entity]>} 
  SERVICE <https://query.wikidata.org/sparql> { 
  ?entity wdt:P150 ?part .
  ?part rdfs:label ?label_en . FILTER(LANG(?label_en) = "en").
  OPTIONAL {?part wdt:P402 ?osmid . } }
 }
GROUP BY ?part ORDER BY ?label_en
'''
get_osmid_query='''
SELECT 
  ?entity (SAMPLE(?osmid) as ?osmid)
WHERE {
  VALUES ?entity { <http://www.wikidata.org/entity/[entity]>}
  SERVICE <https://query.wikidata.org/sparql> { 
  OPTIONAL {?entity wdt:P402 ?osmid . } }
 }
GROUP BY ?entity
'''

def get_members_recursively(entity):
    r = requests.get(url, params = {'format': 'json', 'query': has_part_query.replace('[entity]',entity)})
    entities=[i['part']['value'].split('/')[-1] for i in r.json()['results']['bindings'] if len(r.json()['results']['bindings'])>0]
    translation_dict.update(dict([(i['part']['value'].split('/')[-1],i['osmid']['value']) for i in r.json()['results']['bindings'] if 'osmid' in i]))
    if len(entities)>0:
        for e in entities:
            result_dict.append([entity,e])
            get_members_recursively(e)
    else:
        return

dbs=DBStorage({'dbname':'olu_africa'}.update(connectionstring_localhost2) )
dbs.connect()
conn_olu=dbs.create_ogr_connection()

country='Uganda'
country_wikiid='Q1036'

subprocess.run('wget -O /home/dima/scripts/data/%s-latest.osm.bz2 http://download.geofabrik.de/africa/%s-latest.osm.bz2' % (country.lower(), country.lower()), shell=True, check=True)
if os.path.isfile('/home/dima/scripts/data/%s-latest.osm.bz2' % country.lower()):
    subprocess.run('PGPASSWORD=%s osm2pgsql -d olu_africa -U %s -P %s  -H %s --slim -k /home/dima/scripts/data/%s-latest.osm.bz2' % (connectionstring_localhost2['password'], connectionstring_localhost2['user'], connectionstring_localhost2['port'], connectionstring_localhost2['host'], country.lower()), shell=True, check=True)
else:
    print({'error':'file wasnt downloaded'})

result_dict=[]
translation_dict={}

r = requests.get(url, params = {'format': 'json', 'query': get_osmid_query.replace('[entity]',country_wikiid)})
translation_dict.update({country_wikiid:r.json()['results']['bindings'][0]['osmid']['value']})

optimalni_admin_level=dbs.execute("with celkova_plocha as \
(select admin_level, sum(st_area(way)) as plocha from planet_osm_polygon where admin_level='2' and boundary='administrative' group by admin_level), \
plocha_podrizenych_celku_4_urovne as \
(select a.admin_level, sum(st_area(a.way))/b.plocha as podil_plochy from planet_osm_polygon a, celkova_plocha b where a.admin_level='4' and a.boundary='administrative' group by a.admin_level, b.plocha), \
plocha_podrizenych_celku_5_urovne as \
(select a.admin_level, sum(st_area(a.way))/b.plocha as podil_plochy from planet_osm_polygon a, celkova_plocha b where a.admin_level='5' and a.boundary='administrative' group by a.admin_level, b.plocha), \
plocha_podrizenych_celku_6_urovne as \
(select a.admin_level, sum(st_area(a.way))/b.plocha as podil_plochy from planet_osm_polygon a, celkova_plocha b where a.admin_level='6' and a.boundary='administrative' group by a.admin_level, b.plocha), \
plocha_podrizenych_celku_7_urovne as \
(select a.admin_level, sum(st_area(a.way))/b.plocha as podil_plochy from planet_osm_polygon a, celkova_plocha b where a.admin_level='7' and a.boundary='administrative' group by a.admin_level, b.plocha) \
select admin_level, podil_plochy from plocha_podrizenych_celku_4_urovne \
union all \
select admin_level, podil_plochy from plocha_podrizenych_celku_5_urovne \
union all \
select admin_level, podil_plochy from plocha_podrizenych_celku_6_urovne \
union all \
select admin_level, podil_plochy from plocha_podrizenych_celku_7_urovne \
order by podil_plochy desc \
")[0][0]

results=dbs.execute("select osm_id, tags->'wikidata' as wikiid from planet_osm_polygon where admin_level='%s' and boundary='administrative'" % optimalni_admin_level, format='json')

for result in results:
	result_dict.append([translation_dict[country_wikiid],result['osm_id']*-1])
	translation_dict.update({result['wikiid']:result['osm_id']*-1})

node_record=AdmUnit(type='open', name=country ,  parent=None,  geomwkt=None,  properties=None,  wikiuri=wikidata_url+country_wikiid,  osmuri=osm_relations_url+translation_dict[country_wikiid], level=None)
node_record.copy_from_record('osm', dbs,  'planet_osm_polygon', {'_name':'name','_level':'admin_level','_geomwkt':'way'})
dbs.execute('drop table if exists %s' % country.lower())
node_record.create_table(dbs, country.lower())
node_record.insert_into_table(dbs, country.lower())

for result in results:
	node_record=AdmUnit(type='open', name=None ,  parent=None,  geomwkt=None,  properties=None,  wikiuri=wikidata_url+result['wikiid'] if result['wikiid'] is not None else None,  osmuri=(osm_relations_url+str(result['osm_id']*-1) if result['osm_id']<0 else osm_ways_url+str(result['osm_id'])), level=4)
	node_record.copy_from_record('osm', dbs,  'planet_osm_polygon', {'_name':'name','_geomwkt':'way'})
	node_record.insert_into_table(dbs, country.lower())

node_record_rest=AdmUnit(type='open', name='%s_rest' % country ,  parent=None,  geomwkt=None,  properties=None,  wikiuri='rest_of_:'+wikidata_url+country_wikiid,  osmuri='rest_of_:'+osm_relations_url+str(translation_dict[country_wikiid]), level=4)

node_record_rest.set_geomwkt(
dbs.execute('with cela_zeme as (select id, level, st_geomfromtext(geomwkt) as geom from %s where level=2), \
regiony as (select id, level, st_geomfromtext(geomwkt) as geom from %s where level=4 and geomwkt is not null) \
select st_astext(st_difference(a.geom, st_union(b.geom))) as geomwkt from cela_zeme a, regiony b group by a.geom' % (country.lower(),  country.lower()),'json')[0]['geomwkt']
)

node_record_rest.insert_into_table(dbs, country.lower())

ssh_client =paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(hostname=connectionstring_localhost3['host'],username=connectionstring_localhost3['user'],password=connectionstring_localhost3['password'])

bbox_geo=[float(i) for i in re.findall('[-]?[0-9]+.[0-9]+',dbs.execute('select st_extent(st_buffer(st_setsrid(st_geomfromtext(geomwkt),3857),1000)) from %s where level=2' % country.lower())[0][0])]

stdin,stdout,stderr=ssh_client.exec_command("gdalwarp -t_srs '+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs' -te %s -tr 20 20 -ot Byte  -of GTiff -co 'TILED=YES' -co 'TFW=YES' /home/dima/raster_data/landcover_2016/cci.tif /home/dima/raster_data/landcover_2016/cci_%s.tif" % (' '.join([str(i) for i in bbox_geo]), country.lower()) )
stdout.channel.recv_exit_status() 
subprocess.run("sshpass -p '%s' scp %s@%s:/home/dima/raster_data/landcover_2016/cci_%s.tif /home/dima/scripts/cci_%s.tif" % (connectionstring_localhost3['password'], connectionstring_localhost3['user'], connectionstring_localhost3['host'], country.lower(), country.lower()) , shell=True, check=True)
subprocess.run("sshpass -p '%s' scp %s@%s:/home/dima/raster_data/landcover_2016/cci_%s.tfw /home/dima/scripts/cci_%s.tfw" %  (connectionstring_localhost3['password'], connectionstring_localhost3['user'], connectionstring_localhost3['host'], country.lower(), country.lower()) , shell=True, check=True)


adm_units_ids=[i[0] for i in dbs.execute('select osmuri from %s where level=4' % country.lower())]

metadata_table=Table('origin_metadata',
[{'name': 'fid', 'type': 'integer primary key'},
 {'name': 'metadata', 'type': 'text'},
 {'name': 'origin_name', 'type': 'text'},
 {'name': 'valid_from', 'type': 'date'},
 {'name': 'valid_to', 'type': 'date'},
 {'name': 'origin_type', 'type': 'text'},
 {'name': 'column_names', 'type': 'json'}],
dbs, 'olu2')

ds_table=Table('origin_dataset',
[{'name': 'fid', 'type': 'integer primary key'},
 {'name': 'metadata_fid', 'type': 'integer'},
 {'name': 'uri', 'type': 'text'},
 {'name': 'dataset_type', 'type': 'integer'}],
dbs, 'olu2')

json_feature_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"}]
json_feature_with_raster_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"raster_maps","type":"raster"}]

osm_africa_metadata=MetaData('OSM database for Africa', {"url":"http://download.geofabrik.de/africa-latest.osm.pbf", "format":"pbf", "downloadable":"free"}, 'data')
osm_africa__ds=ds_from_metadata(osm_africa_metadata)

osm_africa=GeoConcept('OSM database for Africa','OSM database for Africa.', 'Feature',json_feature_structure, subgeoconcepts=[], data_source=osm_africa__ds)

osm_africa_metadata.set_metatable(metadata_table)

osm_africa_metadata.set_validitydates(['2021-01-01','2121-01-01'])

metadata_fid_sequence=sequence_generator(dbs.get_last_value_of_seq(osm_africa_metadata.get_metatable().get_self_sequence_name('fid'),set_last_value=True,data_table=osm_africa_metadata.get_metatable().get_scheme()+'.'+osm_africa_metadata.get_metatable().get_name(),id_attribute='fid'))

osm_africa_metadata.set_olu_id(next(metadata_fid_sequence))

dbs.execute(osm_africa_metadata.convert_to_sql_insert())

osm_africa__ds.set_metatable(ds_table)

ds_fid_sequence=sequence_generator(dbs.get_last_value_of_seq(osm_africa__ds.get_metatable().get_self_sequence_name('fid'),set_last_value=True,data_table=osm_africa__ds.get_metatable().get_scheme()+'.'+osm_africa__ds.get_metatable().get_name(),id_attribute='fid'))

osm_africa__ds.set_type(1)

osm_africa__ds.set_olu_id(next(ds_fid_sequence))

dbs.execute(osm_africa__ds.convert_to_sql_insert())

osm_africa__ds.set_type('pbf')

cci_africa_metadata=MetaData('CCI 20m landcover raster of Africa', {"url":"http://2016africalandcover20m.esrin.esa.int/download.php", "format":"GTiff", "downloadable":"on registration"}, 'raster')
cci_africa__ds=ds_from_metadata(cci_africa_metadata)

cci_africa=GeoConcept('CCI 20m landcover raster of Africa','CCI 20m landcover raster of Africa.', 'Feature',json_feature_with_raster_structure, subgeoconcepts=[], data_source=cci_africa__ds)

cci_africa_metadata.set_metatable(metadata_table)

cci_africa_metadata.set_validitydates(['2016-01-01','2116-01-01'])

cci_africa_metadata.set_olu_id(next(metadata_fid_sequence))

dbs.execute(cci_africa_metadata.convert_to_sql_insert())

cci_africa__ds.set_metatable(ds_table)

cci_africa__ds.set_type(2)

cci_africa__ds.set_olu_id(next(ds_fid_sequence))

dbs.execute(cci_africa__ds.convert_to_sql_insert())

cci_africa__ds.set_type('GTiff')

olu_object_table='olu2.olu_object'
olu_attribute_set_table='olu2.olu_attribute_set'
olu_atts_to_object_table='olu2.atts_to_object'
olu_admunit_table='olu2.administrative_unit'
olu_object_to_admunit_table='olu2.olu_object_to_admin_unit'

olu_id_gen=sequence_generator(dbs.get_last_value_of_seq('olu2.olu_object_fid_seq',set_last_value=True,data_table='olu2.olu_object',id_attribute='fid'))
olu_atts_id_gen=sequence_generator(dbs.get_last_value_of_seq('olu2.olu_attributes_fid_seq',set_last_value=True,data_table='olu2.olu_attribute_set',id_attribute='fid'))

'''CREATE TABLE olu2.administrative_unit
(
  fid text NOT NULL,
  parent_fid text,
  level_code integer NOT NULL,
  unit_name text NOT NULL,
  country_iso text NOT NULL,
  geom geometry(MultiPolygon,4326) NOT NULL,
  national_geom geometry,
  CONSTRAINT administrative_unit_pk PRIMARY KEY (fid)
)'''

'''CREATE TABLE olu2.olu_object
(
  fid bigserial NOT NULL,
  dataset_fid integer NOT NULL,
  z_value integer,
  geom geometry(MultiPolygon,4326) NOT NULL,
  valid_from date,
  valid_to date,
  CONSTRAINT olu_object_pkey PRIMARY KEY (fid),
  CONSTRAINT olu_object_origin_dataset_fk FOREIGN KEY (dataset_fid)
      REFERENCES olu2.origin_dataset (fid) MATCH SIMPLE
      ON UPDATE CASCADE ON DELETE CASCADE
)'''

'''CREATE TABLE olu2.olu_attribute_set
(
  fid bigint NOT NULL DEFAULT nextval('olu2.olu_attributes_fid_seq'::regclass),
  hilucs_id integer,
  atts jsonb,
  dataset_fid integer NOT NULL,
  clc_id integer,
  CONSTRAINT olu_attributes_pkey PRIMARY KEY (fid),
  CONSTRAINT olu_atts_clc_id_fk FOREIGN KEY (clc_id)
      REFERENCES olu2.clc_value (clc_code) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT olu_atts_hilucs_pk FOREIGN KEY (hilucs_id)
      REFERENCES olu2.hilucs_value (value_id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT olu_atts_origin_dataset_fk FOREIGN KEY (dataset_fid)
      REFERENCES olu2.origin_dataset (fid) MATCH SIMPLE
      ON UPDATE CASCADE ON DELETE CASCADE
)'''

'''
def corine_transformations(object,object_fid,attributes_fid,admunit_id,object_table_name,attributes_table_name,attributes2object_table_name,object2nuts_table_name): 
    o_dict=\
    {'feature':{'object':object},\
     'feature_json':{'function':(lambda y: json.loads(y.ExportToJson()) ),'parameters':['feature']},\
     'object_id':{'object':object_fid},\
     'attributes_id':{'object':attributes_fid},\
     'admunit_id':{'object':admunit_id},\
     'object_table_name':{'object':object_table_name},\
     'attributes_table_name':{'object':attributes_table_name},\
     'attributes2object_table_name':{'object':attributes2object_table_name},\
     'object2nuts_table_name':{'object':object2nuts_table_name},\
     'object_geom':{'function':(lambda y: y.GetGeometryRef().ExportToWkt() ),'parameters':['feature']},\
     'object_properties':{'function':(lambda x,y: {**{'fid':x},**{'dataset_id':4},**{'z_value':100},**{'admunit_id':y},**{'valid_from':'2018-01-01','valid_to':'2024-01-01'} }),'parameters':['object_id','admunit_id']},
     'attributes_properties':{'function':(lambda x,y: {**{'fid':x},**{'hilucs_value':corine_hilucs[y['properties']['clc_code']]},**{'clc_value':y['properties']['clc_code']},**{'atts':y['properties']},**{'dataset_id':4}}),'parameters':['attributes_id','feature_json']},
     'object_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,dataset_fid,z_value,geom,valid_from,valid_to) VALUES (%s,%s,%s,ST_SetSRID(ST_Multi(ST_GeomFromText('%s')),4326),'%s','%s')" % (w,x,z['dataset_id'],z['z_value'],y,z['valid_from'], z['valid_to'] ) ),'parameters':['object_table_name','object_id','object_geom','object_properties']},\
     'attributes_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,hilucs_id,atts,dataset_fid,clc_id) VALUES (%s,%s,('%s'::json),%s,%s)" % (w, x,z['hilucs_value'],json.dumps(y['properties']),z['dataset_id'],z['clc_value']) ),'parameters':['attributes_table_name','attributes_id','feature_json','attributes_properties']},\
     'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,1)" % (z,x,y) ),'parameters':['object_id','attributes_id','attributes2object_table_name']},\
     'object2admunit_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,unit_fid) VALUES (%s,'%s')" % (z,x,y) ),'parameters':['object_id','admunit_id','object2nuts_table_name']},\
     'four_insert_statements':{'function':(lambda w,x,y,z: [w,x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement','object2admunit_insert_statement']}
    }
    return o_dict
'''

cci_africa.set_raster_output_backend('/home/dima/scripts/','cci_%s.tif' % country.lower())

dbs.execute('create schema %s' % country.lower())

parent_values=dbs.execute("select * from uganda where level=2", format='json')[0]

dbs.execute("insert into olu2.administrative_unit (fid,level_code,unit_name,country_iso,geom) select '%s' as fid, %s as level_code, '%s' as unit_name, 'UGA' as country_iso, st_multi(st_transform(st_setsrid(st_geomfromtext('%s'),3857),4326)) as geom" % (parent_values['osmuri'], parent_values['level'], parent_values['name'] if parent_values['name'] is not None else '', parent_values['geomwkt'] ))

#adm_unit_id=adm_units_ids[0]


for adm_unit_id in adm_units_ids[1:]:
    values=dbs.execute("select * from uganda where osmuri='%s'" % adm_unit_id, format='json')[0]
    dbs.execute("insert into olu2.administrative_unit (fid,parent_fid,level_code,unit_name,country_iso,geom) select '%s' as fid, '%s' as parent_fid, %s as level_code, '%s' as unit_name, 'UGA' as country_iso, st_multi(st_transform(st_setsrid(st_geomfromtext('%s'),3857),4326)) as geom" % (values['osmuri'],parent_values['osmuri'], values['level'], values['name'] if values['name'] is not None else '', values['geomwkt'] ))
    dbs.execute("\
    create table temp_full_lines_polygons as select a.osm_id as id,a.way,'line' as type from planet_osm_line a inner join %s b on st_intersects(a.way,st_setsrid(st_geomfromtext(b.geomwkt),3857)) where \
    ((bicycle is not null) or ( bridge is not null) or ( boundary is not null) or ( covered is not null) or ( embankment is not null) or ( foot is not null) or ( highway is not null) or ( junction is not null) or ( landuse is not null) or ( man_made is not null) or ( military is not null) or ( motorcar is not null) or ( \"natural\" is not null) or ( oneway is not null) or ( railway is not null) or ( ref is not null) or ( route is not null) or ( service is not null) or ( sport is not null) or ( surface is not null) or ( tourism is not null) or ( tracktype is not null) or ( water is not null) or ( waterway is not null) or ( wetland is not null) or ( wood is not null)) \
    and b.osmuri='%s' \
    union all \
    select osm_id as id, st_boundary(way) as way, 'polygon' as type from planet_osm_polygon a inner join %s b on st_intersects(a.way,st_setsrid(st_geomfromtext(b.geomwkt),3857)) where \
    ((aeroway is not null ) or (harbour is not null ) or (highway is not null ) or (landuse is not null ) or (\"natural\" is not null ) or (railway is not null ) or (surface is not null ) or (water is not null ) or (waterway is not null ) or (wetland is not null ) or ( wood is not null)) \
    and b.osmuri='%s' \
    union all \
    select %s as id, st_boundary(st_setsrid(st_geomfromtext(geomwkt),3857)) as way, 'administrative' as type from %s where osmuri='%s'" % (country.lower(), adm_unit_id, country.lower(), adm_unit_id, adm_unit_id.split('/')[-1][0:], country.lower(), adm_unit_id) )
    dbs.execute("\
    create table temp_polygonzed_full_lines as \
    WITH noded AS (SELECT ST_Union(way) geom FROM temp_full_lines_polygons) SELECT ST_Polygonize( geom) as geom FROM noded; \
    ")
    dbs.execute("\
    create table temp_full_polygons as \
    select (st_dump(st_collectionextract(geom,3))).geom from temp_polygonzed_full_lines; \
    ")
    #dbs.execute("create table kenya_lc.unit_%s () inherits (kenya_lc) " % adm_unit_id.split('/')[-1])
    dbs.execute("create table %s.unit_%s () inherits (%s) " % ( country.lower(),  adm_unit_id.split('/')[-1],  olu_object_table ))
    dbs.execute("ALTER TABLE %s.unit_%s ALTER COLUMN fid SET DEFAULT nextval('olu2.olu_object_fid_seq'::regclass)" % ( country.lower(),  adm_unit_id.split('/')[-1]))
    #dbs.execute("insert into kenya_lc.unit_%s (geom,admunit) select geom, '%s' as admunit from  temp_full_polygons" % (adm_unit_id.split('/')[-1], adm_unit_id) )
    dbs.execute("insert into %s.unit_%s (dataset_fid,z_value,geom,valid_from,valid_to) select %d, %d, st_multi(st_transform(geom,4326)), '%s', '%s' from  temp_full_polygons" % (country.lower(),  adm_unit_id.split('/')[-1], osm_africa__ds.get_olu_id(),  100000,  osm_africa__ds.get_metadata().get_validitydates()[0],  osm_africa__ds.get_metadata().get_validitydates()[1] ) )
    dbs.execute('drop table temp_full_lines_polygons')
    dbs.execute('drop table temp_polygonzed_full_lines')
    dbs.execute('drop table temp_full_polygons')
    #dbs.execute("UPDATE kenya_lc.unit_%s SET id=nextval('kenya_lc_id_seq');"% adm_unit_id.split('/')[-1])
    dbs.execute("create unique index idx__%s__unit_%s__fid on %s.unit_%s using btree(fid);" % (country.lower(), adm_unit_id.split('/')[-1],country.lower(), adm_unit_id.split('/')[-1]))
    dbs.execute("create index sidx__%s__unit_%s__geom on %s.unit_%s using gist(geom);"  % (country.lower(), adm_unit_id.split('/')[-1],country.lower(), adm_unit_id.split('/')[-1]))
    dbs.execute("\
    delete from %s.unit_%s where fid in \
    (select a.fid from %s.unit_%s a, %s b where st_area(st_transform(st_intersection(a.geom, st_transform(st_setsrid(st_geomfromtext(b.geomwkt),3857),4326)),3857))<2 and b.osmuri='%s');\
    " % (country.lower(), adm_unit_id.split('/')[-1], country.lower(), adm_unit_id.split('/')[-1], country.lower(), adm_unit_id) )
    dbs.execute("insert into olu2.olu_object_to_admin_unit (object_fid,unit_fid) select fid as object_fid, '%s' as unit_fid from %s.unit_%s" % (parent_values['osmuri'],  country.lower(), adm_unit_id.split('/')[-1] ))
    dbs.execute("insert into olu2.olu_object_to_admin_unit (object_fid,unit_fid) select fid as object_fid, '%s' as unit_fid from %s.unit_%s" % (values['osmuri'],  country.lower(), adm_unit_id.split('/')[-1] ))


tables=[str(i[0]) for i in dbs.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'" % country.lower())]

for table in tables[12:]:
    lyr=conn_olu.GetLayer('%s.%s' % (country.lower(), table) )
    bbox_r=[lyr.GetExtent()[0],lyr.GetExtent()[3],lyr.GetExtent()[1],lyr.GetExtent()[2]]
    bbox_r3857=list(np.array(wgs84_to_mercator3857.TransformPoint(*bbox_r[0:2])[0:2]+wgs84_to_mercator3857.TransformPoint(*bbox_r[2:4])[0:2])+np.array([-500,500,500,-500]))
    cci__im=cci_africa.read_raster_output_backend(1,bbox_r3857)
    colnames=['id','lc_class','frequency']
    df=pd.DataFrame(columns=colnames)
    lyr.ResetReading()
    bad_list=[]
    for c in range(lyr.GetFeatureCount()):
        lyr_f=lyr.GetNextFeature()
        row_d={}
        if lyr_f.GetGeometryRef().IsEmpty():
            continue
        try:
            geom=lyr_f.GetGeometryRef()
            geom.Transform(wgs84_to_mercator3857)
            statistics=Imagee(*cci__im.clip_by_shape(geom.ExportToWkt())).get_statistics(categoric=True,percentage=True,full=True)
            row_d['id']=lyr_f.GetFieldAsInteger64(0)
            row_d['lc_class']=statistics['mode']
            row_d['frequency']=statistics['frequencies'][str(statistics['mode'])]
            df=df.append(row_d,ignore_index=True)
        except:
            bad_list.append(lyr_f.GetFieldAsInteger64(0))
    df.set_index(df.id,inplace=True)
    dbs.execute("create table %s.atts_%s () inherits (%s) " % ( country.lower(),  table,  olu_attribute_set_table ))
    for index, row in df.iterrows():
        atts_fid=next(olu_atts_id_gen)
        dbs.execute('insert into %s.atts_%s (fid,atts,dataset_fid,clc_id) select %d as fid, \'{"frequency":%s}\'::jsonb as atts, %d as dataset_fid, %s as clc_id ' % (country.lower(),  table, atts_fid, row['frequency'] , cci_africa__ds.get_olu_id(), cci_clc[row['lc_class']] ) )
        dbs.execute('insert into %s (object_fid,atts_fid,atts_origin) select %d as object_fid, %d as atts_fid, %d as atts_origin ' % (olu_atts_to_object_table, index, atts_fid, 2) )
    dbs.execute("create unique index idx__%s__atts_%s__fid on %s.atts_%s using btree(fid);" % (country.lower(), table,country.lower(), table) )
    print (table)
