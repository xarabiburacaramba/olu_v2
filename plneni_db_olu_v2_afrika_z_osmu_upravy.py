import pandas as pd

from tridy import DBStorage, Table, Imagee, GeoConcept, MetaData,  ds_from_metadata
import psycopg2
import networkx as nx


import requests
import json
import os
import re
import subprocess
from osgeo import osr
import numpy as np
import paramiko

from credentials import connectionstring_localhost2, connectionstring_localhost3

df_staty=pd.read_csv('/home/dima/scripts/data/africke_staty.csv',names=['name','relation','iso_code3','osm_url'],skiprows=1)


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


cci_clc={1.0:'310',2.0:'320',3.0:'321',4.0:'200',5.0:'400',6.0:'322',7.0:'330',8.0:'100',9.0:'335',10.0:'500',200.0:'900'}


dbs=DBStorage({'dbname':'olu_africa'}.update(connectionstring_localhost2) )


dbs.connect()


conn_olu=dbs.create_ogr_connection()


country,country_wikiid,country_iso,country_osm=df_staty.iloc[2][['name','relation','iso_code3','osm_url']]

country_osm=(country.lower().replace(' ','-') if np.isnan(country_osm) else country_osm)

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
GROUP BY ?part ORDER BY ?label_en'''

get_osmid_query='''
SELECT 
  ?entity (SAMPLE(?osmid) as ?osmid)
WHERE {
  VALUES ?entity { <http://www.wikidata.org/entity/[entity]>}
  SERVICE <https://query.wikidata.org/sparql> { 
  OPTIONAL {?entity wdt:P402 ?osmid . } }
 }
GROUP BY ?entity'''

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


result_dict=[]
translation_dict={}


r = requests.get(url, params = {'format': 'json', 'query': get_osmid_query.replace('[entity]',country_wikiid)})


translation_dict.update({country_wikiid:r.json()['results']['bindings'][0]['osmid']['value']})


subprocess.run('wget -O /home/dima/scripts/data/%s-latest.osm.bz2 http://download.geofabrik.de/africa/%s-latest.osm.bz2' % ( country.lower().replace(' ','-'), country_osm), shell=True, check=True)

if os.path.isfile('/home/dima/scripts/data/%s-latest.osm.bz2' % country.lower().replace(' ','-')):
    subprocess.run('PGPASSWORD=%s osm2pgsql -d olu_africa -U %s -P %s  -H %s --slim -k /home/dima/scripts/data/%s-latest.osm.bz2' % (connectionstring_localhost2['password'], connectionstring_localhost2['user'], connectionstring_localhost2['port'], connectionstring_localhost2['host'], country.lower().replace(' ','-')), shell=True, check=True)
else:
    print({'error':'file wasnt downloaded'})


levels=[int(i[0]) for i in dbs.execute('select distinct admin_level from planet_osm_polygon where admin_level is not null and boundary=\'administrative\'')]
levels.sort()
g=nx.DiGraph()
seznam_celku=[(int(i[0]),{'level':int(i[1])}) for i in dbs.execute('select distinct on (osm_id,admin_level) osm_id,admin_level from planet_osm_polygon where admin_level is not null and boundary=\'administrative\'')]
g.add_nodes_from(seznam_celku)


for level_i in range(len(levels)-1):
    for n in g.nodes:
        if g.nodes[n]['level']==levels[level_i]:
            lower_units=[(n,int(i[0])) for i in dbs.execute('with node as (select osm_id, st_union(way) as geom from planet_osm_polygon where osm_id=%d group by osm_id), \
                        lower_units as (select osm_id, st_union(way) as geom from planet_osm_polygon where admin_level=\'%d\' and boundary=\'administrative\' group by osm_id) \
                        select a.osm_id from lower_units a, node b where st_within(a.geom,b.geom)' % (n,levels[level_i+1]))]
            if len(lower_units)>0:
                proportion=dbs.execute('with node as (select osm_id, st_union(way) as geom from planet_osm_polygon where osm_id=%d group by osm_id), \
                        lower_units as (select osm_id, st_union(way) as geom from planet_osm_polygon where admin_level=\'%d\' and boundary=\'administrative\' group by osm_id), \
                        lower_units_selection as (select a.osm_id, a.geom from lower_units a, node b where st_within(a.geom,b.geom))\
                        select (sum(st_area(b.geom))/st_area(a.geom))*100 from node a, lower_units_selection b group by a.geom' % (n,levels[level_i+1]))
                g.add_edges_from(lower_units)
                g.nodes[n]['proportion']=proportion[0][0]
                print(n,proportion)


vysledne_celky=[]
def select_final_units(graph, node, vysledne_celky, vyjimky=[None]):
    if node in vyjimky:
        for i in graph.neighbors(node):
            select_final_units(graph, i,  vysledne_celky)
    elif len([i for i in graph.reverse().neighbors(node)])==0:
        for i in graph.neighbors(node):
            select_final_units(graph, i,  vysledne_celky)  
    elif  ('proportion' not in graph.nodes[node]) or (graph.nodes[node]['proportion']<95) or (graph.nodes[node]['proportion']>105):
        vysledne_celky.append(node)
        return
    else:
        for i in graph.neighbors(node):
            select_final_units(graph, i,  vysledne_celky)

nodes=[]

for n in g.nodes():
    if len([i for i in g.reverse().neighbors(n)])==0 and g.nodes[n]['level']==2:
        nodes.append(n)

if len(nodes)==1:
    node=nodes[0]

select_final_units(g,  node, vysledne_celky)

results=dbs.execute("select osm_id, tags->'wikidata' as wikiid, admin_level from planet_osm_polygon where osm_id in %s" % str(vysledne_celky).replace('[','(').replace(']',')'), format='json')

for result in results:
    result_dict.append([translation_dict[country_wikiid],result['osm_id']*-1])
    translation_dict.update({result['wikiid']:result['osm_id']*-1})

dbs.execute('drop table if exists %s' % country.lower().replace(' ','-'))

node_record=AdmUnit(type='open', name=country ,  parent=None,  geomwkt=None,  properties=None,  wikiuri=wikidata_url+country_wikiid,  osmuri=osm_relations_url+translation_dict[country_wikiid], level=None)
node_record.copy_from_record('osm', dbs,  'planet_osm_polygon', {'_name':'name','_level':'admin_level','_geomwkt':'way'})
node_record.create_table(dbs, country.lower().replace(' ','-'))
node_record.insert_into_table(dbs, country.lower().replace(' ','-'))

for result in results:
    node_record=AdmUnit(type='open', name=None ,  parent=None,  geomwkt=None,  properties=None,  wikiuri=wikidata_url+result['wikiid'] if result['wikiid'] is not None else None,  osmuri=(osm_relations_url+str(result['osm_id']*-1) if result['osm_id']<0 else osm_ways_url+str(result['osm_id'])), level=4)
    node_record.copy_from_record('osm', dbs,  'planet_osm_polygon', {'_name':'name','_geomwkt':'way', '_level':'admin_level'})
    node_record.set_name(node_record.get_name().replace('\'','\'\''))
    node_record.insert_into_table(dbs, country.lower().replace(' ','-'))
    
for row in dbs.execute('with cela_zeme as (select id, level, st_geomfromtext(geomwkt) as geom from %s where level=2),\
regiony as (select id, level, st_geomfromtext(geomwkt) as geom from %s where level!=2 and geomwkt is not null),\
rozdil as (select st_astext(st_difference(a.geom, st_union(b.geom))) as geomwkt from cela_zeme a, regiony b group by a.geom)\
select row_number() over () as id, path[1], st_astext(geom) as geom from (select (st_dump(st_geomfromtext(geomwkt))).* from rozdil) a where st_area(geom)>100' % (country.lower().replace(' ','-'),country.lower().replace(' ','-')), format='json'):
    node_record_rest=AdmUnit(type='open', name='%s_rest_%s' % (country, str(row['id'])) ,  parent=None,  geomwkt=row['geom'],  properties=None,  wikiuri='rest_of_:'+wikidata_url+country_wikiid+'_'+str(row['id']),  osmuri='rest_of_:'+osm_relations_url+str(translation_dict[country_wikiid])+'_'+str(row['id']), level=999)
    node_record_rest.insert_into_table(dbs, country.lower().replace(' ','-'))
    
dbs.execute('alter table %s add column geom geometry;' % country.lower().replace(' ','-'))
dbs.execute('update %s set geom=st_setsrid(st_geomfromtext(geomwkt),3857);' % country.lower().replace(' ','-'))
dbs.execute('create index sidx__%s__geom on %s using gist(geom);' % (country.lower().replace(' ','-'),country.lower().replace(' ','-') ) )
dbs.execute('create index idx__%s__id on %s using btree(id);' % (country.lower().replace(' ','-'),country.lower().replace(' ','-') ) )

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

def sequence_generator(starting_number):
    while starting_number<=10000000000000:
        starting_number+=1
        yield(starting_number)

ssh_client =paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh_client.connect(hostname=connectionstring_localhost3['host'],username=connectionstring_localhost3['user'],password=connectionstring_localhost3['password'])

bbox_geo=[float(i) for i in re.findall('[-]?[0-9]+.[0-9]+',dbs.execute('select st_extent(st_buffer(st_setsrid(st_geomfromtext(geomwkt),3857),1000)) from %s where level=2' % country.lower().replace(' ','-'))[0][0])]

stdin,stdout,stderr=ssh_client.exec_command("gdalwarp -t_srs '+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs' -te %s -tr 20 20 -ot Byte  -of GTiff -co 'TILED=YES' -co 'TFW=YES' /home/dima/raster_data/landcover_2016/cci.tif /home/dima/raster_data/landcover_2016/cci_%s.tif" % (' '.join([str(i) for i in bbox_geo]), country.lower().replace(' ','-')) )
stdout.channel.recv_exit_status() 
subprocess.run("sshpass -p '%s' scp %s@%s:/home/dima/raster_data/landcover_2016/cci_%s.tif /home/dima/scripts/cci_%s.tif" % (connectionstring_localhost3['password'], connectionstring_localhost3['user'], connectionstring_localhost3['host'], country.lower().replace(' ','-'), country.lower().replace(' ','-')) , shell=True, check=True)
subprocess.run("sshpass -p '%s' scp %s@%s:/home/dima/raster_data/landcover_2016/cci_%s.tfw /home/dima/scripts/cci_%s.tfw" %  (connectionstring_localhost3['password'], connectionstring_localhost3['user'], connectionstring_localhost3['host'], country.lower().replace(' ','-'), country.lower().replace(' ','-')) , shell=True, check=True)

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
osm_africa__ds.set_olu_id(2)
dbs.execute(osm_africa__ds.convert_to_sql_insert())
osm_africa__ds.set_type('pbf')
cci_africa_metadata=MetaData('CCI 20m landcover raster of Africa', {"url":"http://2016africalandcover20m.esrin.esa.int/download.php", "format":"GTiff", "downloadable":"on registration"}, 'raster')
cci_africa__ds=ds_from_metadata(cci_africa_metadata)
cci_africa=GeoConcept('CCI 20m landcover raster of Africa','CCI 20m landcover raster of Africa.', 'Feature',json_feature_with_raster_structure, subgeoconcepts=[], data_source=cci_africa__ds)
cci_africa_metadata.set_metatable(metadata_table)
cci_africa_metadata.set_olu_id(next(metadata_fid_sequence))
cci_africa_metadata.set_validitydates(['2016-01-01','2116-01-01'])
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
cci_africa.set_raster_output_backend('/home/dima/scripts/','cci_%s.tif' % country.lower().replace(' ','-'))

dbs.execute('create schema %s' % country.lower().replace(' ','-'))
parent_values=dbs.execute("select * from %s where level=2" % country.lower().replace(' ','-'), format='json')[0]
dbs.execute("insert into olu2.administrative_unit (fid,level_code,unit_name,country_iso,geom) select '%s' as fid, %s as level_code, '%s' as unit_name, '%s' as country_iso, st_multi(st_transform(st_setsrid(st_geomfromtext('%s'),3857),4326)) as geom" % (parent_values['osmuri'], parent_values['level'], parent_values['name'] if parent_values['name'] is not None else '', country_iso,  parent_values['geomwkt'] ))

dbs.execute('alter table olu2.olu_object_to_admin_unit drop constraint object_to_admunit_object_fk')

adm_units_ids=[i[0] for i in dbs.execute('select osmuri from %s where level!=2' % country.lower().replace(' ','-'))]

bad_adm_unit_list=[]

for adm_unit_id in adm_units_ids :
    values=dbs.execute("select * from %s where osmuri='%s'" % (country.lower().replace(' ','-'), adm_unit_id), format='json')[0]
    try:
        dbs.execute("insert into olu2.administrative_unit (fid,parent_fid,level_code,unit_name,country_iso,geom) select '%s' as fid, '%s' as parent_fid, %s as level_code, '%s' as unit_name, '%s' as country_iso, st_multi(st_transform(st_setsrid(st_geomfromtext('%s'),3857),4326)) as geom" % (values['osmuri'],parent_values['osmuri'], values['level'], values['name'].replace('\'', '\'\'') if values['name'] is not None else '', country_iso, values['geomwkt'] ))    
    except:
        bad_adm_unit_list.append(adm_unit_id)
        dbs.disconnect(), dbs.connect()
        continue
    dbs.execute("\
    create table temp_full_lines_polygons as select a.osm_id as id,a.way,'line' as type from planet_osm_line a inner join %s b on st_intersects(a.way,b.geom) where \
    ((bicycle is not null) or ( bridge is not null) or ( boundary is not null) or ( covered is not null) or ( embankment is not null) or ( foot is not null) or ( highway is not null) or ( junction is not null) or ( landuse is not null) or ( man_made is not null) or ( military is not null) or ( motorcar is not null) or ( \"natural\" is not null) or ( oneway is not null) or ( railway is not null) or ( ref is not null) or ( route is not null) or ( service is not null) or ( sport is not null) or ( surface is not null) or ( tourism is not null) or ( tracktype is not null) or ( water is not null) or ( waterway is not null) or ( wetland is not null) or ( wood is not null)) \
    and b.osmuri='%s' \
    union all \
    select osm_id as id, st_boundary(way) as way, 'polygon' as type from planet_osm_polygon a inner join %s b on st_intersects(a.way,b.geom) where \
    ((aeroway is not null ) or (harbour is not null ) or (highway is not null ) or (landuse is not null ) or (\"natural\" is not null ) or (railway is not null ) or (surface is not null ) or (water is not null ) or (waterway is not null ) or (wetland is not null ) or ( wood is not null)) \
    and b.osmuri='%s' \
    union all \
    select %s as id, st_boundary(geom) as way, 'administrative' as type from %s where osmuri='%s'" % (country.lower().replace(' ','-'), adm_unit_id, country.lower().replace(' ','-'), adm_unit_id, adm_unit_id.split('/')[-1][0:].replace('_',''), country.lower().replace(' ','-'), adm_unit_id) )
    dbs.execute("\
    create table temp_polygonzed_full_lines as \
    WITH noded AS (SELECT ST_Union(way) geom FROM temp_full_lines_polygons) SELECT ST_Polygonize( geom) as geom FROM noded; \
    ")
    dbs.execute("\
    create table temp_full_polygons as \
    select (st_dump(st_collectionextract(geom,3))).geom from temp_polygonzed_full_lines; \
    ")
    dbs.execute('alter table temp_full_polygons add column fid serial primary key;')
    dbs.execute('create index sidx__temp_full_polygons__geom on temp_full_polygons using gist(geom);')
    dbs.execute('create index idx__temp_full_polygons__fid on temp_full_polygons using btree(fid);')
    dbs.execute("\
    delete from temp_full_polygons where fid in \
    (select a.fid from  temp_full_polygons a, %s b where st_within(a.geom, st_buffer(b.geom,5)) is false  and b.osmuri='%s');\
    " % (country.lower().replace(' ','-'), adm_unit_id) )
    dbs.execute("create table %s.unit_%s () inherits (%s) " % ( country.lower().replace(' ','-'),  adm_unit_id.split('/')[-1],  olu_object_table ))
    dbs.execute("ALTER TABLE %s.unit_%s ALTER COLUMN fid SET DEFAULT nextval('olu2.olu_object_fid_seq'::regclass)" % ( country.lower().replace(' ','-'),  adm_unit_id.split('/')[-1]))
    dbs.execute("insert into %s.unit_%s (dataset_fid,z_value,geom,valid_from,valid_to) select %d, %d, st_multi(st_transform(geom,4326)), '%s', '%s' from  temp_full_polygons" % (country.lower().replace(' ','-'),  adm_unit_id.split('/')[-1], osm_africa__ds.get_olu_id(),  100000,  osm_africa__ds.get_metadata().get_validitydates()[0],  osm_africa__ds.get_metadata().get_validitydates()[1] ) )
    dbs.execute('drop table temp_full_lines_polygons')
    dbs.execute('drop table temp_polygonzed_full_lines')
    dbs.execute('drop table temp_full_polygons')
    dbs.execute("create unique index idx__%s__unit_%s__fid on %s.unit_%s using btree(fid);" % (country.lower().replace(' ','-'), adm_unit_id.split('/')[-1],country.lower().replace(' ','-'), adm_unit_id.split('/')[-1]))
    dbs.execute("create index sidx__%s__unit_%s__geom on %s.unit_%s using gist(geom);"  % (country.lower().replace(' ','-'), adm_unit_id.split('/')[-1],country.lower().replace(' ','-'), adm_unit_id.split('/')[-1]))
    dbs.execute("insert into olu2.olu_object_to_admin_unit (object_fid,unit_fid) select fid as object_fid, '%s' as unit_fid from %s.unit_%s" % (parent_values['osmuri'],  country.lower().replace(' ','-'), adm_unit_id.split('/')[-1] ))
    dbs.execute("insert into olu2.olu_object_to_admin_unit (object_fid,unit_fid) select fid as object_fid, '%s' as unit_fid from %s.unit_%s" % (values['osmuri'],  country.lower().replace(' ','-'), adm_unit_id.split('/')[-1] ))

tables=[str(i[0]) for i in dbs.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'" % country.lower().replace(' ','-'))]
dbs.execute('alter table %s drop constraint oluatts_to_obj_attsfid' % olu_atts_to_object_table)
dbs.execute('alter table %s drop constraint oluatts_to_obj_objfid' % olu_atts_to_object_table)
