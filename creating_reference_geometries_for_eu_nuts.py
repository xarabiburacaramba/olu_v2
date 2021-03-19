from tridy import apply_function
import json
import sys
from osgeo import ogr

import psycopg2

#from credentials import connectionstring_localhost


import math
import numpy as np
np.set_printoptions(threshold=sys.maxsize)


def sequence_generator(starting_number):
    while starting_number<=10000000000000:
        starting_number+=1
        yield(starting_number)
        
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


lpis_hilucs={'2':'111', '3':'111', '4':'111', '5':'111', '6':'110', '7':'110', '9':'110', '10':'110', '11':'110', '12':'631', '91':'120', '97':'142', '98':'120', '99':'120'} 
ruian_zpv_hilucs={'1':'112', '2':'120', '3':'121', '4':'120', '5':'120', '6':'632', '7':'632', '8':'632', '9':'632', '10':'632', '11':'632', '12':'530', '13':'530', '14':'411', '15':'411', '16':'411', '17':'411', '18':'415', '19':'344', '20':'343', '21':'335', '22':'341', '23':'411', '24':'130', '25':'433', '26':'650', '27':'630', '28':'632', '29':'244','30':'630'}  
ruian_dp_hilucs={'2':'111', '3':'111', '4':'111', '5':'500', '6':'110', '7':'110', '8':'110', '10':'120', '11':'632', '13':'510', '14':'660'} 
corine_hilucs={'111':'500', '112':'500', '121':'660', '122':'410', '123':'414', '124':'413', '131':'130', '132':'433', '133':'660', '141':'340', '142':'340', '211':'111', '212':'111', '213':'111', '221':'111', '222':'111', '223':'111', '231':'111', '241':'111', '242':'111', '243':'111', '244':'120', '311':'120', '312':'120', '313':'120', '321':'631', '322':'631', '323':'631', '324':'631', '331':'631', '332':'631', '333':'631', '334':'620', '335':'631', '411':'631', '412':'631', '421':'631', '422':'631', '423':'631', '511':'414', '512':'660', '521':'660', '522':'414', '523':'660'} 
ua_hilucs={'11100':'500', '11200':'500', '11210':'500', '11220':'500', '11230':'500', '11240':'500', '11300':'500', '12100':'300', '12200':'410', '12210':'411', '12220':'411', '12230':'412', '12300':'414', '12400':'413', '13100':'130', '13300':'600', '13400':'600', '14100':'344', '14200':'340', '21000':'110', '22000':'110', '23000':'110', '24000':'110', '25000':'110', '30000':'120', '31000':'120', '32000':'120', '33000':'660', '40000':'660', '50000':'660', '91000':'660', '92000':'660'} 
lpis_clc={'2':'210','3':'220','4':'220','5':'220','6':'200','7':'231','9':'200','10':'200','11':'200','12':'240','91':'200','97':'500','98':'300','99':'300'}
ruian_zpv_clc={'1':'200','2':'200','3':'300','4':'300','5':'300','6':'510','7':'510','8':'510','9':'510','10':'510','11':'410','12':'100','13':'130','14':'122','15':'122','16':'122','17':'122','18':'122','19':'141','20':'142','21':'100','22':'100','23':'120','24':'131','25':'132','26':'100','27':'990','28':'510','29':'120','30':'300'}
ruian_dp_clc={'2':'210','3':'220','4':'220','5':'141','6':'200','7':'231','8':'231','10':'300','11':'510','13':'110','14':'990'}
ua_clc={'11100':'111','11200':'112','11210':'112','11220':'112','11230':'112','11240':'112','11300':'100','12100':'120','12210':'122','12200':'122','12220':'122','12230':'122','12300':'123','12400':'124','13100':'130','13300':'133','13400':'990','14100':'141','14200':'142','21000':'200','22000':'220','23000':'230','24000':'240','25000':'200','30000':'300','31000':'300','32000':'320','33000':'330','40000':'410','50000':'510','91000':'999','92000':'999'}

ua_conn=psycopg2.connect("dbname=urban_atlas host=localhost user=admin password=admin port=port_number")
corine_conn=psycopg2.connect("dbname=corine_land_cover host=localhost user=admin password=admin port=port_number")
olu_conn=psycopg2.connect("dbname=euoluv2 host=localhost user=admin password=admin port=port_number")

conn_corine=ogr.Open("PG: host=localhost dbname=corine_land_cover user=admin password=admin")
conn_ua=ogr.Open("PG: host=localhost dbname=urban_atlas user=admin password=admin")
ua_cur=ua_conn.cursor()
corine_cur=corine_conn.cursor()
olu_cur=olu_conn.cursor()
olu_cur2=olu_conn.cursor()
olu_cur.execute('SELECT last_value FROM olu2.olu_object_fid_seq')
olu_id_gen=sequence_generator(olu_cur.fetchone()[0])
olu_cur.execute('SELECT last_value FROM olu2.olu_attributes_fid_seq')
olu_atts_id_gen=sequence_generator(olu_cur.fetchone()[0])
olu_cur2.execute("select st_asbinary(geom), fid from olu2.administrative_unit where level_code=3")

def ua_transformations(object,object_fid,attributes_fid,admunit_id,object_table_name,attributes_table_name,attributes2object_table_name,object2nuts_table_name): 
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
     'object_properties':{'function':(lambda x,y,z: {**{'fid':x},**{'dataset_id':3},**{'z_value':1000},**{'admunit_id':y},**{'valid_from':(z['properties']['prod_date']+'-01-01'),'valid_to':'2024-01-01'} }),'parameters':['object_id','admunit_id','feature_json']},
     'attributes_properties':{'function':(lambda x,y: {**{'fid':x},**{'hilucs_value':ua_hilucs[y['properties']['code2012']]},**{'clc_value':ua_clc[y['properties']['code2012']]},**{'atts':y['properties']},**{'dataset_id':3}}),'parameters':['attributes_id','feature_json']},
     'object_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,dataset_fid,z_value,geom,valid_from,valid_to) VALUES (%s,%s,%s,ST_SetSRID(ST_Multi(ST_GeomFromText('%s')),4326),'%s','%s')" % (w,x,z['dataset_id'],z['z_value'],y,z['valid_from'], z['valid_to'] ) ),'parameters':['object_table_name','object_id','object_geom','object_properties']},\
     'attributes_insert_statement':{'function':(lambda w,x,y,z: "INSERT INTO %s (fid,hilucs_id,atts,dataset_fid,clc_id) VALUES (%s,%s,('%s'::json),%s,%s)" % (w, x,z['hilucs_value'],json.dumps(y['properties']),z['dataset_id'],z['clc_value']) ),'parameters':['attributes_table_name','attributes_id','feature_json','attributes_properties']},\
     'attributes2object_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,atts_fid,atts_origin) VALUES (%s,%s,1)" % (z,x,y) ),'parameters':['object_id','attributes_id','attributes2object_table_name']},\
     'object2admunit_insert_statement':{'function':(lambda x,y,z: "INSERT INTO %s (object_fid,unit_fid) VALUES (%s,'%s')" % (z,x,y) ),'parameters':['object_id','admunit_id','object2nuts_table_name']},\
     'four_insert_statements':{'function':(lambda w,x,y,z: [w,x,y,z]),'parameters':['object_insert_statement','attributes_insert_statement','attributes2object_insert_statement','object2admunit_insert_statement']}
    }
    return o_dict

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
    
while True:
    geom, nuts_id= olu_cur2.fetchone()
    
    olu_cur.execute('drop table if exists olu_object.%s' % nuts_id.lower())
    olu_cur.execute('drop table if exists olu_attribute_set.%s' % nuts_id.lower())
    olu_cur.execute('drop table if exists atts_to_object.%s' % nuts_id.lower())
    olu_cur.execute('drop table if exists olu_object_to_admin_unit.%s' % nuts_id.lower())
    
    olu_cur.execute('create table if not exists olu_object.%s () inherits (olu2.olu_object)' % nuts_id.lower())
    olu_cur.execute('create table if not exists olu_attribute_set.%s () inherits (olu2.olu_attribute_set)' % nuts_id.lower())
    olu_cur.execute('create table if not exists atts_to_object.%s () inherits (olu2.atts_to_object)' % nuts_id.lower())
    olu_cur.execute('create table if not exists olu_object_to_admin_unit.%s () inherits (olu2.olu_object_to_admin_unit)' % nuts_id.lower())
    
    olu_conn.commit()
    
    olu_object_table='olu_object.%s' % nuts_id.lower()
    olu_attribute_set_table='olu_attribute_set.%s' % nuts_id.lower()
    olu_atts_to_object_table='atts_to_object.%s' % nuts_id.lower()
    olu_object_to_admin_unit_table='olu_object_to_admin_unit.%s' % nuts_id.lower()
    
    corine_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'data' and table_name = '%s'" % nuts_id[:2].lower())
    
    if corine_cur.fetchone() is not None:
        corine_layer=conn_corine.ExecuteSQL("select * from data.%s where st_intersects(geom,st_geomfromtext('%s',4326)) and year='2018'" % (nuts_id[:2].lower(),ogr.CreateGeometryFromWkb(geom).ExportToWkt() ) )
        parts=corine_layer.GetFeatureCount()
        number=100
        i=1
        count=0
        j=math.ceil(parts/number)
        while i<j:
            for k in range(number):
                olu_id=next(olu_id_gen)
                atts_id=next(olu_atts_id_gen)
                f=corine_layer.GetNextFeature()
                #[print(i) for i in apply_function(corine_transformations(f,olu_id,atts_id,nuts_id,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table,olu_object_to_admin_unit_table),'four_insert_statements')]
                [olu_cur.execute(i) for i in apply_function(corine_transformations(f,olu_id,atts_id,nuts_id,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table,olu_object_to_admin_unit_table),'four_insert_statements')]
                count+=1
            olu_conn.commit()
            i+=1
        
        for k in range(parts-count):
            olu_id=next(olu_id_gen)
            atts_id=next(olu_atts_id_gen)
            f=corine_layer.GetNextFeature()
            [olu_cur.execute(i) for i in apply_function(corine_transformations(f,olu_id,atts_id,nuts_id,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table,olu_object_to_admin_unit_table),'four_insert_statements')]

        olu_conn.commit()
        del(corine_layer)
    ua_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'data' and table_name = '%s'" % nuts_id[:2].lower())
    if ua_cur.fetchone() is not None:
        ua_layer=conn_ua.ExecuteSQL("select * from data.%s where st_intersects(geom,st_geomfromtext('%s',4326)) and code2012 not in ('12210','12220','12230')" % (nuts_id[:2].lower(),ogr.CreateGeometryFromWkb(geom).ExportToWkt() ) )
        parts=ua_layer.GetFeatureCount()
        number=100
        i=1
        count=0
        j=math.ceil(parts/number)
        while i<j:
            for k in range(number):
                olu_id=next(olu_id_gen)
                atts_id=next(olu_atts_id_gen)
                f=ua_layer.GetNextFeature()
                #[print(i) for i in apply_function(corine_transformations(f,olu_id,atts_id,nuts_id,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table,olu_object_to_admin_unit_table),'four_insert_statements')]
                [olu_cur.execute(i) for i in apply_function(ua_transformations(f,olu_id,atts_id,nuts_id,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table,olu_object_to_admin_unit_table),'four_insert_statements')]
                count+=1
            olu_conn.commit()
            i+=1
        
        for k in range(parts-count):
            olu_id=next(olu_id_gen)
            atts_id=next(olu_atts_id_gen)
            f=ua_layer.GetNextFeature()
            [olu_cur.execute(i) for i in apply_function(ua_transformations(f,olu_id,atts_id,nuts_id,olu_object_table,olu_attribute_set_table,olu_atts_to_object_table,olu_object_to_admin_unit_table),'four_insert_statements')]

        olu_conn.commit()
        del(ua_layer)

olu_cur.execute('SELECT max(fid) FROM olu2.olu_object')
max_value=olu_cur.fetchone()[0]
olu_cur.execute("select setval('olu2.olu_object_fid_seq', %s)" % max_value)

olu_cur.execute('SELECT max(fid) olu2.olu_attribute_set')
max_value=olu_cur.fetchone()[0]
olu_cur.execute("select setval('olu2.olu_attributes_fid_seq', %s)" % max_value)
