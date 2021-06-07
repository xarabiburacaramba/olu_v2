from tridy import GeoConcept,  MetaData, Imagee,  ds_from_metadata
import sys
from osgeo import ogr

import psycopg2

import numpy as np
np.set_printoptions(threshold=sys.maxsize)

import pandas as pd

json_feature_with_raster_structure=[{"name":"id","type":"serial primary key"},{"name":"geom","type":"geometry"},{"name":"data","type":"json"},{"name":"raster_maps","type":"raster"}]

olu_conn=psycopg2.connect("dbname=database host=host user=user password=password port=port")

olu_cur=olu_conn.cursor()

conn_olu=ogr.Open("PG: host=host dbname=database user=user password=password")

# Example of Tanzania
olu_cur.execute("select id from admunits where adm1_code ilike 'TZA%'")

adm_units_ids=[int(i[0]) for i in olu_cur.fetchall()]

olu_cur.execute('create schema tanzania_lc')

olu_conn.commit()

olu_cur.execute('create table tanzania_lc (geom geometry, id integer, lc_class integer, frequency numeric(5,2), admunit integer)')

olu_conn.commit()

olu_cur.execute('CREATE SEQUENCE tanzania_lc_id_seq START 1 ;')

olu_conn.commit()

for adm_unit_id in adm_units_ids:
    olu_cur.execute("\
    create table temp_full_lines_polygons as select a.osm_id as id,a.way,'line' as type from tanzania.planet_osm_line a inner join admunits b on st_intersects(a.way,b.geom) where \
    ((bicycle is not null) or ( bridge is not null) or ( boundary is not null) or ( covered is not null) or ( embankment is not null) or ( foot is not null) or ( highway is not null) or ( junction is not null) or ( landuse is not null) or ( man_made is not null) or ( military is not null) or ( motorcar is not null) or ( \"natural\" is not null) or ( oneway is not null) or ( railway is not null) or ( ref is not null) or ( route is not null) or ( service is not null) or ( sport is not null) or ( surface is not null) or ( tourism is not null) or ( tracktype is not null) or ( water is not null) or ( waterway is not null) or ( wetland is not null) or ( wood is not null)) \
    and b.id=%s \
    union all \
    select osm_id as id, st_boundary(way) as way, 'polygon' as type from tanzania.planet_osm_polygon a inner join admunits b on st_intersects(a.way,b.geom) where \
    ((aeroway is not null ) or (harbour is not null ) or (highway is not null ) or (landuse is not null ) or (\"natural\" is not null ) or (railway is not null ) or (surface is not null ) or (water is not null ) or (waterway is not null ) or (wetland is not null ) or ( wood is not null)) \
    and b.id=%s \
    union all \
    select id, st_boundary(geom) as way, 'administrative' as type from admunits where id=%s" % (adm_unit_id,adm_unit_id,adm_unit_id) )
    olu_conn.commit()
    olu_cur.execute("\
    create table temp_polygonzed_full_lines as \
    WITH noded AS (SELECT ST_Union(way) geom FROM temp_full_lines_polygons) SELECT ST_Polygonize( geom) as geom FROM noded; \
    ")
    olu_conn.commit()
    olu_cur.execute("\
    create table temp_full_polygons as \
    select (st_dump(st_collectionextract(geom,3))).geom from temp_polygonzed_full_lines; \
    ")
    olu_conn.commit()
    olu_cur.execute("create table tanzania_lc.unit_%s () inherits (tanzania_lc) " % adm_unit_id)
    olu_cur.execute("insert into tanzania_lc.unit_%s (geom,admunit) select geom, %s as admunit from  temp_full_polygons" % (adm_unit_id, adm_unit_id) )
    olu_conn.commit()
    olu_cur.execute('drop table temp_full_lines_polygons')
    olu_cur.execute('drop table temp_polygonzed_full_lines')
    olu_cur.execute('drop table temp_full_polygons')
    olu_conn.commit()
    olu_cur.execute("UPDATE tanzania_lc.unit_%s SET id=nextval('tanzania_lc_id_seq');" % adm_unit_id)
    olu_conn.commit()
    olu_cur.execute("create unique index idx__tanzania_lc__unit_%s__id on tanzania_lc.unit_%s using btree(id);" % (adm_unit_id,adm_unit_id) )
    olu_cur.execute("create index sidx__tanzania_lc__unit_%s__geom on tanzania_lc.unit_%s using gist(geom);" % (adm_unit_id,adm_unit_id) )
    olu_conn.commit()
    olu_cur.execute("\
    delete from tanzania_lc.unit_%s where id in \
    (select a.id from tanzania_lc.unit_%s a, admunits b where st_area(st_intersection(a.geom, b.geom))<1 and b.id=%s);\
    " % (adm_unit_id,adm_unit_id,adm_unit_id) )
    olu_conn.commit()
    
    
olu_cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'tanzania_lc'")

tables=[str(i[0]) for i in olu_cur.fetchall()]

cci20__metadata=MetaData('CCI 20 meter African Land Cover map', {"local":"osm_africa/cci_tanzania_3857_2020.tif", "format":"GTiff"},'raster')
cci20__ds=ds_from_metadata(cci20__metadata)
cci20=GeoConcept('CCI 20 meter African Land Cover map','CCI 20 meter African Land Cover map by ESA. ', 'Feature',json_feature_with_raster_structure, data_source=cci20__ds,subgeoconcepts=[])
cci20.set_raster_output_backend('',cci20.get_data_source().get_attributes()['local'])

olu_cur.execute('create table tanzania_lc_atts (id integer, lc_class integer, frequency numeric(5,2))')

olu_conn.commit()

olu_cur.execute('create unique index idx__tanzania_lc_atts__id on kenya_lc_atts using btree(id) ')

olu_conn.commit()

for table in tables:
    lyr=conn_olu.GetLayer('tanzania_lc.%s' % table)
    bbox_r=[lyr.GetExtent()[0],lyr.GetExtent()[3],lyr.GetExtent()[1],lyr.GetExtent()[2]]
    cci20__im=cci20.read_raster_output_backend(1,bbox_r)
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
            statistics=Imagee(*cci20__im.clip_by_shape(lyr_f.GetGeometryRef().ExportToWkt())).get_statistics(categoric=True,percentage=True,full=True)
            row_d['id']=lyr_f.GetFieldAsInteger64(0)
            row_d['lc_class']=statistics['mode']
            row_d['frequency']=statistics['frequencies'][str(statistics['mode'])]
            df=df.append(row_d,ignore_index=True)
        except:
            bad_list.append(lyr_f.GetFieldAsInteger64(0))
    df.set_index(df.id,inplace=True)
    all_args=''
    for index, row in df.iterrows():
        args='),('.join([str(int(index))+','+str(int(row['lc_class']))+','+str(float(row['frequency']))])
        all_args+=',('+args+')'
    all_args=all_args[1:]
    olu_cur.execute('insert into tanzania_lc_atts values %s' % (all_args) )
    olu_conn.commit()
    olu_cur.execute('update %s.%s a set lc_class=b.lc_class, frequency=b.frequency from tanzania_lc_atts b where a.id=b.id' % ('tanzania_lc',table))
    olu_conn.commit()
    print(table)
