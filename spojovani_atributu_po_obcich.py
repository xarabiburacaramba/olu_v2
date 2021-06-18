from osgeo import ogr
import psycopg2
import time

olu_conn=psycopg2.connect("dbname=olu_db_name host=host user=username password=use_password port=port")

olu_cur=olu_conn.cursor()

olu_cur.execute('select a.fid from olu2.administrative_unit a right join(select distinct(unit_fid) from olu2.olu_object_to_admin_unit ) b on a.fid=b.unit_fid where a.level_code=4')

obce=[str(i[0]) for i in olu_cur.fetchall()]

datasets_belonging={'2':'attribute','1':'attribute','3':'spatial'}

for obec in obce:
    datasets=[2,1,3]
    olu_cur.execute("select st_asbinary(st_transform(st_setsrid(national_geom,5514),4326)) as wkb_geom from olu2.administrative_unit where fid='%s'" % obec)
    plocha=ogr.CreateGeometryFromWkb(olu_cur.fetchone()[0])
    for i in range(len(datasets)):
        if datasets_belonging[str(datasets[i])]=='attribute':
            TableObjA="select b.fid, b.geom from olu2.olu_object_to_admin_unit a left join olu2.olu_object b on a.object_fid=b.fid where b.dataset_fid=%d and a.unit_fid='%s'" % (datasets[i],  obec)
            for j in range(i+1,len(datasets)):
                if datasets_belonging[str(datasets[j])]=='attribute':
                    TableObjB="select b.fid, b.geom from olu2.olu_object_to_admin_unit a left join olu2.olu_object b on a.object_fid=b.fid where b.dataset_fid=%d and a.unit_fid='%s'" % (datasets[j],  obec)
                else:
                    TableObjB="select fid, geom from olu2.olu_object where dataset_fid=%d and st_intersects(geom,st_setsrid(st_geomfromtext('%s'),4326))" % (datasets[j], plocha.ExportToWkt())
                query=('with tableA as (%s), tableB as (%s) \
                insert into temp.atts_to_object (object_fid,atts_fid,atts_origin) \
                select a.fid as object_fid, b.atts_fid as atts_fid, 2 as atts_origin from tableA a inner join \
                (select a.fid, a.geom, b.atts_fid, b.atts_origin from tableB a left join olu2.atts_to_object b on a.fid=b.object_fid where b.atts_origin=1) b \
                on st_intersects(a.geom, b.geom)' % (TableObjA,TableObjB))
                start_time = time.time()
                olu_cur.execute(query)
                olu_conn.commit()
                print(obec, datasets[i], datasets[j], '--- %s seconds ---' % (time.time() - start_time))
        else:
            TableObjA="select fid, geom from olu2.olu_object where dataset_fid=%d and st_intersects(geom,st_setsrid(st_geomfromtext('%s'),4326))" % (datasets[i], plocha.ExportToWkt())
            for j in range(i+1,len(datasets)):
                if datasets_belonging[str(datasets[j])]=='attribute':
                    TableObjB="select b.fid, b.geom from olu2.olu_object_to_admin_unit a left join olu2.olu_object b on a.object_fid=b.fid where b.dataset_fid=%d and a.unit_fid='%s'" % (datasets[j],  obec)
                else:
                    TableObjB="select fid, geom from olu2.olu_object where dataset_fid=%d and st_intersects(geom,st_setsrid(st_geomfromtext('%s'),4326))" % (datasets[j], plocha.ExportToWkt())
                query=('with tableA as (%s), tableB as (%s) \
                insert into temp.atts_to_object (object_fid,atts_fid,atts_origin) \
                select a.fid as object_fid, b.atts_fid as atts_fid, 2 as atts_origin from tableA a inner join \
                (select a.fid, a.geom, b.atts_fid, b.atts_origin from tableB a left join olu2.atts_to_object b on a.fid=b.object_fid where b.atts_origin=1) b \
                on st_intersects(a.geom, b.geom)' % (TableObjA,TableObjB))
                start_time = time.time()
                olu_cur.execute(query)
                olu_conn.commit()
                print(obec, datasets[i], datasets[j], '--- %s seconds ---' % (time.time() - start_time))
                
'''
alter table temp.atts_to_object2 add column id serial primary key;

DELETE FROM temp.atts_to_object2 a USING temp.atts_to_object2 b WHERE a.id < b.id AND a.object_fid=b.object_fid AND a.atts_fid=b.atts_fid;

insert into olu2.atts_to_object (object_fid, atts_fid, atts_origin ) select object_fid, atts_fid, atts_origin from temp.atts_to_object2
'''
