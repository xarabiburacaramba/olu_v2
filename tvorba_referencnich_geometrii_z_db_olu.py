from osgeo import ogr
import psycopg2
import numpy as np

olu_conn=psycopg2.connect("dbname=olu_db_name host=host user=username password=use_password port=port")

olu_cur=olu_conn.cursor()

olu_cur.execute('select a.fid from olu2.administrative_unit a right join(select distinct(unit_fid) from olu2.olu_object_to_admin_unit ) b on a.fid=b.unit_fid where a.level_code=4')

obce=[str(i[0]) for i in olu_cur.fetchall()]

datasets_belonging={'5':'spatial','1':'attribute','2':'attribute','3':'spatial','4':'spatial'}

all_ids=[]
for obec in obce:
    datasets=[5,1,2,3,4]
    obec_ids=[]
    olu_cur.execute("select st_asbinary(st_transform(st_setsrid(national_geom,5514),4326)) as wkb_geom from olu2.administrative_unit where fid='%s'" % obec)
    plocha=ogr.CreateGeometryFromWkb(olu_cur.fetchone()[0])
    for dataset in datasets:
        ids=[]
        if datasets_belonging[str(dataset)]=='attribute':
            if len(obec_ids)==0:
                olu_cur.execute("select b.fid from olu2.olu_object_to_admin_unit a left join olu2.olu_object b on a.object_fid=b.fid where b.dataset_fid=%d and a.unit_fid='%s'" % (dataset,obec))
                vysledky=olu_cur.fetchall()
                ids=[int(i[0]) for i in vysledky if len(vysledky)>0]
            else:
                try:
                    olu_cur.execute("select b.fid from olu2.olu_object_to_admin_unit a left join olu2.olu_object b on a.object_fid=b.fid where b.dataset_fid=%d and a.unit_fid='%s' and st_intersects(b.geom,st_setsrid(st_geomfromtext('%s'),4326))" % (dataset,obec,plocha.ExportToWkt()))
                except:
                    olu_conn.rollback()
                    olu_cur.execute("select b.fid from olu2.olu_object_to_admin_unit a left join olu2.olu_object b on a.object_fid=b.fid where b.dataset_fid=%d and a.unit_fid='%s' and st_intersects(st_buffer(st_makevalid(b.geom),0.0),st_buffer(st_makevalid(st_setsrid(st_geomfromtext('%s'),4326)),0))" % (dataset,obec,plocha.ExportToWkt()))
                vysledky=olu_cur.fetchall()
                ids=[int(i[0]) for i in vysledky if len(vysledky)>0]
        else:
            try:
                olu_cur.execute("select fid from olu2.olu_object where dataset_fid=%d and st_intersects(geom,st_setsrid(st_geomfromtext('%s'),4326))" % (dataset,plocha.ExportToWkt()))
            except:
                olu_conn.rollback()
                olu_cur.execute("select fid from olu2.olu_object where dataset_fid=%d and st_intersects(st_buffer(st_makevalid(geom),0.0),st_buffer(st_makevalid(st_setsrid(st_geomfromtext('%s'),4326)),0))" % (dataset,plocha.ExportToWkt()))              
            vysledky=olu_cur.fetchall()
            ids=[int(i[0]) for i in vysledky if len(vysledky)>0]     
        if len(ids)>0:
            obec_ids=np.append(obec_ids,ids)
            try:
                olu_cur.execute("select st_asbinary(st_difference(st_setsrid(st_geomfromtext('%s'),4326),st_union(geom))) from olu2.olu_object where fid in (%s) " % (plocha.ExportToWkt(),','.join([str(i) for i in ids])))
            except:
                olu_conn.rollback()
                olu_cur.execute("select st_asbinary(st_difference(st_buffer(st_makevalid(st_setsrid(st_geomfromtext('%s'),4326)),0.0),st_union(geom))) from olu2.olu_object where fid in (%s) " % (plocha.ExportToWkt(),','.join([str(i) for i in ids])))
            plocha=ogr.CreateGeometryFromWkb(olu_cur.fetchone()[0])
            plochab=plocha.Buffer(-0.00001)
            if plochab.Area()==0:
                break
    all_ids=np.append(all_ids,obec_ids)
    
all_ids_unique=np.unique(all_ids)

olu_cur.execute('create table temp.refgeom51234_ids (id integer)')

olu_conn.commit()

args='),('.join([str(int(i)) for i in all_ids_unique])

args='('+args+')'

olu_cur.execute('insert into temp.refgeom51234_ids values %s' % args)

olu_conn.commit()

olu_cur.execute('create unique index on temp.refgeom51234_ids using btree(id)')

olu_conn.commit()
