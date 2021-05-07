import psycopg2
import psycopg2.extras

import networkx as nx
from networkx.algorithms.traversal.depth_first_search import *
from networkx.algorithms.simple_paths import *

from lxml import etree
from datetime import date, timedelta
import requests


def examine(node,ap):
	if len(list(g.successors(node)))==0:
		print(node);
		print(ap*(g.node[node]['area_prop']));
		return;
	else:
		ap=ap*(g.node[node]['area_prop']);
		for i in g.successors(node):
			examine(i,ap);
	
class feature_OLU:
	def __init__(self, id, geom, eurostat_code, name, parent_id, hierarchy_level, country, area, national_code=None, name_lat=None, year_valid=None, population=None, area_prop=None, landuse=None, zdroje=None):
		self.id = id
		self.geom = geom
		self.eurostat_code = eurostat_code
		self.name = name
		self.parent_id = parent_id
		self.hierarchy_level = hierarchy_level
		self.country = country
		self.area = area
		self.national_code = national_code
		self.name_lat = name_lat
		self.year_valid = year_valid
		self.population = population
		self.area_prop = area_prop
		self.landuse = landuse
		self.zdroje = zdroje
	def get_id(self):
		print(self.name_lat)
	def get_springs(self):
		springs=[n for n in g.neighbors(self.id)]
		print(springs)
		return(springs)
	def print_landuse(self):
		return(self._landuse)
	def print_xml_feed(self): 
		NSMAP = {"dc" : 'http://purl.org/dc/elements/1.1', "xlink" : 'http://www.w3.org/1999/xlink', "srv":'http://www.isotc211.org/2005/srv', "gmd":"http://www.isotc211.org/2005/gmd"}
		thetail = "\n  \t \t"
		with open('E:/tmp/sablona_OLU_2.xml', 'r',encoding='utf8') as file :
			filedata = file.read()
		
		
		time_extent=''
		eurostat_code=self.eurostat_code
		eurostat_code_lc=(self.eurostat_code).lower()
		
		if self.parent_id is None:
			parent_id='-1'
			parent_eurostat_code='OLU'
		else:
			parent_id=self.parent_id
			parent_eurostat_code=g.node[parent_id]['eurostat_code']		
		
		
		if self.name:
			name=self.name
		else:
			name=''
		
		if name:
			name_or_eurostat_code=name
		else:
			name_or_eurostat_code=eurostat_code
			
		publication_date=str(date.today())		
		
		landuse_composition=g.node[self.id]['landuse']
		landuse_composition.update((x, round(float(y),2) ) for x, y in landuse_composition.items())
		landuse_composition={ k : v for k,v in landuse_composition.items() if v>=0.01}	
		#if sum(landuse_composition.values())!=100:
		if (99.99>sum(landuse_composition.values())) or (sum(landuse_composition.values())>100.01):
			landuse_composition[list(landuse_composition.keys())[0]]=list(landuse_composition.values())[0]+(100-sum(landuse_composition.values()))
			
		if '1' in landuse_composition.keys():
			primary_production=float(landuse_composition['1'])
		else:
			primary_production=0
		
		if '2' in landuse_composition.keys():
			secondary_production=float(landuse_composition['2'])
		else:
			secondary_production=0
			
		if '3' in landuse_composition.keys():
			tertiary_production=float(landuse_composition['3'])
		else:
			tertiary_production=0
			
		if '4' in landuse_composition.keys():
			tnlau=float(landuse_composition['4'])
		else:
			tnlau=0
			
		if '5' in landuse_composition.keys():
			residential_use=float(landuse_composition['5'])
		else:
			residential_use=0
			
		if '6' in landuse_composition.keys():
			other_uses=float(landuse_composition['6'])
		else:
			other_uses=0
			
		cursor.execute("select floor(st_xmin(geom)) as xmin, ceiling(st_xmax(geom)) as xmax, floor(st_ymin(geom)) as ymin, ceiling(st_ymax(geom)) as ymax, st_xmin(st_snaptogrid(st_transform(geom,4326),0.000001)) as xmin_4326, st_xmax(st_snaptogrid(st_transform(geom,4326),0.000001)) as xmax_4326, st_ymin(st_snaptogrid(st_transform(geom,4326),0.000001)) as ymin_4326, st_ymax(st_snaptogrid(st_transform(geom,4326),0.000001)) as ymax_4326 from european_data.elu_vsechny_celky where id='%s'" % self.id)
		rec=cursor.fetchone()
		
		xmin,xmax,ymin,ymax,xmin_4326,xmax_4326,ymin_4326,ymax_4326=rec[0],rec[1],rec[2],rec[3],rec[4],rec[5],rec[6],rec[7] 
		
		if (xmax-xmin)>=(ymax-ymin):
			height=256
			width=256*((xmax-xmin)/(ymax-ymin))
		else:
			width=256
			height=256*((ymax-ymin)/(xmax-xmin))
		
		sources_composition=g.node[self.id]['source']
		sources_composition.update((x, round(float(y),2) ) for x, y in sources_composition.items())
		sources_composition={ k : v for k,v in sources_composition.items() if v>=0.01}	
		if (99.99>sum(sources_composition.values())) or (sum(sources_composition.values())>100.01):
			sources_composition[list(sources_composition.keys())[0]]=list(sources_composition.values())[0]+(100-sum(sources_composition.values()))
		
		scale_denominator=set([])
		composition_by_datasources=''
		
		tree=etree.fromstring(filedata)
		
		if (name_or_eurostat_code[0:2]=='CZ' and self.hierarchy_level==5): 
			dodatok_extent='''<gmd:geographicElement xmlns:gmd="http://www.isotc211.org/2005/gmd>
							<gmd:EX_GeographicDescription>
							  <gmd:geographicIdentifier>
								<gmd:RS_Identifier>
								  <gmd:code>
									<gmx:Anchor xmlns:gmx="http://www.isotc211.org/2005/gmx" xmlns:xlink="http://www.w3.org/1999/xlink" xlink:href="https://linked.cuzk.cz/resource/ruian/obec/[czechstat_code]">[name]</gmx:Anchor>
								  </gmd:code>
								</gmd:RS_Identifier>
							  </gmd:geographicIdentifier>
							</gmd:EX_GeographicDescription>
						</gmd:geographicElement>'''
			for param, val in {"[name]":name,"[czechstat_code]":name[-6:]}.items():
				dodatok_extent = dodatok_extent.replace(param, val)		
			dodatok_extent = etree.fromstring(dodatok_extent)
			dodatok_extent.tail=thetail
			tree.find('.//gmd:EX_Extent', namespaces=NSMAP).append(dodatok_extent)
			
		if self.hierarchy_level==3: 
			dodatok_format='''<gmd:MD_Format xmlns:gmd="http://www.isotc211.org/2005/gmd">
								  <gmd:name>
									<gmx:Anchor xmlns:gmx="http://www.isotc211.org/2005/gmx" xmlns:xlink="http://www.w3.org/1999/xlink" xlink:href="http://www.gdal.org/drv_shapefile.html">ESRI Shapefile</gmx:Anchor>
								  </gmd:name>
								  <gmd:version>
									<gco:CharacterString xmlns:gco="http://www.isotc211.org/2005/gco">unknown</gco:CharacterString>
								  </gmd:version>
							  </gmd:MD_Format>'''	
			dodatok_format = etree.fromstring(dodatok_format)
			dodatok_format.tail=thetail
			tree.find('.//gmd:distributionFormat', namespaces=NSMAP).append(dodatok_format)
			
			dodatok_format_2='''<gmd:onLine xmlns:gmd="http://www.isotc211.org/2005/gmd">
								  <gmd:CI_OnlineResource>
									<gmd:linkage>
									  <gmd:URL>http://sdi4apps.eu/open_land_use/download/adm_unit/[eurostat_code]</gmd:URL>
									</gmd:linkage>
									<gmd:protocol>
									  <gmx:Anchor xmlns:gmx="http://www.isotc211.org/2005/gmx" xmlns:xlink="http://www.w3.org/1999/xlink" xlink:href="http://services.cuzk.cz/registry/codelist/OnlineResourceProtocolValue/WFS 1.1.0">WFS 1.1.0</gmx:Anchor>
									</gmd:protocol>
									<gmd:name>
									  <gco:CharacterString xmlns:gco="http://www.isotc211.org/2005/gco">.SHP download</gco:CharacterString>
									</gmd:name>
									<gmd:function>
									  <gmd:CI_OnLineFunctionCode codeListValue="download" codeList="http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#CI_OnLineFunctionCode">download</gmd:CI_OnLineFunctionCode>
									</gmd:function>
								  </gmd:CI_OnlineResource>
								</gmd:onLine>'''
			for param, val in {"[eurostat_code]":eurostat_code}.items():
				dodatok_format_2 = dodatok_format_2.replace(param, val)		
			dodatok_format_2 = etree.fromstring(dodatok_format_2)
			dodatok_format_2.tail=thetail
			tree.find('.//gmd:MD_DigitalTransferOptions', namespaces=NSMAP).append(dodatok_format_2)
			
		for key in sources_composition.keys():
			if key.startswith('ruian'):
				scale_denominator.add(1000)
				composition_by_datasources=composition_by_datasources+('RÚIAN - '+str(sources_composition[key])+'% ;')
				dodatok_RUIAN='''<gmd:source xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gml="http://www.opengis.net/gml" xmlns:gmi="http://standards.iso.org/iso/19115/-2/gmi/1.0" xmlns:srv="http://www.isotc211.org/2005/srv" xmlns:ogc="http://www.opengis.net/ogc" xmlns:gco="http://www.isotc211.org/2005/gco" xmlns:ows="http://www.opengis.net/ows" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:gmx="http://www.isotc211.org/2005/gmx">
				  <gmd:LI_Source>
					<gmd:sourceCitation xlink:href="https://micka.lesprojekt.cz/record/basic/CZ-CUZK-RUIAN_CR" xlink:title="RÚIAN"/>
				  </gmd:LI_Source>
				</gmd:source>'''
				dodatok_RUIAN = etree.fromstring(dodatok_RUIAN)
				dodatok_RUIAN.tail=thetail
				tree.find('.//gmd:LI_Lineage', namespaces=NSMAP).append(dodatok_RUIAN)		
				
			if key=='lpis':
				scale_denominator.add(1000)
				composition_by_datasources=composition_by_datasources+('Registr půdy LPIS - '+str(sources_composition[key])+'% ;')
				dodatok_LPIS='''<gmd:source xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gml="http://www.opengis.net/gml" xmlns:gmi="http://standards.iso.org/iso/19115/-2/gmi/1.0" xmlns:srv="http://www.isotc211.org/2005/srv" xmlns:ogc="http://www.opengis.net/ogc" xmlns:gco="http://www.isotc211.org/2005/gco" xmlns:ows="http://www.opengis.net/ows" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:gmx="http://www.isotc211.org/2005/gmx">
				  <gmd:LI_Source>
					<gmd:sourceCitation xlink:href="https://micka.lesprojekt.cz/record/basic/5a8e76e1-0874-4aa5-b616-0dc5585671f6" xlink:title="Registr půdy - LPIS"/>
				  </gmd:LI_Source>
				</gmd:source>'''
				dodatok_LPIS = etree.fromstring(dodatok_LPIS)
				dodatok_LPIS.tail=thetail
				tree.find('.//gmd:LI_Lineage', namespaces=NSMAP).append(dodatok_LPIS)	
				
			if key=='clc_2012':
				scale_denominator.add(100000)
				composition_by_datasources=composition_by_datasources+('Corine Land Cover 2012 - '+str(sources_composition[key])+'% ;')
				dodatok_clc='''<gmd:source xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gml="http://www.opengis.net/gml" xmlns:gmi="http://standards.iso.org/iso/19115/-2/gmi/1.0" xmlns:srv="http://www.isotc211.org/2005/srv" xmlns:ogc="http://www.opengis.net/ogc" xmlns:gco="http://www.isotc211.org/2005/gco" xmlns:ows="http://www.opengis.net/ows" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:gmx="http://www.isotc211.org/2005/gmx">
				  <gmd:LI_Source>
					<gmd:sourceCitation xlink:href="https://micka.lesprojekt.cz/record/basic/5a8e648c-2370-4e53-9475-0b4c585671f6" xlink:title="Corine Land Cover 2012"/>
				  </gmd:LI_Source>
				</gmd:source>'''
				dodatok_clc = etree.fromstring(dodatok_clc)
				dodatok_clc.tail=thetail
				tree.find('.//gmd:LI_Lineage', namespaces=NSMAP).append(dodatok_clc)	
				
			if key.startswith('urban atlas'):
				scale_denominator.add(10000)
				composition_by_datasources=composition_by_datasources+('Urban Atlas - '+str(sources_composition[key])+'% ;')
				dodatok_ua='''<gmd:source xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gml="http://www.opengis.net/gml" xmlns:gmi="http://standards.iso.org/iso/19115/-2/gmi/1.0" xmlns:srv="http://www.isotc211.org/2005/srv" xmlns:ogc="http://www.opengis.net/ogc" xmlns:gco="http://www.isotc211.org/2005/gco" xmlns:ows="http://www.opengis.net/ows" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:gmx="http://www.isotc211.org/2005/gmx">
				  <gmd:LI_Source>
					<gmd:sourceCitation xlink:href="https://micka.lesprojekt.cz/record/basic/5a8d6391-85f8-4062-ac69-29ba585671f6" xlink:title="Urban Atlas"/>
				  </gmd:LI_Source>
				</gmd:source>'''
				dodatok_ua = etree.fromstring(dodatok_ua)
				dodatok_ua.tail=thetail
				tree.find('.//gmd:LI_Lineage', namespaces=NSMAP).append(dodatok_ua)
				
			if key.startswith('SCHLAEGE'):
				scale_denominator.add(1000)
				composition_by_datasources=composition_by_datasources+('INVEKOS Referenzflächen, Feldstücke und Schläge - '+str(sources_composition[key])+'% ;')
				dodatok_invekos='''<gmd:source xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gml="http://www.opengis.net/gml" xmlns:gmi="http://standards.iso.org/iso/19115/-2/gmi/1.0" xmlns:srv="http://www.isotc211.org/2005/srv" xmlns:ogc="http://www.opengis.net/ogc" xmlns:gco="http://www.isotc211.org/2005/gco" xmlns:ows="http://www.opengis.net/ows" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:gmx="http://www.isotc211.org/2005/gmx">
				  <gmd:LI_Source>
					<gmd:sourceCitation xlink:href="https://micka.lesprojekt.cz/record/basic/bda17d44-e4d8-47bb-bc76-8ed8d1728223" xlink:title="INVEKOS Referenzflächen, Feldstücke und Schläge"/>
				  </gmd:LI_Source>
				</gmd:source>'''
				dodatok_invekos = etree.fromstring(dodatok_invekos)
				dodatok_invekos.tail=thetail
				tree.find('.//gmd:LI_Lineage', namespaces=NSMAP).append(dodatok_invekos)
				
			if key=='adp':
				scale_denominator.add(500)
				composition_by_datasources=composition_by_datasources+('GRBgis (Administratief perceel) - '+str(sources_composition[key])+'% ;')
				dodatok_grbgis='''<gmd:source xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gml="http://www.opengis.net/gml" xmlns:gmi="http://standards.iso.org/iso/19115/-2/gmi/1.0" xmlns:srv="http://www.isotc211.org/2005/srv" xmlns:ogc="http://www.opengis.net/ogc" xmlns:gco="http://www.isotc211.org/2005/gco" xmlns:ows="http://www.opengis.net/ows" xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:gmx="http://www.isotc211.org/2005/gmx">
				  <gmd:LI_Source>
					<gmd:sourceCitation xlink:href="https://micka.lesprojekt.cz/record/basic/4a889731-bee2-4a3b-b8f9-9e929a46c9af" xlink:title="GRBgis"/>
				  </gmd:LI_Source>
				</gmd:source>'''
				dodatok_grbgis = etree.fromstring(dodatok_grbgis)
				dodatok_grbgis.tail=thetail
				tree.find('.//gmd:LI_Lineage', namespaces=NSMAP).append(dodatok_grbgis)
				
			if key=='adp_1':
				composition_by_datasources=composition_by_datasources+('GRBgis (Administratief perceel 2) - '+str(sources_composition[key])+'% ;')
				
			if key=='gbg':
				composition_by_datasources=composition_by_datasources+('GRBgis (Gebouw aan de grond) - '+str(sources_composition[key])+'% ;')
				
			if key=='wbn':
				composition_by_datasources=composition_by_datasources+('GRBgis (Wegbaan) - '+str(sources_composition[key])+'% ;')
				
			if key=='wtz':
				composition_by_datasources=composition_by_datasources+('GRBgis (Watergang) - '+str(sources_composition[key])+'% ;')
				
			if key=='sbn':
				composition_by_datasources=composition_by_datasources+('GRBgis (Spoorbaan) - '+str(sources_composition[key])+'% ;')
				
			if key=='trn':
				composition_by_datasources=composition_by_datasources+('GRBgis (Terrein) - '+str(sources_composition[key])+'% ;')
				
		filedata=etree.tostring(tree, pretty_print=True, encoding="unicode")
		
		for param, val in {"[eurostat_code]":eurostat_code,"[parent_eurostat_code]":parent_eurostat_code,"[publication_date]":publication_date,"[name]":name,"[eurostat_code_lc]":eurostat_code_lc,"[primary_production]":str(primary_production),"[secondary_production]":str(secondary_production),"[tertiary_production]":str(tertiary_production),"[tnlau]":str(tnlau),"[residential_use]":str(residential_use),"[other_uses]":str(other_uses),"[xmin]":str(xmin_4326),"[ymin]":str(ymin_4326),"[xmax]":str(xmax_4326),"[ymax]":str(ymax_4326),"[xmin_3857]":str(xmin),"[ymin_3857]":str(ymin),"[xmax_3857]":str(xmax),"[ymax_3857]":str(ymax),"[width]":str(int(width)),"[height]":str(int(height)),"[name_or_eurostat_code]":name_or_eurostat_code,"[scale_denominator]":str(list(scale_denominator)),"[time_extent]":time_extent,"[composition_by_datasources]":composition_by_datasources}.items():
			filedata = filedata.replace(param, val)
			
		#with open(('E:/Documents/xmls/%s.xml' % eurostat_code), 'w', encoding='utf8') as file:
		#	file.write(filedata)
			
		headers={'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'}
		
		url='https://micka.lesprojekt.cz/csw/'
		
		response = requests.post(url,data=filedata.encode('UTF-8'),headers=headers,auth=('name','password'))
		
		print(response.text)
		
	

def main():
	global g
	global connection
	global cursor
	connection=psycopg2.connect("dbname=dbname user=user password=password host=host")
	cursor=connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
	cursor.execute("select id, eurostat_code, area_prop, landuse, zdroje from european_data.elu_vsechny_celky where st_area(geom)>0")
	records=cursor.fetchall()
	g=nx.DiGraph()
	for row in records:
		if (row[3] is None) & (row[4] is None):
			g.add_node(row[0], eurostat_code=row[1], area_prop=row[2], landuse=row[3], source=row[4])
		else:
			print(row[1])
			landuse_rm={}
			[landuse_rm.update(i) for i in row[3]]
			source_rm={}
			sum=0
			[source_rm.update(i) for i in row[4]]
			for key,value in list(source_rm.items()):
				if key[0:2]=='ur':
						sum+=float(value)
						source_rm.pop(key,None)
			if sum>0:
				source_rm['urban atlas']=sum
			g.add_node(row[0], eurostat_code=row[1], area_prop=row[2], landuse=landuse_rm, source=source_rm)
	cursor.execute("SELECT coalesce(parent_id, id) as id1, id as id2 from european_data.elu_vsechny_celky where st_area(geom)>0")
	records=cursor.fetchall()
	for row in records:
		g.add_edge(row[0],row[1])
	depth=len(list(all_simple_paths(g,23714,20862))[0])
	for d in range (depth,1,-1):
		first=dfs_postorder_nodes(g,23714,d)
		next=dfs_postorder_nodes(g,23714,d-1)
		for i in next:
			sum_list_landuse=[]
			sum_list_source=[]
			for j in first:
				if i!=j:
					print(j)
					dict_prop_landuse={}
					dict_prop_landuse.update((x, float(y)*g.node[j]['area_prop']) for x, y in g.node[j]['landuse'].items())
					sum_list_landuse.append(dict_prop_landuse)
					dict_prop_source={}
					dict_prop_source.update((x, float(y)*g.node[j]['area_prop']) for x, y in g.node[j]['source'].items())
					sum_list_source.append(dict_prop_source)				
				else:
					if len(sum_list_landuse)>0:
						dict_fin_landuse={}
						for dict in sum_list_landuse:
							for key,value in dict.items():
								if key in dict_fin_landuse.keys():
									dict_fin_landuse[key]=value+dict_fin_landuse[key]
								else:
									dict_fin_landuse[key]=value
						g.node[i]['landuse']=dict_fin_landuse
					if len(sum_list_source)>0:
						dict_fin_source={}
						for dict in sum_list_source:
							for key,value in dict.items():
								if key in dict_fin_source.keys():
									dict_fin_source[key]=value+dict_fin_source[key]
								else:
									dict_fin_source[key]=value
						g.node[i]['source']=dict_fin_source
					sum_list_landuse=[]
					sum_list_source=[]
					break
	
	order=dfs_postorder_nodes(g,23714,depth)
	
	#for node in order:
	#	if g.node[node]['eurostat_code']=='FR5322114056':
	#		break
	#	else:
	#		print(g.node[node]['eurostat_code'])
		
	#for node in order:
	cursor.execute("select id, st_astext(geom) as geom, eurostat_code, name, parent_id, hierarchy_level, country, area, national_code, name_lat, year_valid, pop as population, area_prop, landuse, zdroje from european_data.elu_vsechny_celky where id=%d" % 23714)
	rec=cursor.fetchone()
	o=feature_OLU(*rec)
	o.print_xml_feed()
					


if __name__ == "__main__":
	main()
	cursor.close()
	connection.close()
