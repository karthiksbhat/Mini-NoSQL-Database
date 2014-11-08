import lucene
import simplejson as json
import os
import csv
import snappy          #compression technology
from org.apache.lucene.store import FSDirectory, SimpleFSDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, IndexReader
from org.apache.lucene.search import IndexSearcher, BooleanQuery, BooleanClause
from org.apache.lucene.util import Version
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.document import Document, Field
from java.io import File


def store(primary_keys_map,to_be_compressed_input,collection_name,data,commit=False):
	INDEX_DIR_DEFAULT="IndexFiles.index"
	if collection_name!="DEFAULT":
		INDEX_DIR=collection_name
	else:
		INDEX_DIR=INDEX_DIR_DEFAULT	
	print "started indexing input data......"
	
	#extracting values
	try:
		contents=json.loads(data)
	except:
		return 100


	direc=SimpleFSDirectory(File(INDEX_DIR))
	analyzer=StandardAnalyzer(Version.LUCENE_CURRENT)
	

	#checking for existance of record with same primary_key set
	try:
		ireader=IndexReader.open(direc)	
		searcher=IndexSearcher(ireader)
		query=BooleanQuery()
		for key in primary_keys_map:
			temp=QueryParser(Version.LUCENE_CURRENT,key,analyzer).parse(contents[key])
			query.add(BooleanClause(temp,BooleanClause.Occur.MUST))
		hits=searcher.search(query,MAX_RESULTS).scoreDocs
		if len(hits) > 0:
			return 106
	except:
		pass 	 
	
	


	#setting writer configurations
	config=IndexWriterConfig(Version.LUCENE_CURRENT,analyzer)
	config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
	writer=IndexWriter(direc,config)
	#fix this later.....FieldType not defined
	#field_type=FieldType()
	#field_type.setIndexed(True)
	#field_type.setStored(False)
	#field_type.setTokenized(False)
	
	try:
		doc=Document()
		#index files wrt primary key
		for primary_key in primary_keys_map:
			try:
				field=Field(primary_key,contents[primary_key],Field.Store.NO,Field.Index.ANALYZED)
				doc.add(field)
			except:
				# primary_keys_map.pop(collection_name)
				return 101
		#compress data using snappy if compression is on		
		if to_be_compressed_input==True:
			data=snappy.compress(data)
		field=Field("$DATA$",data,Field.Store.YES,Field.Index.ANALYZED)
		doc.add(field)
		writer.addDocument(doc)
		if commit==True:
			writer.commit()
		writer.close()
		return 000
	except:
		return 102

def  search(primary_keys_map,to_be_compressed_input,collection_name,tofind,MAX_RESULTS=1000):
	INDEX_DIR_DEFAULT="IndexFiles.index"
	if collection_name!="DEFAULT":
		INDEX_DIR=collection_name
	else:
		INDEX_DIR=INDEX_DIR_DEFAULT
	try:
		tofind_keyvalue_pairs=json.loads(tofind)
	except:
		return 100	
	direc=SimpleFSDirectory(File(INDEX_DIR))
	analyzer=StandardAnalyzer(Version.LUCENE_CURRENT)
	try:
		ireader=IndexReader.open(direc)	
		searcher=IndexSearcher(ireader)
	except:
		return 105

	#initializing return list 
	return_list=[]
	#check_list=[]
	tofind_primary_keyvalue_pairs={}
	tofind_nonprimary_keyvalue_pairs={}

	#separating out primary and non_primary keys
	for key in tofind_keyvalue_pairs.keys():
		if key in primary_keys_map:
			tofind_primary_keyvalue_pairs[key]=tofind_keyvalue_pairs[key]
		else:
			tofind_nonprimary_keyvalue_pairs[key]=tofind_keyvalue_pairs[key]

	#filtering documents		
	if len(tofind_primary_keyvalue_pairs)>0:		
		query=BooleanQuery()
		for key in tofind_primary_keyvalue_pairs.keys():
			temp=QueryParser(Version.LUCENE_CURRENT,key,analyzer).parse(tofind_primary_keyvalue_pairs[key])
			query.add(BooleanClause(temp,BooleanClause.Occur.MUST))
		hits=searcher.search(query,MAX_RESULTS).scoreDocs
		for hit in hits:
			doc=searcher.doc(hit.doc)
			if to_be_compressed_input==True:
				data=snappy.uncompress(doc.get("$DATA$"))
			else:
				data=doc.get("$DATA$")
			#non primary key filtering(without having to load all the primary key filtered values into main memory!)	
			if len(tofind_nonprimary_keyvalue_pairs)>0:
				entry=json.loads(data)
				satisfied=True
				for key in tofind_nonprimary_keyvalue_pairs.keys():
					if entry.get(key)!=tofind_nonprimary_keyvalue_pairs[key]:
						satisfied=False
						break
				if satisfied==True:
					return_list.append(data)
			else:
				return_list.append(data)
			
	else:
		for i in range(0,ireader.numDocs()):
			doc=searcher.doc(i)
			if to_be_compressed_input==True:
				data=snappy.uncompress(str(doc.get("$DATA$")))
			else:
				data=doc.get("$DATA$")

				
			#non primary key filtering(without having to load all the primary key filtered values into main memory!)	
			if len(tofind_nonprimary_keyvalue_pairs)>0:
				entry=json.loads(data)
				satisfied=True
				for key in tofind_nonprimary_keyvalue_pairs.keys():
					if entry.get(key)!=tofind_nonprimary_keyvalue_pairs[key]:
						satisfied=False
						break
				if satisfied==True:
					return_list.append(data)
			else:
				return_list.append(data)
			
	ireader.close()

	if len(return_list)==0:
		return None	
	else:
		return return_list 

def update(primary_keys_map,to_be_compressed_input,collection_name,tofind,update,commit=False,add_field_if_not_exists=True,MAX_RESULTS=1000):
	INDEX_DIR_DEFAULT="IndexFiles.index"
	#As of now the update will be implemented as search,modify data in json file,delete and re-write
	if collection_name!="DEFAULT":
		INDEX_DIR=collection_name
	else:
		INDEX_DIR=INDEX_DIR_DEFAULT
	try:
		tofind_keyvalue_pairs=json.loads(tofind)
	except:
		return 100	
	direc=SimpleFSDirectory(File(INDEX_DIR))
	analyzer=StandardAnalyzer(Version.LUCENE_CURRENT)
	try:
		ireader=IndexReader.open(direc)	
		searcher=IndexSearcher(ireader)
		#setting writer configurations
		config=IndexWriterConfig(Version.LUCENE_CURRENT,analyzer)
		config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
		writer=IndexWriter(direc,config)
	except:
		return 105
	no_of_documents_modified=0	
	#finding the document to update
	#Scope for making this more efficient
	def rewrite(data_string):
		data=json.loads(data_string)
		toupdate=json.loads(update)
		#primary_key_modified=False

		#delete the appropriate document
		query=BooleanQuery()
		for key in primary_keys_map:
			temp=QueryParser(Version.LUCENE_CURRENT,key,analyzer).parse(data[key])
			query.add(BooleanClause(temp,BooleanClause.Occur.MUST))
		

		#modify the values
		for key,value in toupdate.items():
			#if such a key is not present the we either add and update that key into data,or just ignore it!(By default it is set to True!)	
			if add_field_if_not_exists==False:
				if key in data.keys():
					data[key]=value
			else:		
				data[key]=value

		#this deletion statement has been intenstionally added here		
		#only if the modified data,has primary keys already not existing,will the updating process continue
		primary_key_update=False
		for key in toupdate.keys():
			if key in primary_keys_map:
				primary_key_update=True
				break
		if primary_key_update == True:
			query_search=BooleanQuery()
			for key in primary_keys_map:
				temp=QueryParser(Version.LUCENE_CURRENT,key,analyzer).parse(data[key])
				query_search.add(BooleanClause(temp,BooleanClause.Occur.MUST))
			hits=searcher.search(query_search,MAX_RESULTS).scoreDocs
			if len(hits) > 0:
				return 106			
		writer.deleteDocuments(query)

		#add the newly modified document
		doc=Document()
		#index files wrt primary key
		for primary_key in primary_keys_map:
			try:
				field=Field(primary_key,data[primary_key],Field.Store.NO,Field.Index.ANALYZED)
				doc.add(field)
			except:
				# primary_keys_map.pop(collection_name)
				return 101
		#compress data using snappy if compression is on		
		if to_be_compressed_input==True:
			data_string=snappy.compress(str(json.dumps(data)))
		else:
			data_string=json.dumps(data)	
		field=Field("$DATA$",data_string,Field.Store.YES,Field.Index.ANALYZED)
		doc.add(field)
		writer.addDocument(doc)

	tofind_primary_keyvalue_pairs={}
	tofind_nonprimary_keyvalue_pairs={}

	#separating out primary and non_primary keys
	for key in tofind_keyvalue_pairs.keys():
		if key in primary_keys_map:
			tofind_primary_keyvalue_pairs[key]=tofind_keyvalue_pairs[key]
		else:
			tofind_nonprimary_keyvalue_pairs[key]=tofind_keyvalue_pairs[key]

	#filtering documents		
	if len(tofind_primary_keyvalue_pairs)>0:		
		query=BooleanQuery()
		for key in tofind_primary_keyvalue_pairs.keys():
			temp=QueryParser(Version.LUCENE_CURRENT,key,analyzer).parse(tofind_primary_keyvalue_pairs[key])
			query.add(BooleanClause(temp,BooleanClause.Occur.MUST))
		hits=searcher.search(query,MAX_RESULTS).scoreDocs
		
		for hit in hits:
			doc=searcher.doc(hit.doc)
			if to_be_compressed_input==True:
				data=snappy.uncompress(doc.get("$DATA$"))
			else:
				data=doc.get("$DATA$")
			#non primary key filtering(without having to load all the primary key filtered values into main memory!)	
			if len(tofind_nonprimary_keyvalue_pairs)>0:
				entry=json.loads(data)
				satisfied=True
				for key in tofind_nonprimary_keyvalue_pairs.keys():
					if entry.get(key)!=tofind_nonprimary_keyvalue_pairs[key]:
						satisfied=False
						break
				if satisfied==True:
					if rewrite(data)!=106:
						no_of_documents_modified+=1
					else:
						writer.rollback()
						return 106	
			else:
				if rewrite(data)!=106:
					no_of_documents_modified+=1
				else:
					writer.rollback()
					return 106
				
			
	else:
		for i in range(0,ireader.numDocs()):
			doc=searcher.doc(i)
			if to_be_compressed_input==True:
				data=snappy.uncompress(doc.get("$DATA$"))
			else:
				data=doc.get("$DATA$")
			#non primary key filtering(without having to load all the primary key filtered values into main memory!)	
			if len(tofind_nonprimary_keyvalue_pairs)>0:
				entry=json.loads(data)
				satisfied=True
				for key in tofind_nonprimary_keyvalue_pairs.keys():
					if entry.get(key)!=tofind_nonprimary_keyvalue_pairs[key]:
						satisfied=False
						break
				if satisfied==True:
					if rewrite(data)!=106:
						no_of_documents_modified+=1
					else:
						writer.rollback()
						return 106
			else:
				if rewrite(data)!=106:
					no_of_documents_modified+=1
				else:
					writer.rollback()
					return 106
			
	
	ireader.close()
	if commit==True:
			writer.commit()
	writer.close()
	return str(no_of_documents_modified)+" have been modified"

def number(collection_name):
	INDEX_DIR_DEFAULT="IndexFiles.index"
	if collection_name!="DEFAULT":
		INDEX_DIR=collection_name
	else:
		INDEX_DIR=INDEX_DIR_DEFAULT
		
	direc=SimpleFSDirectory(File(INDEX_DIR))
  	analyzer=StandardAnalyzer(Version.LUCENE_CURRENT)
  	try:
  		ireader=IndexReader.open(direc)
  	except:
  		return 105
  	numdocs = int(ireader.numDocs())

  	ireader.close()
  	
  	return numdocs

def delete(primary_keys_map,collection_name,todelete,commit=False):
	INDEX_DIR_DEFAULT="IndexFiles.index"
	if collection_name!="DEFAULT":
		INDEX_DIR=collection_name
	else:
		INDEX_DIR=INDEX_DIR_DEFAULT

	try:
		tofind_keyvalue_pairs=json.loads(todelete)
	except:
		return 100	
	

	direc=SimpleFSDirectory(File(INDEX_DIR))
	analyzer=StandardAnalyzer(Version.LUCENE_CURRENT)

	#setting writer configurations
	try:
		config=IndexWriterConfig(Version.LUCENE_CURRENT,analyzer)
		config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
		writer=IndexWriter(direc,config)
		ireader=IndexReader.open(direc)
	except:
		return 105

	###as of now deletion of documents support is only based on indexed keys.###################3 
	tofind_primary_keyvalue_pairs={}
	tofind_nonprimary_keyvalue_pairs={}

	#separating out primary and non_primary keys
	for key in tofind_keyvalue_pairs.keys():
		if key in primary_keys_map:
			tofind_primary_keyvalue_pairs[key]=tofind_keyvalue_pairs[key]
		else:
			tofind_nonprimary_keyvalue_pairs[key]=tofind_keyvalue_pairs[key]

	#filtering documents according to primary keys		
	query=BooleanQuery()
	for key in tofind_primary_keyvalue_pairs.keys():
		temp=QueryParser(Version.LUCENE_CURRENT,key,analyzer).parse(tofind_primary_keyvalue_pairs[key])
		query.add(BooleanClause(temp,BooleanClause.Occur.MUST))

	a=writer.deleteDocuments(query)
	if commit==True:
		writer.commit()
	writer.close()
	return 000;

def commit(collection_name):
	INDEX_DIR_DEFAULT="IndexFiles.index"
	if collection_name!="DEFAULT":
		INDEX_DIR=collection_name
	else:
		INDEX_DIR=INDEX_DIR_DEFAULT

	direc=SimpleFSDirectory(File(INDEX_DIR))
	analyzer=StandardAnalyzer(Version.LUCENE_CURRENT)

	#setting writer configurations
	config=IndexWriterConfig(Version.LUCENE_CURRENT,analyzer)
	config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
	writer=IndexWriter(direc,config)

	writer.commit()
	writer.close()

def rollback(collection_name):
	INDEX_DIR_DEFAULT="IndexFiles.index"
	if collection_name!="DEFAULT":
		INDEX_DIR=collection_name
	else:
		INDEX_DIR=INDEX_DIR_DEFAULT

	direc=SimpleFSDirectory(File(INDEX_DIR))
	analyzer=StandardAnalyzer(Version.LUCENE_CURRENT)

	#setting writer configurations
	config=IndexWriterConfig(Version.LUCENE_CURRENT,analyzer)
	config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
	writer=IndexWriter(direc,config)

	writer.rollback()
	writer.close()

def desc(primary_keys_map,to_be_compressed,collection_name):
	description={"collection_name":collection_name,"primary_keys":primary_keys_map,"compressed":to_be_compressed,"compressed_type":"snappy","NumberOfRecords":number(collection_name)}
	return description

def drop(collection_name):
	try:
		#folder="ins_test2"
		#shutil.rmtree(folder)
		os.system('rm -r '+collection_name)
		#shutil.rmtree(collection_name)
		return 'deleted '+collection_name+' successfully'
	except:
		return 'Error deleting'