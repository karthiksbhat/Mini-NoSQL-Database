import lucene
import simplejson as json
import os
import csv
import snappy          #compression technology
import shutil
import base64
from org.apache.lucene.codecs import Codec
from org.apache.lucene.store import FSDirectory, SimpleFSDirectory
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, IndexReader
from org.apache.lucene.search import IndexSearcher, BooleanQuery, BooleanClause
from org.apache.lucene.util import Version
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.document import Document, Field
from java.io import File

INDEX_DIR_DEFAULT="IndexFiles.index"     #default value
primary_keys_map={}
to_be_compressed_map={}
MAX_RESULTS=1000

def store(collection_name,data,commit=False):
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
		for key in primary_keys_map[INDEX_DIR]:
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
		for primary_key in primary_keys_map[collection_name]:
			try:
				field=Field(primary_key,contents[primary_key],Field.Store.NO,Field.Index.NOT_ANALYZED)
				doc.add(field)
			except:
				primary_keys_map.pop(collection_name)
				return 101
		#compress data using snappy if compression is on		
		if to_be_compressed_map[collection_name]==True:
			# print "here"
			#data=data.encode('utf-8')
			data=base64.b64encode(snappy.compress(data))
			# print data
		else:
			data=base64.b64encode(data)

		field=Field("$DATA$",data,Field.Store.YES,Field.Index.ANALYZED)
		doc.add(field)
		writer.addDocument(doc)
		if commit==True:
			writer.commit()
		writer.close()
		return 000
	except:
		return 102

def  search(collection_name,tofind):
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
		if key in primary_keys_map[collection_name]:
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
			if to_be_compressed_map[collection_name]==True:
				temp=doc.get("$DATA$")
				data=snappy.uncompress(base64.b64decode(temp))
			else:
				temp=doc.get("$DATA$")
				data=base64.b64decode(temp)
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
			if to_be_compressed_map[collection_name]==True:
				temp=doc.get("$DATA$")
				data=snappy.uncompress(base64.b64decode(temp))
			else:
				temp=doc.get("$DATA$")
				data=base64.b64decode(temp)

				
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

def update(collection_name,tofind,update,commit=False,add_field_if_not_exists=True):
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
		for key in primary_keys_map[collection_name]:
			temp=QueryParser(Version.LUCENE_CURRENT,key,analyzer).parse(data[key])
			query.add(BooleanClause(temp,BooleanClause.Occur.MUST))
		
		#print query	
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
			if key in primary_keys_map[INDEX_DIR]:
				primary_key_update=True
				break
		if primary_key_update == True:		
			query_search=BooleanQuery()
			for key in primary_keys_map[INDEX_DIR]:
				temp=QueryParser(Version.LUCENE_CURRENT,key,analyzer).parse(data[key])
				query_search.add(BooleanClause(temp,BooleanClause.Occur.MUST))
			hits=searcher.search(query_search,MAX_RESULTS).scoreDocs
			if len(hits) > 0:
				return 106			
		writer.deleteDocuments(query)
		
		#add the newly modified document
		doc=Document()
		#index files wrt primary key
		for primary_key in primary_keys_map[collection_name]:
			try:
				field=Field(primary_key,data[primary_key],Field.Store.NO,Field.Index.NOT_ANALYZED)
				doc.add(field)
			except:
				primary_keys_map.pop(collection_name)
				return 101
		#compress data using snappy if compression is on		
		if to_be_compressed_map[collection_name]==True:
			temp=json.dumps(data)
			data_string=base64.b64encode(snappy.compress(temp))
		else:
			temp=json.dumps(data)
			data_string=base64.b64encode(temp)

		field=Field("$DATA$",data_string,Field.Store.YES,Field.Index.ANALYZED)
		doc.add(field)
		writer.addDocument(doc)

	tofind_primary_keyvalue_pairs={}
	tofind_nonprimary_keyvalue_pairs={}

	#separating out primary and non_primary keys
	for key in tofind_keyvalue_pairs.keys():
		if key in primary_keys_map[collection_name]:
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
			if to_be_compressed_map[collection_name]==True:
				temp=doc.get("$DATA$")
				data=snappy.uncompress(base64.b64decode(temp))
			else:
				temp=doc.get("$DATA$")
				data=base64.b64decode(temp)
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
						return 106	
			else:
				if rewrite(data)!=106:
					no_of_documents_modified+=1
				else:
					return 106
				
			
	else:
		for i in range(0,ireader.numDocs()):
			doc=searcher.doc(i)
			if to_be_compressed_map[collection_name]==True:
				temp=doc.get("$DATA$")
				data=snappy.uncompress(base64.b64decode(temp))
			else:
				temp=doc.get("$DATA$")
				data=base64.b64decode(temp)

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
						return 106
			else:
				if rewrite(data)!=106:
					no_of_documents_modified+=1
				else:
					return 106
			
	
	ireader.close()
	if commit==True:
			writer.commit()
	writer.close()
	return str(no_of_documents_modified)+" have been modified"

def number(collection_name):
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

def delete(collection_name,todelete,commit=False):
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
		if key in primary_keys_map[collection_name]:
			tofind_primary_keyvalue_pairs[key]=tofind_keyvalue_pairs[key]
		else:
			tofind_nonprimary_keyvalue_pairs[key]=tofind_keyvalue_pairs[key]

	#filtering documents according to primary keys		
	query=BooleanQuery()
	for key in tofind_primary_keyvalue_pairs.keys():
		temp=QueryParser(Version.LUCENE_CURRENT,key,analyzer).parse(tofind_primary_keyvalue_pairs[key])
		query.add(BooleanClause(temp,BooleanClause.Occur.MUST))

	writer.deleteDocuments(query)
	if commit==True:
		writer.commit()
	writer.close()
	return 000;

def commit(collection_name):
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

def desc(collection_name):
	if collection_name not in primary_keys_map.keys():
		return 105
	description={"collection_name":collection_name,"primary_keys":primary_keys_map[collection_name],"compressed":to_be_compressed_map[collection_name],"compressed_type":"snappy","NumberOfRecords":number(collection_name)}
	return description

if __name__ == "__main__":
	#init lucene vm
	lucene.initVM()
	#init rabbit-mq server
	
	#####load required resources from metafile ##################
	print "Initialized lucene with version number :",lucene.VERSION
	if os.path.exists("collectionmetafile.csv"):	
		
		f=open("collectionmetafile.csv",'rb')
		for key, val, compressed in csv.reader(f):
			primary_keys_map[key]=eval(val)
			to_be_compressed_map[key]=eval(compressed)

		f.close()


	
	###use RabbitMQ to handle multiple requests and call appropriate functions
	###remove this lame if else conditional execution
	
	while(True):
		choice=raw_input("Enter operation to be performed(store,select,delete,update,number,desc,exit,commit,rollback)")
		if (choice=="store"):
						collection_name=raw_input("Enter name of the Collection(ENTER \"DEFAULT\" for default table)::")
						data=raw_input("Enter the data in json format::")
						if data is None:
							print "Enter non Null data!"
							continue
						if(collection_name not in primary_keys_map):	
							#input and store primary-index keys
							primary_keys_input=raw_input("Enter primary key names separated by \',\'::")
							primary_keys=primary_keys_input.split(',')
							primary_keys_map[collection_name]=primary_keys
							#input compression enabled
							to_be_compressed=raw_input("Should data be compressed when stored?(Choose True if space is a constraint,False if time is a constraint!)::")
							if to_be_compressed in ["True","true"]:
								to_be_compressed_map[collection_name]=True
							else:
								to_be_compressed_map[collection_name]=False

						SUCCESS_MESSAGE=store(collection_name,data)

						if SUCCESS_MESSAGE==000:
							print "added to database successfully!"
							continue
						#later add success/failure codes for denoting what failed
						elif SUCCESS_MESSAGE==100:
							print "JSON format error!Check input!"
							continue
						elif SUCCESS_MESSAGE==101:
							print "Make sure you gave correct primary_keys!"
							continue
						elif SUCCESS_MESSAGE==102:
							print "Lucene Storage error!"
							continue
						elif SUCCESS_MESSAGE==106:
							print "Record with same primary keys already exists!"
							continue
						else:
							print "error in insertion!"
							continue
		elif (choice=="select"):
						collection_name=raw_input("Enter name of the Collection(ENTER \"DEFAULT\" for default table)::")
						
						####################  to be changed as select query  #############################
						#tofind_keyvalue_pairs={}
						#for primary_key in primary_keys_map[collection_name]:
						#	primary_value=raw_input("Enter the primary key value for "+primary_key+":::")
						#	tofind_keyvalue_pairs[primary_key]=primary_value
						tofind=raw_input("Enter key value pairs to search against in JSON format::")
						####################  to be changed as select query  ##############################	
						
						SUCCESS_MESSAGE=search(collection_name,tofind)

						if (not isinstance(SUCCESS_MESSAGE,int)):
							if SUCCESS_MESSAGE is None:
								print "No records Found"
							else:	
								print SUCCESS_MESSAGE
							continue
						#later add success/failure codes for denoting what failed
						elif SUCCESS_MESSAGE==100:
							print "JSON format error!Check input!"
							continue
						elif SUCCESS_MESSAGE==105:
							print "Invalid collection_name!"	
						else:
							print "error in Retrieval!"
							continue
		elif (choice=="delete"):
						collection_name=raw_input("Enter name of the Collection(ENTER \"DEFAULT\" for default table)::")
						tofind=raw_input("Enter key value pairs to search against in JSON format::")
						SUCCESS_MESSAGE=delete(collection_name,tofind)

						if SUCCESS_MESSAGE==105:
							print "Invalid collection_name!"
							continue
						elif SUCCESS_MESSAGE==100:
							print "JSON format error!Check input!"
							continue
						elif SUCCESS_MESSAGE==000:
							print "deleted appropriate records!"
		elif (choice=="number"):
						collection_name=raw_input("Enter name of the Collection(ENTER \"DEFAULT\" for default table)::")
						SUCCESS_MESSAGE=number(collection_name)
						if SUCCESS_MESSAGE == 105:
							print "Invalid collection_name!"
							continue
						else:	
							print SUCCESS_MESSAGE
		elif (choice=="commit"):
						collection_name=raw_input("Enter the collection_name to save changes to(warning:cannot be rolled back!)::")
						commit(collection_name)
						continue
		elif (choice=="rollback"):
						collection_name=raw_input("Enter the collection_name to save rollback changes(warning:unsaved commits will be lost!)::")
						commit(collection_name)
						continue
		elif (choice=="update"):
						collection_name=raw_input("Enter name of the Collection(ENTER \"DEFAULT\" for default table)::")
						tofind=raw_input("Enter key value pairs to search document to update,in JSON format::")
						update_pairs=raw_input("Enter the key value pairs to be updates(key-to-update:new-value) in JSON format(by default non existant key-to-update will be added to record)::")
						SUCCESS_MESSAGE=update(collection_name,tofind,update_pairs)

						if SUCCESS_MESSAGE==100:
							print "JSON format error!Check input!"
							continue
						elif SUCCESS_MESSAGE == 105:
							print "Invalid collection_name!"
							continue
						elif SUCCESS_MESSAGE==106:
							print "Record with same primary keys already exists!"
							continue
						else:
							print SUCCESS_MESSAGE
							continue
		elif (choice=="desc"):
						collection_name=raw_input("Enter name of the Collection(ENTER \"ALL\" for all tables)::")
						SUCCESS_MESSAGE=desc(collection_name)
						if (not isinstance(SUCCESS_MESSAGE,int)):
							print SUCCESS_MESSAGE
							continue
						if SUCCESS_MESSAGE==106:
							print "Invalid collection_name!"
							continue	
		elif (choice=="exit"):
						if len(primary_keys_map) > 0:
							f=open("collectionmetafile.csv","wb")
							w = csv.writer(f)
							for key, val in primary_keys_map.items():
								w.writerow([key, val,to_be_compressed_map[key]])
							f.close()
						break





	

	######################NOTES##############################
	
	#2)Set of existing primary keys for the frequently accessed collection
	#  can be loaded into p-memory just to prevent checking latency always while inserting.
	#3)Partial data can be cached for frequently accessed records.
	#2)Data overriten if same primary_key given
	#4)Storage can be done in BSON instead of JSON
	#5)Basic understanding is that we retrieve documents based on some primary_keys and process the returned records for further 
	#  filtering by parsing the JSON/BSON documents stored
	