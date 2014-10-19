import lucene
import simplejson as json
import os
import csv

INDEX_DIR_DEFAULT="IndexFiles.index"     #default value
primary_keys_map={}
MAX_RESULTS=1000

def store(collection_name,data):
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


	direc=lucene.SimpleFSDirectory(lucene.File(INDEX_DIR))
	analyzer=lucene.StandardAnalyzer(lucene.Version.LUCENE_CURRENT)
	

	#checking for existance of record with same primary_key set
	try:
		searcher=lucene.IndexSearcher(direc)
		query=lucene.BooleanQuery()
		for key in primary_keys_map[INDEX_DIR]:
			temp=lucene.QueryParser(lucene.Version.LUCENE_CURRENT,key,analyzer).parse(contents[key])
			query.add(lucene.BooleanClause(temp,lucene.BooleanClause.Occur.MUST))
		hits=searcher.search(query,MAX_RESULTS).scoreDocs
		if len(hits) > 0:
			return 106
	except:
		pass 	 
	
	


	#setting writer configurations
	config=lucene.IndexWriterConfig(lucene.Version.LUCENE_CURRENT,analyzer)
	config.setOpenMode(lucene.IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
	writer=lucene.IndexWriter(direc,config)
	#fix this later.....FieldType not defined
	#field_type=lucene.FieldType()
	#field_type.setIndexed(True)
	#field_type.setStored(False)
	#field_type.setTokenized(False)
	
	try:
		doc=lucene.Document()
		#index files wrt primary key
		for primary_key in primary_keys_map[collection_name]:
			try:
				field=lucene.Field(primary_key,contents[primary_key],lucene.Field.Store.NO,lucene.Field.Index.ANALYZED)
				doc.add(field)
			except:
				primary_keys_map.pop(collection_name)
				return 101
		field=lucene.Field("$DATA$",data,lucene.Field.Store.YES,lucene.Field.Index.ANALYZED)
		doc.add(field)
		writer.addDocument(doc)
		writer.optimize()
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
	direc=lucene.SimpleFSDirectory(lucene.File(INDEX_DIR))
	analyzer=lucene.StandardAnalyzer(lucene.Version.LUCENE_CURRENT)
	try:	
		searcher=lucene.IndexSearcher(direc)
	except:
		return 105

	#initializing return list 
	return_list=[]
	check_list=[]
	tofind_primary_keyvalue_pairs={}
	tofind_nonprimary_keyvalue_pairs={}

	#separating out primary and non_primary keys
	for key in tofind_keyvalue_pairs.keys():
		if key in primary_keys_map[collection_name]:
			tofind_primary_keyvalue_pairs[key]=tofind_keyvalue_pairs[key]
		else:
			tofind_nonprimary_keyvalue_pairs[key]=tofind_keyvalue_pairs[key]

	#filtering documents according to primary keys		
	if len(tofind_primary_keyvalue_pairs)>0:		
		query=lucene.BooleanQuery()
		for key in tofind_primary_keyvalue_pairs.keys():
			temp=lucene.QueryParser(lucene.Version.LUCENE_CURRENT,key,analyzer).parse(tofind_primary_keyvalue_pairs[key])
			query.add(lucene.BooleanClause(temp,lucene.BooleanClause.Occur.MUST))
		hits=searcher.search(query,MAX_RESULTS).scoreDocs
		
		for hit in hits:
			doc=searcher.doc(hit.doc)
			check_list.append(doc.get("$DATA$"))
	else:
		for i in range(0,searcher.maxDoc()):
			doc=searcher.doc(i)
			check_list.append(doc.get("$DATA$"))

	#filtering documents according to non primary keys ###########find a better method.more efficient
	if len(tofind_nonprimary_keyvalue_pairs)>0:
		for record in check_list:
			entry=json.loads(record)
			satisfied=True
			for key in tofind_nonprimary_keyvalue_pairs.keys():
				if entry.get(key)!=tofind_nonprimary_keyvalue_pairs[key]:
					satisfied=False
					break
			if satisfied==True:
				return_list.append(record)
	else:
		return_list=check_list

	

	if len(return_list)==0:
		return None	
	else:
		return return_list 

def number(collection_name):
	if collection_name!="DEFAULT":
		INDEX_DIR=collection_name
	else:
		INDEX_DIR=INDEX_DIR_DEFAULT
		
	direc=lucene.SimpleFSDirectory(lucene.File(INDEX_DIR))
  	analyzer=lucene.StandardAnalyzer(lucene.Version.LUCENE_CURRENT)
  	try:
  		searcher=lucene.IndexSearcher(direc)
  	except:
  		return 105
  	numdocs = int(searcher.maxDoc())
  	
  	return numdocs


if __name__ == "__main__":
	lucene.initVM()

	#####load required resources from metafile ##################
	print "Initialized lucene with version number :",lucene.VERSION
	if os.path.exists("metafile.csv"):	
		
		f=open("metafile.csv",'rb')
		for key, val in csv.reader(f):
			primary_keys_map[key]=eval(val)

		f.close()
	
	###use RabbitMQ to handle multiple requests and call appropriate functions
	###remove this lame if else conditional execution
	
	while(True):
		choice=raw_input("Enter operation to be performed(store,select,number,exit)")
		if (choice=="store"):
						collection_name=raw_input("Enter name of the Collection(ENTER \"DEFAULT\" for default table)::")
						
						data=raw_input("Enter the data in json format::")
						if data is None:
							print "Enter non Null data!"
							continue
						if(collection_name not in primary_keys_map):	
							primary_keys_input=raw_input("Enter primary key names separated by \',\'::")
							primary_keys=primary_keys_input.split(',')
							primary_keys_map[collection_name]=primary_keys

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
		elif (choice=="number"):
						collection_name=raw_input("Enter name of the Collection(ENTER \"DEFAULT\" for default table)::")
						SUCCESS_MESSAGE=number(collection_name)
						if SUCCESS_MESSAGE == 105:
							print "Invalid collection_name!"
							continue
						else:	
							print SUCCESS_MESSAGE
		elif (choice=="exit"):
						if len(primary_keys_map) > 0:
							f=open("metafile.csv","wb")
							w = csv.writer(f)
							for key, val in primary_keys_map.items():
								w.writerow([key, val])
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
	
