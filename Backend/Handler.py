import lucene
import string
import simplejson as json
import os
import csv
from lucene import Document

INDEX_DIR_DEFAULT="IndexFiles.index"     #default value
primary_keys_map={}
MAX_RESULTS=1000

def store(collection_name,data):
	if collection_name!="DEFAULT":
		INDEX_DIR=collection_name
	else:
		INDEX_DIR=INDEX_DIR_DEFAULT	
	print "started indexing input data......"
	#setting writer configurations
	direc=lucene.SimpleFSDirectory(lucene.File(INDEX_DIR))
	analyzer=lucene.StandardAnalyzer(lucene.Version.LUCENE_CURRENT)
	config=lucene.IndexWriterConfig(lucene.Version.LUCENE_CURRENT,analyzer)
	config.setOpenMode(lucene.IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
	writer=lucene.IndexWriter(direc,config)
	#fix this later.....FieldType not defined
	#field_type=lucene.FieldType()
	#field_type.setIndexed(True)
	#field_type.setStored(False)
	#field_type.setTokenized(False)
	try:
		contents=json.loads(data)
	except:
		return 100
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


def  search(collection_name,primary_keyvalue_pairs):
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
	query=lucene.BooleanQuery()
	for key in primary_keyvalue_pairs.keys():
		temp=lucene.QueryParser(lucene.Version.LUCENE_CURRENT,key,analyzer).parse(primary_keyvalue_pairs[key])
		query.add(lucene.BooleanClause(temp,lucene.BooleanClause.Occur.MUST))
	hits=searcher.search(query,MAX_RESULTS).scoreDocs
	
	return_list=[]
	for hit in hits:
		doc=searcher.doc(hit.doc)
		return_list.append(doc.get("$DATA$"))

	if len(return_list)==0:
		return None	
	else:
		return return_list 

def number(collection_name):
	if collection_name!="DEFAULT":
		INDEX_DIR=collection_name
	else:
		INDEX_DIR=INDEX_DIR_DEFAULT
	try:	
		direc=lucene.SimpleFSDirectory(lucene.File(INDEX_DIR))
  	except:
  		return 105
  	analyzer=lucene.StandardAnalyzer(lucene.Version.LUCENE_CURRENT)
  	searcher=lucene.IndexSearcher(direc)
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
		if(choice=="store"):
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
						else:
							print "error in insertion!"
							continue
		elif (choice=="select"):
						collection_name=raw_input("Enter name of the Collection(ENTER \"DEFAULT\" for default table)::")
						####################  to be changed as select query  #############################
						primary_keyvalue_pairs={}
						for primary_key in primary_keys_map[collection_name]:
							primary_value=raw_input("Enter the primary key value for "+primary_key+":::")
							primary_keyvalue_pairs[primary_key]=
						####################  to be changed as select query  ##############################	
						
						SUCCESS_MESSAGE=search(collection_name,primary_keyvalue_pairs)

						if (not isinstance(SUCCESS_MESSAGE,int)):
							if SUCCESS_MESSAGE is None:
								print "No records Found"
							else:	
								print SUCCESS_MESSAGE
							continue
						#later add success/failure codes for denoting what failed
						elif SUCCESS_MESSAGE==100:
							print "Check primary_key value!"
							continue
						elif SUCCESS_MESSAGE==101:
							print "Make sure you gave correct primary_keys!"
							continue
						elif SUCCESS_MESSAGE==103:
							print "Lucene Retrieval error!"
							continue
						elif SUCCESS_MESSAGE==105:
							print "Invalid collection_name!"	
						else:
							print "error in Retrieval!"
							continue
		elif (choice=="number"):
						collection_name=raw_input("Enter name of the Collection(ENTER \"DEFAULT\" for default table)::")
						SUCCESS_MESSAGE=number(collection_name)
						print SUCCESS_MESSAGE
		elif (choice=="exit"):
						f=open("metafile.csv","wb");
						w = csv.writer(f)
    					for key, val in primary_keys_map.items():
        					w.writerow([key, val])
    					f.close()
						break





	

	######################NOTES##############################
	#1)Make a check condition for primary key
	#2)Data overriten if same primary_key given