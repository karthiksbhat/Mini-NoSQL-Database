import lucene
import string
import simplejson as json
import os
from lucene import Document

INDEX_DIR="IndexFiles.index"     #default value
primary_keys_map={}

def store(collection_name,data):
	if collection_name!="DEFAULT":
		INDEX_DIR=collection_name

	print "started indexing input data......"

	#setting writer configurations
	direc=lucene.SimpleFSDirectory(lucene.File(INDEX_DIR))
	analyzer=lucene.StandardAnalyzer(lucene.Version.LUCENE_CURRENT)
	config=lucene.IndexWriterConfig(lucene.Version.LUCENE_CURRENT,analyzer)
	config.setOpenMode(lucene.IndexWriterConfig.OpenMode.CREATE)
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
				primary_keys.pop(collection_name)
				return 101
		field=lucene.Field("$DATA$",data,lucene.Field.Store.YES,lucene.Field.Index.ANALYZED)
		doc.add(field)
		writer.addDocument(doc)
		writer.optimize()
		writer.close()
		return 000
	except:
		return 102

if __name__ == "__main__":
	lucene.initVM()
	print "Initialized lucene with version number :",lucene.VERSION
	
	###use RabbitMQ to handle multiple requests and call appropriate functions
	
	while(True):
		collection_name=raw_input("Enter name of the Collection(ENTER \"STOP\" to exit)(ENTER \"DEFAULT\" for default table)::")
		if collection_name == "STOP":
			break
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


	

	######################NOTES##############################
	#1)Make a check condition for primary key