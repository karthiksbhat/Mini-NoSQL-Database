import lucene
import string
import simplejson as json

INDEX_DIR="IndexFiles.index"
MAX_RESULTS=1

def  search(collection_name,primary_keyvalue_pairs):
	if collection_name!="DEFAULT":
		INDEX_DIR=collection_name


	try:	
		direc=lucene.SimpleFSDirectory(lucene.File(INDEX_DIR))
		analyzer=lucene.StandardAnalyzer(lucene.Version.LUCENE_CURRENT)
		searcher=lucene.IndexSearcher(direc)
		#query=lucene.QueryParser(lucene.Version.LUCENE_CURRENT,primary_key,analyzer).parse(primary_value)

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

	return return_list 

	
if __name__ == '__main__':
	lucene.initVM()
	print "Initialized lucene with version number :",lucene.VERSION
	
	###use RabbitMQ to handle multiple requests and call appropriate functions
	
	while(True):
		collection_name=raw_input("Enter name of the Collection(ENTER \"STOP\" to exit)(ENTER \"DEFAULT\" for default table)::")
		if collection_name == "STOP":
			break
		primary_keyvalue_pairs={}	
		for x in range(2):
			primary_key=raw_input("Enter the primary_key to search upon::")
			primary_value=raw_input("Enter the primary key value::")
			primary_keyvalue_pairs[primary_key]=primary_value

		SUCCESS_MESSAGE=search(collection_name,primary_keyvalue_pairs)

		if (not isinstance(SUCCESS_MESSAGE,int)):
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
		else:
			print "error in Retrieval!"
			continue



	#############################NOTES#############################
	#1)Make error messages		