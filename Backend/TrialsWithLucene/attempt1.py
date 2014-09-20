import lucene
import string
import simplejson as json

from lucene import Document
INDEX_DIR="IndexFiles.index"

def main1():
	print "started indexing sample files......"
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

	file1=open("nitin.json")
	data = file1.read()
	contents=json.loads(data)
	doc=lucene.Document()
	field=lucene.Field("name",contents['name'],lucene.Field.Store.NO,lucene.Field.Index.ANALYZED)
	doc.add(field)
	field=lucene.Field("data",data,lucene.Field.Store.YES,lucene.Field.Index.ANALYZED)
	doc.add(field)
	writer.addDocument(doc)
	file1.close()

	file1=open("nitin2.json")
	data=file1.read()
	contents=json.loads(data)
	doc=lucene.Document()
	field=lucene.Field("name",contents['name'],lucene.Field.Store.NO,lucene.Field.Index.ANALYZED)
	doc.add(field)
	field=lucene.Field("data",data,lucene.Field.Store.YES,lucene.Field.Index.ANALYZED)
	doc.add(field)
	writer.addDocument(doc)
	file1.close()

	writer.optimize()
	print "Indexed and optimized %d documents"%writer.numDocs()
	writer.close()

if __name__ == "__main__":
	lucene.initVM()
	print "Initializing lucene with version number :",lucene.VERSION
	main1()