import lucene
import string
import simplejson as json

INDEX_DIR="IndexFiles.index"
MAX_RESULTS=1

def  search(searcher,analyzer):
	print 
	query=lucene.QueryParser(lucene.Version.LUCENE_CURRENT,"name",analyzer).parse("nitin")
	hits=searcher.search(query,MAX_RESULTS).scoreDocs
	for hit in hits:
		doc=searcher.doc(hit.doc)
		print "name is:nitin and data is:",doc

def search2(searcher,analyzer):
	print 
	query=lucene.QueryParser(lucene.Version.LUCENE_CURRENT,"name",analyzer).parse("nitin2")
	hits=searcher.search(query,MAX_RESULTS).scoreDocs
	for hit in hits:
		doc=searcher.doc(hit.doc)
		print "name is:nitin2 and data is:",doc

	print 
	print "Successfully retrieved documents....."	 

def main1():
	print "retrieve and display files......"
	direc=lucene.SimpleFSDirectory(lucene.File(INDEX_DIR))
	analyzer=lucene.StandardAnalyzer(lucene.Version.LUCENE_CURRENT)
	searcher=lucene.IndexSearcher(direc)
	search(searcher,analyzer)
	search2(searcher,analyzer)

if __name__ == '__main__':
	lucene.initVM()
	print "Started lucene with version number :",lucene.VERSION
	main1()