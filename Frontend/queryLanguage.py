import simplejson as json

#def PrimaryKeyCheck(collection):
#    if(pk of collection is notNull):
#        return 1
#    else:
#FIX INDENTATION HERE
#       PrimaryKey=raw_input("Enter the primary key for your collection")
#       pk[collection]=PrimaryKey
#       return 1
#   pass

print "Welcome to NoSQL-Name Database. You are currently using version 0.1. Here are a few specifications to follow while entering your queries."
print "1. insert in collection <primary key>:<value> <primary key>(optional):<value> <attribute>:<value>  and so on. It may be variable."
print "2. delete from collection where primary key is <value>"
print "3. modify collection where primary key is <value> new values are <attribute>:<value>"
print "4. display collection where primary key is <value>"
print "Note: There are to be no commas in the commands"
print ">>"

#Taking user input as the query
query = raw_input("")

#split by space, to obtain keywords
words = query.split(" ")

#basic parser to do the required functions
#this is now supposed to take the words written, and make them as key:value pairs and store in a json object
if words[0]=="insert":
    #call PrimaryKeyCheck()
    print "insert("+",".join(words[2:])+")"
    #new code here

elif words[0]=="delete":
    print "delete("+words[2]+","+",".join(words[7:])+")"
    #new code here

elif words[0]=="display":
    print "display("+words[1]+","+",".join(words[6:])+")"
    #new code here

elif words[0]=="modify":
    print "modify("+words[1]+","+words[6]+","+",".join(words[10:])+")"
    #new code here

print words[0]

#test printing
print query
print words