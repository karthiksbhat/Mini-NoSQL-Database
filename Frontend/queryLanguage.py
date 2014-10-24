import httplib as http
#conn = http.HTTPConnection('localhost:3000/api')

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
print "1. insert in colletion=<collection> <attribute>=<value> <attribute>=<value>  and so on. It may be variable."
print "2. delete from collection=<collection> <attribute>=<value> <attribute>=<value> and so on."
print "3. modify collection=<collection> <attribute>=<value> <attribute>=<new_value>"
print "4. display collection=<collection> <attribute>=<value>"
print "Note: There are to be no commas in the commands"

#Taking user input as the query
query = raw_input("")

#split by space, to obtain keywords
word = query.split(" ")

andChar = "&"
words = [word1+andChar for word1 in word]

length = len(words)
words[length-1]=words[length-1].replace("&","")
words[0]=words[0].replace("&","")
print words

#basic parser to do the required functions
#this is now supposed to take the words written, and make them as key:value pairs and store in a json object
if words[0]=="insert":
    #call PrimaryKeyCheck()
    #/insert?id=2&ques=4&name=karthik
    print "/insert?"+"".join(words[2:])
    #print "".join(words[2:])
    #conn.request("GET","/insert?"+"".join(words[2:]))

    #print "insert("+",".join(words[2:])+")"

elif words[0]=="delete":
    #print "delete("+words[2]+","+",".join(words[7:])+")"
    print "/delete?"+"".join(words[2:])
    #conn.request("GET","/delete?"+"".join(words[2:]))

elif words[0]=="display":
    #print "display("+words[1]+","+",".join(words[6:])+")"
    print "/display?"+"".join(words[1:])
    #conn.request("GET","/display?"+"".join(words[1:]))

elif words[0]=="modify":
    #print "modify("+words[1]+","+words[6]+","+",".join(words[10:])+")"
    print "/modify?"+"".join(words[1:])
    #conn.request("GET","/modify?"+"".join(words[1:]))
#print words[0]

#test printing
#print query
#print words