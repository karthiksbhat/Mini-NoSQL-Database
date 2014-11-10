import httplib as http 
import urllib
import sys
import re
import json
import unicodedata
conn = http.HTTPConnection('10.42.0.1:3000')

query = str(sys.argv[1])
#The query is sent as an argument by hello.py, and is received heree

#The following lines determine the query being performed, by matching the first word
#of the string to its corresponding query
insertVar = re.match('^insert .*', query, re.M|re.I)
deleteVar = re.match('^delete .*', query, re.M|re.I)
descVar = re.match('^describe .*', query, re.M|re.I)
modifyVar = re.match('^modify .*', query, re.M|re.I)
displayVar = re.match('^display .*', query, re.M|re.I)
dropVar = re.match('^drop .*', query, re.M|re.I)
numberVar = re.match('^number .*', query, re.M|re.I)
#Regex matches are performed, to find keyword at the beginning of a line
#The matches are case insensitive.

#####################################################
'''
split by space, to obtain keywords
word = query.split(" ")

andChar = "&"
words = [word1+andChar for word1 in word]

length = len(words)
words[length-1]=words[length-1].replace("&","")
ords[0]=words[0].replace("&","")
print words
print query
'''

#In the case that the user has forgotten to add a space after the colon, we add it ourselves.
#Cuz changing the code to handle those requests seemed harder.
query = re.sub(':[ ]?',': ',query)
#print query
#####################################################

#The below regex defenition is used to determine major fields in the query.
# It finds a colon, and takes the word before the colon, the colon, a space, next few words
#as a match.
regex = re.compile(r'''
    [\S]+:                # a key (any word followed by a colon)
    (?:
    \s                    # then a space in between
        (?!\S+:)\S+       # then a value (any word not followed by a colon)
    )+                    # match multiple values if present
    ''', re.VERBOSE)

matches = regex.findall(query)

##########################################################################################################

if insertVar:

    #/api/insert?collection=<collection_name>&values=[attr1:val1,attr2:val2]&primary_keys="key1,key2"&compressed=True|true|false
    #The below regex defenition is used to determine major fields in the query.
    # It finds a colon, and takes the word before the colon, the colon, a space, next few words
    #as a match.
    regex = re.compile(r'''
    [\S]+:                # a key (any word followed by a colon)
    (?:
    \s                    # then a space in between
        (?!\S+:)\S+       # then a value (any word not followed by a colon)
    )+                    # match multiple values if present
    ''', re.VERBOSE)

    #conn.request("GET","/api/insert?"+"collection="+collName+"&values=["+keyvalue+"]&primary_keys="+primaryKey+"&compressed="+compressed

    matches = regex.findall(query)
    #Set collection variable as the element 'collection:collection-name'
    #and remove the entry from the list.
    collection= matches[0].replace("collection: ","collection:").split(":")
    collName= collection[1].strip()
    matches.remove(matches[0])

    #keyvalue holds a string of all key:value pairs, and is populated by the loop.
    keyvalue=""
    #print collName
    for match in matches:
        temp=match.split(": ")
        if(not(temp[0].strip()=="primary_keys" or temp[0].strip()=="compressed")):
            keyvalue=keyvalue+"\""+temp[0].strip()+"\":\""+temp[1].strip()+"\""
            if(not(matches.index(match)==len(matches)-1)):
                keyvalue+=","

    #The primaryKey string holds all the attributes which are primary keys to the collection
    primaryKey=""
    for match in matches:
        temp=match.split(": ")
        if(temp[0].strip()=="primary_keys"):
            primaryKey=primaryKey+"\""+temp[1].strip()+"\""

    #compressed is an attribute which holds whether the user wants the data compressed or not?
    compressed="false"
    for match in matches:
        temp=match.split(": ")
        if(temp[0].strip()=="compressed"):
            compressed=temp[1].strip()

    #The code to send the data in required format to the middleware goes here
    params = urllib.urlencode({'collection': collName,'values':"["+keyvalue+"]",'primary_keys':'"'+primaryKey+'"','compressed':compressed})
    headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    conn.request("POST","/api/insert", params, headers)

    # print "/api/insert?"+"collection="+collName+"&values=["+keyvalue+"]&primary_keys="+primaryKey+"&compressed="+compressed
    # conn.request("GET","/api/insert?"+"collection="+collName+"&values=["+keyvalue+"]&primary_keys="+primaryKey+"&compressed="+compressed)
    
    #a captures the response obtained from sending the data to the middleware
    a=conn.getresponse()
    
    #The output is obtained as unicode encoded data.
    #The below code is to decode the data.
    var1= json.load(a)
    var2=unicodedata.normalize('NFKD', var1['response']).encode('ascii','ignore')
    eval(var2)
    list1= eval(var2.replace("u\'{", "\'{"))

    #The code for printing out the output data.
    for a in list1:
        b=json.loads(a)
        for c in b:
            print c+"->"+b[c]
        print "\n"
#####################################################################################################################################
if deleteVar:
    
    #The below regex defenition is used to determine major fields in the query.
    # It finds a colon, and takes the word before the colon, the colon, a space, next few words
    #as a match.
    regex = re.compile(r'''
    [\S]+:                # a key (any word followed by a colon)
    (?:
    \s                    # then a space in between
        (?!\S+:)\S+       # then a value (any word not followed by a colon)
    )+                    # match multiple values if present
    ''', re.VERBOSE)
    matches = regex.findall(query)

    #conn.request("GET","/api/delete?"+"collection="+collName+"&values=["+keyvalue+"]"
    #Set collection variable as the element 'collection:collection-name'
    #and remove the entry from the list.
    collection= matches[0].replace("collection: ","collection:").split(":")
    collName= collection[1].strip()
    matches.remove(matches[0])

    #keyvalue holds a string of all key:value pairs, and is populated by the loop.
    keyvalue=""
    #print collName
    for match in matches:
        temp=match.split(": ")
        if(not(temp[0].strip()=="primary_keys" or temp[0].strip()=="compressed")):
            keyvalue=keyvalue+"\""+temp[0].strip()+"\":\""+temp[1].strip()+"\""
            if(not(matches.index(match)==len(matches)-1)):
                keyvalue+=","

    #The code to send the data in required format to the middleware goes here
    params = urllib.urlencode({'collection': collName,'values':"["+keyvalue+"]"})
    headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    conn.request("POST","/api/delete", params, headers)
    #print "/api/delete?"+"collection="+collName+"&values=["+keyvalue+"]"
    #conn.request("GET","/api/delete?"+"collection="+collName+"&values=["+keyvalue+"]")

    #a captures the response obtained from sending the data to the middleware
    a=conn.getresponse()

    #The output is obtained as unicode encoded data.
    #The below code is to decode the data.
    var1= json.load(a)
    var2=unicodedata.normalize('NFKD', var1['response']).encode('ascii','ignore')
    eval(var2)
    list1= eval(var2.replace("u\'{", "\'{"))

    #The code for printing out the output data.
    for a in list1:
        b=json.loads(a)
        for c in b:
            print c+"->"+b[c]
        print "\n"


#####################################################################################################################################
if displayVar:
    #The below regex defenition is used to determine major fields in the query.
    # It finds a colon, and takes the word before the colon, the colon, a space, next few words
    #as a match.

    regex = re.compile(r'''
    [\S]+:                # a key (any word followed by a colon)
    (?:
    \s                    # then a space in between
        (?!\S+:)\S+       # then a value (any word not followed by a colon)
    )+                    # match multiple values if present
    ''', re.VERBOSE)
    matches = regex.findall(query)

    #conn.request("GET","/api/delete?"+"collection="+collName+"&values=["+keyvalue+"]"

    #Set collection variable as the element 'collection:collection-name'
    #and remove the entry from the list.
    collection= matches[0].replace("collection: ","collection:").split(":")
    collName= collection[1].strip()
    matches.remove(matches[0])

    #keyvalue holds a string of all key:value pairs, and is populated by the loop.
    keyvalue=""
    #print collName
    for match in matches:
        temp=match.split(": ")
        if(not(temp[0].strip()=="primary_keys" or temp[0].strip()=="compressed")):
            keyvalue=keyvalue+"\""+temp[0].strip()+"\":\""+temp[1].strip()+"\""
            if(not(matches.index(match)==len(matches)-1)):
                keyvalue+=","

    #The code to send the data in required format to the middleware goes here
    params = urllib.urlencode({'collection': collName,'values':"["+keyvalue+"]"})
    headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    conn.request("POST","/api/display", params, headers)

    #conn.request("GET","/api/display?"+"collection="+collName+"&values=["+keyvalue+"]")

    #a captures the response obtained from sending the data to the middleware
    a=conn.getresponse()

    #The output is obtained as unicode encoded data.
    #The below code is to decode the data.
    var1= json.load(a)
    var2=unicodedata.normalize('NFKD', var1['response']).encode('ascii','ignore')
    eval(var2)
    list1= eval(var2.replace("u\'{", "\'{"))

    #The code for printing out the output data.
    for a in list1:
        b=json.loads(a)
        for c in b:
            print c+"->"+b[c]
        print "\n"


#####################################################################################################################################
if modifyVar:
    #The below regex defenition is used to determine major fields in the query.
    # It finds a colon, and takes the word before the colon, the colon, a space, next few words
    #as a match.

    mod = query.split("NEWVALUES")
    oldVals = mod[0]
    newVals = mod[1]

    # print oldVals
    # print "old vals done"
    # print newVals
    # print "newVals done"
    regex = re.compile(r'''
    [\S]+:                # a key (any word followed by a colon)
    (?:
    \s                    # then a space in between
        (?!\S+:)\S+       # then a value (any word not followed by a colon)
    )+                    # match multiple values if present
    ''', re.VERBOSE)

    matchesOld = regex.findall(oldVals)
    matchesNew = regex.findall(newVals)

    #Set collection variable as the element 'collection:collection-name'
    #and remove the entry from the list.
    collection= matchesOld[0].replace("collection: ","collection:").split(":")
    collName= collection[1].strip()
    matchesOld.remove(matchesOld[0])

    #keyvalue holds a string of all key:value pairs, and is populated by the loop.
    #keyvalueOld will hold the keyValue pair which will correspond to an element in the collection
    #keyvalueNew holds the updated values of the key:value pairs.
    keyvalueOld=""
    keyvalueNew=""
    #conn.request("GET","/api/modify?"+"collection="+collName+"&conditions=["+keyvalueOld+"]&values=["+keyvalueNew+"]"
    for match in matchesOld:
        temp=match.split(": ")
        if(not(temp[0].strip()=="primary_keys" or temp[0].strip()=="compressed")):
            keyvalueOld=keyvalueOld+"\""+temp[0].strip()+"\":\""+temp[1].strip()+"\""
            if(not(matchesOld.index(match)==len(matchesOld)-1)):
                keyvalueOld+=","

    for match in matchesNew:
        temp=match.split(": ")
        if(not(temp[0].strip()=="primary_keys" or temp[0].strip()=="compressed")):
            keyvalueNew=keyvalueNew+"\""+temp[0].strip()+"\":\""+temp[1].strip()+"\""
            if(not(matchesNew.index(match)==len(matchesNew)-1)):
                keyvalueNew+=","

    #The code to send the data in required format to the middleware goes here
    params = urllib.urlencode({'collection': collName,'conditions':"["+keyvalueOld+"]",'values':"["+keyvalueNew+"]"})
    headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    conn.request("POST","/api/modify", params, headers)
    #print "/api/modify?"+"collection="+collName+"&conditions=["+keyvalueOld+"]&values=["+keyvalueNew+"]"
    #conn.request("GET","/api/modify?"+"collection="+collName+"&conditions=["+keyvalueOld+"]&values=["+keyvalueNew+"]")

    #a captures the response obtained from sending the data to the middleware
    a=conn.getresponse()

    #The output is obtained as unicode encoded data.
    #The below code is to decode the data.
    var1= json.load(a)
    var2=unicodedata.normalize('NFKD', var1['response']).encode('ascii','ignore')
    eval(var2)
    list1= eval(var2.replace("u\'{", "\'{"))

    #The code for printing out the output data.
    for a in list1:
        b=json.loads(a)
        for c in b:
            print c+"->"+b[c]
        print "\n"


#####################################################################################################################################

if descVar:
    #The below regex defenition is used to determine major fields in the query.
    # It finds a colon, and takes the word before the colon, the colon, a space, next few words
    #as a match.

    regex = re.compile(r'''
    [\S]+:                # a key (any word followed by a colon)
    (?:
    \s                    # then a space in between
        (?!\S+:)\S+       # then a value (any word not followed by a colon)
    )+                    # match multiple values if present
    ''', re.VERBOSE)

    matches = regex.findall(query)

    #conn.request("GET","/api/desc?"+"collection="+collName
    #Set collection variable as the element 'collection:collection-name'
    #and remove the entry from the list.
    collection= matches[0].replace("collection: ","collection:").split(":")
    collName= collection[1].strip()
    matches.remove(matches[0])

    #The code to send the data in required format to the middleware goes here
    params = urllib.urlencode({'collection': collName})
    headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    conn.request("POST","/api/desc", params, headers)

    # print "/api/desc?"+"collection="+collName
    # conn.request("GET","/api/desc?"+"collection="+collName)

    #a captures the response obtained from sending the data to the middleware
    a=conn.getresponse()

    #The output is obtained as unicode encoded data.
    #The below code is to decode the data.
    var1= json.load(a)
    var2=var1['response']
    # var2=unicodedata.normalize('NFKD', var1['response']).encode('ascii','ignore')
    eval(var2)
    list1= eval(var2.replace("u\'{", "\'{"))

    #The code for printing out the output data.
    for a in list1:
        b=json.loads(a)
        # print b
        for c in b:
            # print type(c)
            # print type(b)
            print c+"->"+json.dumps(b[c])
        print "\n"

#####################################################################################################################################

if dropVar:
    #The below regex defenition is used to determine major fields in the query.
    #It finds a colon, and takes the word before the colon, the colon, a space, next few words
    #as a match.


    regex = re.compile(r'''
    [\S]+:                # a key (any word followed by a colon)
    (?:
    \s                    # then a space in between
        (?!\S+:)\S+       # then a value (any word not followed by a colon)
    )+                    # match multiple values if present
    ''', re.VERBOSE)

    matches = regex.findall(query)

    #conn.request("GET","/api/desc?"+"collection="+collName
    #Set collection variable as the element 'collection:collection-name'
    #and remove the entry from the list.
    collection= matches[0].replace("collection: ","collection:").split(":")
    collName= collection[1].strip()
    matches.remove(matches[0])

    #The code to send the data in required format to the middleware goes here
    params = urllib.urlencode({'collection': collName})
    headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    conn.request("POST","/api/drop", params, headers)

    # print "/api/desc?"+"collection="+collName
    # conn.request("GET","/api/desc?"+"collection="+collName)

    #a captures the response obtained from sending the data to the middleware
    a=conn.getresponse()

    #The output is obtained as unicode encoded data.
    #The below code is to decode the data.
    var1= json.load(a)
    var2=unicodedata.normalize('NFKD', var1['response']).encode('ascii','ignore')
    eval(var2)
    list1= eval(var2.replace("u\'{", "\'{"))

    #The code for printing out the output data.
    for a in list1:
        b=json.loads(a)
        for c in b:
            print c+"->"+b[c]
        print "\n"

#####################################################################################################################################
if numberVar:
    #The below regex defenition is used to determine major fields in the query.
    # It finds a colon, and takes the word before the colon, the colon, a space, next few words
    #as a match.


    regex = re.compile(r'''
    [\S]+:                # a key (any word followed by a colon)
    (?:
    \s                    # then a space in between
        (?!\S+:)\S+       # then a value (any word not followed by a colon)
    )+                    # match multiple values if present
    ''', re.VERBOSE)

    matches = regex.findall(query)

    #conn.request("GET","/api/desc?"+"collection="+collName

    #Set collection variable as the element 'collection:collection-name'
    #and remove the entry from the list.
    collection= matches[0].replace("collection: ","collection:").split(":")
    collName= collection[1].strip()
    matches.remove(matches[0])

    #The code to send the data in required format to the middleware goes here
    params = urllib.urlencode({'collection': collName})
    headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    conn.request("POST","/api/number", params, headers)

    # print "/api/desc?"+"collection="+collName
    # conn.request("GET","/api/desc?"+"collection="+collName)

    #a captures the response obtained from sending the data to the middleware
    a=conn.getresponse()

    #The output is obtained as unicode encoded data.
    #The below code is to decode the data.
    var1= json.load(a)
    var2=unicodedata.normalize('NFKD', var1['response']).encode('ascii','ignore')
    eval(var2)
    list1= eval(var2.replace("u\'{", "\'{"))

    #The code for printing out the output data.
    for a in list1:
        b=json.loads(a)
        for c in b:
            print c+"->"+b[c]
        print "\n"
#####################################################################################################################################