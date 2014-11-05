import httplib as http
import urllib
import sys
import re
import json
conn = http.HTTPConnection('localhost:3000')

"""
print "Welcome to NoSQL-Name Database. You are currently using version 0.1. Here are a few specifications to follow while entering your queries."
print "1. insert in collection=<collection> <attribute>=<value> <attribute>=<value>  and so on. It may be variable."
print "2. delete from collection=<collection> <attribute>=<value> <attribute>=<value> and so on."
print "3. modify collection=<collection> <attribute>=<value> <attribute>=<new_value>"
print "4. display collection=<collection> <attribute>=<value>"
print "Note: There are to be no commas in the commands"
"""
query = str(sys.argv[1])

insertVar = re.match('^insert .*', query, re.M|re.I)
deleteVar = re.match('^delete .*', query, re.M|re.I)
descVar = re.match('^describe .*', query, re.M|re.I)
modifyVar = re.match('^modify .*', query, re.M|re.I)
displayVar = re.match('^display .*', query, re.M|re.I)

#####################################################

#split by space, to obtain keywords
#word = query.split(" ")

#andChar = "&"
#words = [word1+andChar for word1 in word]

#length = len(words)
#words[length-1]=words[length-1].replace("&","")
#ords[0]=words[0].replace("&","")
#print words

#####################################################

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
    print "in insert"
    #/api/insert?collection=<collection_name>&values=[attr1:val1,attr2:val2]&primary_keys="key1,key2"&compressed=True|true|false
    regex = re.compile(r'''
    [\S]+:                # a key (any word followed by a colon)
    (?:
    \s                    # then a space in between
        (?!\S+:)\S+       # then a value (any word not followed by a colon)
    )+                    # match multiple values if present
    ''', re.VERBOSE)

    #conn.request("GET","/api/insert?"+"collection="+collName+"&values=["+keyvalue+"]&primary_keys="+primaryKey+"&compressed="+compressed

    matches = regex.findall(query)

    collection= matches[0].replace("collection: ","collection:").split(":")
    collName= collection[1].strip()
    matches.remove(matches[0])
    keyvalue=""
    #print collName
    for match in matches:
        temp=match.split(": ")
        if(not(temp[0].strip()=="primary_key" or temp[0].strip()=="compressed")):
            keyvalue=keyvalue+"\""+temp[0].strip()+"\":\""+temp[1].strip()+"\""
            if(not(matches.index(match)==len(matches)-1)):
                keyvalue+=","

    primaryKey=""
    for match in matches:
        temp=match.split(": ")
        if(temp[0].strip()=="primary_key"):
            primaryKey=primaryKey+"\""+temp[1].strip()+"\""

    compressed="false"
    for match in matches:
        temp=match.split(": ")
        if(temp[0].strip()=="compressed"):
            compressed=temp[1].strip()

    print keyvalue
    print primaryKey
    print compressed

    params = urllib.urlencode({'collection': collName,'values':"["+keyvalue+"]",'primary_keys':'"'+primaryKey+'"','compressed':compressed})
    headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    conn.request("POST","/api/insert", params, headers)

    # print "/api/insert?"+"collection="+collName+"&values=["+keyvalue+"]&primary_keys="+primaryKey+"&compressed="+compressed
    # conn.request("GET","/api/insert?"+"collection="+collName+"&values=["+keyvalue+"]&primary_keys="+primaryKey+"&compressed="+compressed)
    a=conn.getresponse()
    print type(a)
    print json.load(a)
#####################################################################################################################################
if deleteVar:
    print "in delete"
    regex = re.compile(r'''
    [\S]+:                # a key (any word followed by a colon)
    (?:
    \s                    # then a space in between
        (?!\S+:)\S+       # then a value (any word not followed by a colon)
    )+                    # match multiple values if present
    ''', re.VERBOSE)
    matches = regex.findall(query)

    #conn.request("GET","/api/delete?"+"collection="+collName+"&values=["+keyvalue+"]"
    collection= matches[0].replace("collection: ","collection:").split(":")
    collName= collection[1].strip()
    matches.remove(matches[0])

    keyvalue=""
    #print collName
    for match in matches:
        temp=match.split(": ")
        if(not(temp[0].strip()=="primary_key" or temp[0].strip()=="compressed")):
            keyvalue=keyvalue+"\""+temp[0].strip()+"\":\""+temp[1].strip()+"\""
            if(not(matches.index(match)==len(matches)-1)):
                keyvalue+=","

    params = urllib.urlencode({'collection': collName,'values':"["+keyvalue+"]"})
    headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    conn.request("POST","/api/delete", params, headers)
    #print "/api/delete?"+"collection="+collName+"&values=["+keyvalue+"]"
    #conn.request("GET","/api/delete?"+"collection="+collName+"&values=["+keyvalue+"]")
    a=conn.getresponse()
    print type(a)
    print json.load(a)
#####################################################################################################################################
if displayVar:
    regex = re.compile(r'''
    [\S]+:                # a key (any word followed by a colon)
    (?:
    \s                    # then a space in between
        (?!\S+:)\S+       # then a value (any word not followed by a colon)
    )+                    # match multiple values if present
    ''', re.VERBOSE)
    matches = regex.findall(query)

    #conn.request("GET","/api/delete?"+"collection="+collName+"&values=["+keyvalue+"]"
    collection= matches[0].replace("collection: ","collection:").split(":")
    collName= collection[1].strip()
    matches.remove(matches[0])

    keyvalue=""
    #print collName
    for match in matches:
        temp=match.split(": ")
        if(not(temp[0].strip()=="primary_key" or temp[0].strip()=="compressed")):
            keyvalue=keyvalue+"\""+temp[0].strip()+"\":\""+temp[1].strip()+"\""
            if(not(matches.index(match)==len(matches)-1)):
                keyvalue+=","

    params = urllib.urlencode({'collection': collName,'values':"["+keyvalue+"]"})
    headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    conn.request("POST","/api/display", params, headers)

    #conn.request("GET","/api/display?"+"collection="+collName+"&values=["+keyvalue+"]")
    a=conn.getresponse()
    print type(a)
    print json.load(a)
#####################################################################################################################################
if modifyVar:
    print "in Modify"
    mod = query.split("NEWVALUES")
    oldVals = mod[0]
    newVals = mod[1]

    print oldVals
    print "old vals done"
    print newVals
    print "newVals done"
    regex = re.compile(r'''
    [\S]+:                # a key (any word followed by a colon)
    (?:
    \s                    # then a space in between
        (?!\S+:)\S+       # then a value (any word not followed by a colon)
    )+                    # match multiple values if present
    ''', re.VERBOSE)

    matchesOld = regex.findall(oldVals)
    matchesNew = regex.findall(newVals)

    collection= matchesOld[0].replace("collection: ","collection:").split(":")
    collName= collection[1].strip()
    matchesOld.remove(matchesOld[0])

    keyvalueOld=""
    keyvalueNew=""
    #conn.request("GET","/api/modify?"+"collection="+collName+"&conditions=["+keyvalueOld+"]&values=["+keyvalueNew+"]"
    for match in matchesOld:
        temp=match.split(": ")
        if(not(temp[0].strip()=="primary_key" or temp[0].strip()=="compressed")):
            keyvalueOld=keyvalueOld+"\""+temp[0].strip()+"\":\""+temp[1].strip()+"\""
            if(not(matchesOld.index(match)==len(matchesOld)-1)):
                keyvalueOld+=","

    for match in matchesNew:
        temp=match.split(": ")
        if(not(temp[0].strip()=="primary_key" or temp[0].strip()=="compressed")):
            keyvalueNew=keyvalueNew+"\""+temp[0].strip()+"\":\""+temp[1].strip()+"\""
            if(not(matchesNew.index(match)==len(matchesNew)-1)):
                keyvalueNew+=","


    params = urllib.urlencode({'collection': collName,'conditions':"["+keyvalueOld+"]",'values':"["+keyvalueNew+"]"})
    headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    conn.request("POST","/api/modify", params, headers)
    #print "/api/modify?"+"collection="+collName+"&conditions=["+keyvalueOld+"]&values=["+keyvalueNew+"]"
    #conn.request("GET","/api/modify?"+"collection="+collName+"&conditions=["+keyvalueOld+"]&values=["+keyvalueNew+"]")
    a=conn.getresponse()
    print type(a)
    print json.load(a)
#####################################################################################################################################

if descVar:
    print "in Display"
    regex = re.compile(r'''
    [\S]+:                # a key (any word followed by a colon)
    (?:
    \s                    # then a space in between
        (?!\S+:)\S+       # then a value (any word not followed by a colon)
    )+                    # match multiple values if present
    ''', re.VERBOSE)

    matches = regex.findall(query)

    #conn.request("GET","/api/desc?"+"collection="+collName
    collection= matches[0].replace("collection: ","collection:").split(":")
    collName= collection[1].strip()
    matches.remove(matches[0])

    params = urllib.urlencode({'collection': collName})
    headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
    conn.request("POST","/api/desc", params, headers)

    # print "/api/desc?"+"collection="+collName
    # conn.request("GET","/api/desc?"+"collection="+collName)
    a=conn.getresponse()
    print type(a)
    print json.load(a)
#####################################################################################################################################


"""
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

"""