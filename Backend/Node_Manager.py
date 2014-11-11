#!/usr/bin/env python
import pika
import lucene
import Handler_callable as handler
import os
import csv
import atexit
import json


primary_keys_map={}
to_be_compressed_map={}
MAX_RESULTS=1000

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='productiondb')


def exit_handler():
    if len(primary_keys_map) > 0:
        f=open("collectionmetafile.csv","wb")
        w = csv.writer(f)
        for key, val in primary_keys_map.items():
            w.writerow([key, val,to_be_compressed_map[key]])
        f.close()
                        

def on_request(ch, method, props, body):
    request_body = body
    request = eval(request_body)

    
    #here call the appropriate function from Handler.py
    #response = fib(n)
    if request[2] == "select":
        collection_name=request[1]
        tofind=request[3]
        if collection_name in primary_keys_map.keys():
            primarykeys=primary_keys_map[collection_name]
            to_be_compressed_input=to_be_compressed_map[collection_name]

            SUCCESS_MESSAGE=handler.search(primarykeys,to_be_compressed_input,collection_name,tofind)

            if (not isinstance(SUCCESS_MESSAGE,int)):
                if SUCCESS_MESSAGE is None:
                    SUCCESS_MESSAGE=repr([json.dumps({"error":"No records Found"})])
                else:   
                    SUCCESS_MESSAGE=repr(SUCCESS_MESSAGE)

                            
            elif SUCCESS_MESSAGE==100:
                SUCCESS_MESSAGE=repr([json.dumps({"error":"JSON format error!Check input!"})])

            elif SUCCESS_MESSAGE==105:
            	SUCCESS_MESSAGE=repr([json.dumps({"error":"Invalid collection_name!"})])
                   
            else:
                SUCCESS_MESSAGE=repr([json.dumps({"error":"error in Retrieval!"})])
        else:
            SUCCESS_MESSAGE = repr([json.dumps({"error":"Invalid collection_name"})])

    elif request[2] == "delete":
        collection_name=request[1]
        todelete=request[3]
        primarykeys=primary_keys_map[collection_name]

        SUCCESS_MESSAGE = handler.delete(primarykeys,collection_name,todelete)

        if SUCCESS_MESSAGE==105:
            SUCCESS_MESSAGE = repr([json.dumps({"error":"Invalid collection_name"})])
        
        elif SUCCESS_MESSAGE==100:
            SUCCESS_MESSAGE = repr([json.dumps({"error":"JSON format error!Check input!"})])
            
        elif SUCCESS_MESSAGE==000:
            SUCCESS_MESSAGE = repr([json.dumps({"error":"deleted appropriate records!"})])

    elif request[2] == "store":
        collection_name = request[1]
        data = request[3]
        SUCCESS_MESSAGE=""
        flag=True
        
        if collection_name not in primary_keys_map.keys():
            if len(request)==6:
                primary_keys=request[4].split(',')
                primary_keys_map[collection_name]=primary_keys
                if request[5] in ["True","true"]:
                    to_be_compressed_map[collection_name]=True
                else:
                    to_be_compressed_map[collection_name]=False
                flag=True
                #write to metafile the addition of a new collection
                f=open("collectionmetafile.csv","wb")
                w = csv.writer(f)
                for key, val in primary_keys_map.items():
                    w.writerow([key, val,to_be_compressed_map[key]])
                f.close()    
            else:
                SUCCESS_MESSAGE = repr([json.dumps({"error":"Invalid collection name,enter parameters(primary_keys and to_be_compressed) along with data first time to create collection"})])
                flag=False
        if flag==True:
            primarykeys=primary_keys_map[collection_name]
            to_be_compressed_input=to_be_compressed_map[collection_name]
            SUCCESS_MESSAGE = handler.store(primarykeys,to_be_compressed_input,collection_name,data)

            if SUCCESS_MESSAGE==000:
                SUCCESS_MESSAGE = repr([json.dumps({"SUCCESS":"added to database successfully!"})])
                                
                            #later add success/failure codes for denoting what failed
            elif SUCCESS_MESSAGE==100:
                SUCCESS_MESSAGE = repr([json.dumps({"error":"JSON format error!Check input!"})])
                                
            elif SUCCESS_MESSAGE==101:
                SUCCESS_MESSAGE = repr([json.dumps({"error":"Make sure you gave correct primary_keys!"})])
                                
            elif SUCCESS_MESSAGE==102:
                SUCCESS_MESSAGE = repr([json.dumps({"error":"Lucene Storage error!"})])
                                
            elif SUCCESS_MESSAGE==106:
                SUCCESS_MESSAGE = repr([json.dumps({"error":"Record with same primary keys already exists!"})])
                                
            else:
                SUCCESS_MESSAGE = repr([json.dumps({"error":"error in insertion!"})])

    elif request[2] == "number":
        collection_name = request[1]
        SUCCESS_MESSAGE = handler.number(collection_name)
        if SUCCESS_MESSAGE == 105:
            SUCCESS_MESSAGE = repr([json.dumps({"error":"Invalid collection_name!"})])

    elif request[2] == "update":
        SUCCESS_MESSAGE=""
        if len(request)==5:
            collection_name= request[1]
            tofind = request[3]
            toupdate = request[4]
            if collection_name in primary_keys_map.keys():
                primarykeys=primary_keys_map[collection_name]
                to_be_compressed_input=to_be_compressed_map[collection_name]

                SUCCESS_MESSAGE = handler.update(primarykeys,to_be_compressed_input,collection_name,tofind,toupdate)

                if SUCCESS_MESSAGE==100:
                    SUCCESS_MESSAGE = repr([json.dumps({"error":"JSON format error!Check input!"})])
                elif SUCCESS_MESSAGE == 105:
                    SUCCESS_MESSAGE = repr([json.dumps({"error":"Invalid collection_name!"})])
                elif SUCCESS_MESSAGE==106:
                    SUCCESS_MESSAGE = repr([json.dumps({"error":"Record with same primary keys already exists!"})])
                else:
                    SUCCESS_MESSAGE = repr([json.dumps({"SUCCESS":SUCCESS_MESSAGE})])
            else:
                SUCCESS_MESSAGE = repr([json.dumps({"error":"Invalid collection_name!"})])

        else:
            SUCCESS_MESSAGE = repr([json.dumps({"error":"Send all parameters to update(primary_keys_map,to_be_compressed_input,collection_name,tofind,update)"})])

    elif request[2] == "desc":
        collection_name = request[1]
        

        if collection_name not in primary_keys_map.keys():
            SUCCESS_MESSAGE = repr([json.dumps({"error":"Invalid collection_name!"})])
        else:
            primarykeys=primary_keys_map[collection_name]
            to_be_compressed_input=to_be_compressed_map[collection_name]
            SUCCESS_MESSAGE = handler.desc(primarykeys,to_be_compressed_input,collection_name)
            SUCCESS_MESSAGE = repr([json.dumps({"SUCCESS":SUCCESS_MESSAGE})])    

    elif request[2] == "drop":
        collection_name = request[1]

        if collection_name not in primary_keys_map.keys():
            SUCCESS_MESSAGE = repr([json.dumps({"error":"Invalid collection_name!"})])
        else:
            primary_keys_map.pop(collection_name)
            to_be_compressed_map.pop(collection_name)

            f=open("collectionmetafile.csv","wb")
            w = csv.writer(f)
            
            for key, val in primary_keys_map.items():
                w.writerow([key, val,to_be_compressed_map[key]])
            
            f.close()
            
            SUCCESS_MESSAGE = handler.drop(collection_name)
            SUCCESS_MESSAGE = repr([json.dumps({"SUCCESS":SUCCESS_MESSAGE})])
        

                            




    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                     props.correlation_id),
                     body=str(SUCCESS_MESSAGE))
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_request, queue='productiondb')



if __name__ == "__main__":
    #init lucene vm
    lucene.initVM()
        
    #####load required resources from metafile ##################
    print "Initialized lucene with version number :",lucene.VERSION
    if os.path.exists("collectionmetafile.csv"):    
        
        f=open("collectionmetafile.csv",'rb')
        for key, val, compressed in csv.reader(f):
            primary_keys_map[key]=eval(val)
            to_be_compressed_map[key]=eval(compressed)

        f.close()
        

    channel.start_consuming()


