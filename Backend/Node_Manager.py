#!/usr/bin/env python
import pika
import lucene
import Handler_callable as handler
import os
import csv

primary_keys_map={}
to_be_compressed_map={}
MAX_RESULTS=1000

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='testdb')




def on_request(ch, method, props, body):
    request_body = body
    request = eval(request_body)

    
    #here call the appropriate function from Handler.py
    #response = fib(n)
    if request[2] == "select":
        collection_name=request[1]
        tofind=request[3]
        primarykeys=primary_keys_map[collection_name]
        to_be_compressed_input=to_be_compressed_map[collection_name]

        SUCCESS_MESSAGE=handler.search(primarykeys,to_be_compressed_input,collection_name,tofind)

        if (not isinstance(SUCCESS_MESSAGE,int)):
            if SUCCESS_MESSAGE is None:
                SUCCESS_MESSAGE="No records Found"
            else:   
                SUCCESS_MESSAGE=repr(SUCCESS_MESSAGE)

                        #later add success/failure codes for denoting what failed
        elif SUCCESS_MESSAGE==100:
            SUCCESS_MESSAGE="JSON format error!Check input!"

        elif SUCCESS_MESSAGE==105:
            SUCCESS_MESSAGE="Invalid collection_name!"    
        
        else:
            SUCCESS_MESSAGE="error in Retrieval!"


    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                     props.correlation_id),
                     body=str(SUCCESS_MESSAGE))
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_request, queue='testdb')



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
