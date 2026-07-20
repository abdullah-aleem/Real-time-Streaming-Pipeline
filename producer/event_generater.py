from kafka import KafkaProducer
import json
import time
import random

class EventGenerater:
    def __init__(self,bootstrap_servers,topic):
        #the value_serializer is used to serialize the value to a byte string as kafka only accepts byte strings
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.topic = topic
    
    def send_event(self,event):
        self.producer.send(self.topic,event)
        #flush is used to ensure that the event is sent to the broker and not briefly stored in memory 
        self.producer.flush()
    
    def close(self):
        self.producer.close()