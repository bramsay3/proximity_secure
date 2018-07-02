from confluent_kafka import Producer
import socket
import sys
import time

class Production:

    def __init__(self, topic_name, port=9092):
        broker = 'localhost:' + str(port)
        self.topic = topic_name

        # Producer configuration
        # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

        #Phone location producer configuration
        config = {'bootstrap.servers': broker,
                   'client.id': socket.gethostname(),
                   'queue.buffering.max.messages': 1000000}

        self.prod = Producer(**config)

    def start_producing(self, data_feed, continuous=False):
        if continuous:
            while True:
                for i in range(len(data_feed)):
                    self.prod.produce(self.topic,str(data_feed[i]).replace("'",'"'))

                time.sleep(3)
                print('recycling data')
        else:
            for i in range(len(data_feed)):
                self.prod.produce(self.topic,str(data_feed[i]).replace("'",'"'))
            self.prod.poll(0)
            
