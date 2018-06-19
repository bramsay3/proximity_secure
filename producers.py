from confluent_kafka import Producer
import socket
import sys

class Production

    def __init__(self, topic_name, port=9092):
        broker = 'localhost:' + port
        self.topic = topic_name

        # Producer configuration
        # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

        #Phone location producer configuration
        config = {'bootstrap.servers': broker,
                      'client.id': socket.gethostname()}

        self.prod = Producer(**config)

    def start_producing(self, data_feed, continuous=False):
        if continuous:
            while true:
                for i in range(len(data_feed)):
                    self.prod.produce(self.topic,data_feed)
        else:
            for i in range(len(data_feed)):
                    self.prod.produce(self.topic,data_feed)
            
