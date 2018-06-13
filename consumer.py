from confluent_kafka import Consumer, KafkaError, KafkaException
import sys


if __name__ == '__main__':

    broker = 'localhost:9092'
    topic_phone = 'phone_loc'
    topic_trans = 'trans_loc'
    group_id = 'general'

    conf = {'bootstrap.servers': broker,
            'group.id': group_id}

    read = Consumer(**conf)

    read.subscribe(topic_phone)

    runing = True

    try:
        while runing:
            msg = read.poll() 
            if msg is None:
                continue
                if msg.error().code() == KafkaError._Partition_EOF:
                    sys.stderr.write('%% %s reached EOF - partition %d, offset %d' % 
                                    (msg.topic(), msg.partition(), msg.offset()))
                else:
                    raise KafkaException(msg.error())
            else:
                sys.stderr.write('%% %s - partition %d, offset %d key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                print(msg.value())

    except KeyboardInterrupt:
        sys.stderr.write('%% User terminated')




