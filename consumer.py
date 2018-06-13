from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import time


if __name__ == '__main__':

    broker = 'localhost:9092'
    topic_phone = 'phone_loc'
    topic_trans = 'trans_loc'
    group_id = 'general'

    conf = {'bootstrap.servers': broker,
            'group.id': group_id}

    read = Consumer(**conf)

    read.subscribe([topic_phone])

    running = True
    msg = read.poll()
    while running:

        time.sleep(1)
        print('sec')
        
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                sys.stderr.write('%% %s reached EOF - partition %d, offset %d' % 
                                (msg.topic(), msg.partition(), msg.offset()))
            else:
                raise KafkaException(msg.error())
        else:
            sys.stderr.write('%% %s - partition %d, offset %d\n key: %s  value: %s\n' %
                             (msg.topic(), msg.partition(), msg.offset(),
                              msg.key(),msg.value().decode('utf-8')))

    except KeyboardInterrupt:
        sys.stderr.write('%% User terminated')

        msg = read.poll()
