from confluent_kafka import Producer
import socket
import sys

if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <bootstrap-brokers>\n' % sys.argv[0])
        sys.exit(1)

    broker = 'localhost:9092'
    topic_phone = 'phone_loc'
    topic_trans = 'trans_loc'

    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

    #Phone location producer configuration
    phone_conf = {'bootstrap.servers': broker,
                  'client.id': socket.gethostname()}


    phone_prod = Producer(**phone_conf)

    #Transaction location producer configuration
    trans_conf = {'bootstrap.servers': broker,
                  'client.id': socket.gethostname()}                  

    trans_prod = Producer(**trans_conf)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            sys.stderr.write('%% **Message delivery failed**\n **error: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s partion:[%d], offset: %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    # Read lines from stdin, produce each line to Kafka
    for line in sys.stdin:
        try:
            # Produce line (without newline)
            phone_prod.produce(topic_phone, line.rstrip(), callback=delivery_callback)

        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(phone_prod))

        phone_prod.poll(0)

        try:
            # Produce line (without newline)
            trans_prod.produce(topic_trans, line.rstrip(), callback=delivery_callback)

        except BufferError as e:
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(trans_prod))

        # Serve delivery callback queue.
        # NOTE: Since produce() is an asynchronous API this poll() call
        #       will most likely not serve the delivery callback for the
        #       last produce()d message.
        trans_prod.poll(0)

    # Wait until all messages have been delivered
    sys.stderr.write('%% Waiting for %d deliveries from %s\n' % (len(phone_prod), topic_phone))    
    phone_prod.flush()
    
    sys.stderr.write('%% Waiting for %d deliveries from %s\n' % (len(trans_prod), topic_trans))
    trans_prod.flush()