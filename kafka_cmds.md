create topic
./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor rep_NUM --partitions part_NUM -topic topic_NAME

producer from the console to a topic
kafka-console-producer.sh --broker-list localhost:9092 --topic test

consumer to the console from a topic
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test

list the active topics./bin/kafka-topics.sh --list --zookeeper localhost:2181


describe the topic./bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic test
