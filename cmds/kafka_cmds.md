create topic
./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor rep_NUM --partitions part_NUM -topic topic_NAME

producer from the console to a topic
kafka-console-producer.sh --broker-list localhost:9092 --topic test

consumer to the console from a topic
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test

list the active topics./bin/kafka-topics.sh --list --zookeeper localhost:2181


describe the topic./bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic test



Spark
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.1,anguenot:pyspark-cassandra:0.9.0 K2S_add.py