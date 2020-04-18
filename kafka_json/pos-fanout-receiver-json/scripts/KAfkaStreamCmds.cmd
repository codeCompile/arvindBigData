kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic hadoop-sink  --config min.insync.replicas=2
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic loyalty  --config min.insync.replicas=2
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 3 --partitions 3 --topic pos --config min.insync.replicas=2kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic shipment --config min.insync.replicas=2kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic hadoop-sink --from-beginning
kafka-server-start.bat %KAFKA_HOME%\config\server-0.properties
kafka-server-start.bat %KAFKA_HOME%\config\server-1.properties
kafka-server-start.bat %KAFKA_HOME%\config\server-2.properties
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic loyalty --from-beginning
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic shipment --from-beginning
zookeeper-server-start.bat %KAFKA_HOME%\config\zookeeper.properties