kafka-topics.bat --create --zookeeper 192.168.99.100:2181 --replication-factor 3 --partitions 2 --topic loyalty  --config min.insync.replicas=2
kafka-topics.bat --create --zookeeper 192.168.99.100:2181 --replication-factor 3 --partitions 3 --topic pos --config min.insync.replicas=2
kafka-topics.bat --create --zookeeper 192.168.99.100:2181 --replication-factor 3 --partitions 2 --topic shipment --config min.insync.replicas=2
kafka-topics.bat --create --zookeeper 192.168.99.100:2181 --replication-factor 3 --partitions 2 --topic hadoop-sink  --config min.insync.replicas=2


kafka-server-start.bat %KAFKA_HOME%\config\server-0.properties
kafka-server-start.bat %KAFKA_HOME%\config\server-1.properties
kafka-server-start.bat %KAFKA_HOME%\config\server-2.properties
zookeeper-server-start.bat %KAFKA_HOME%\config\zookeeper.properties

kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic hadoop-sink --from-beginning
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic loyalty --from-beginning
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic shipment --from-beginning

kafka-console-consumer.bat --bootstrap-server 192.168.99.100:9092 --topic pos --from-beginning
