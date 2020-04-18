package guru.learningjournal.kafka.examples;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaAvroJavaConsumerV2Demo {

    public static void main(String[] args) {
        Properties properties = new Properties();
        // normal consumer
        properties.put("auto.commit.enable", "false");
        properties.put("auto.offset.reset", "earliest");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleAvroConsumer");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //Use Kafka Avro Deserializer.
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,KafkaAvroDeserializer.class.getName());  //<----------------------
        //Use Specific Record or else you get Avro GenericRecord.
        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        //Schema registry location.
        properties.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
                AppConfigs.schemaRegistryServers); //<----- Run Schema Registry on 8081


        KafkaConsumer<String, PosInvoice> kafkaConsumer = new KafkaConsumer<>(properties);
        String topic = AppConfigs.posTopicName;
        kafkaConsumer.subscribe(Collections.singleton(topic));

        System.out.println("Waiting for data...");

        while (true){
            System.out.println("Polling");
            ConsumerRecords<String, PosInvoice> records = kafkaConsumer.poll(1000);

            for (ConsumerRecord<String, PosInvoice> record : records){
                PosInvoice customer = record.value();
                System.out.println("Received: " + customer);
            }

            kafkaConsumer.commitSync();
        }
    }
}
