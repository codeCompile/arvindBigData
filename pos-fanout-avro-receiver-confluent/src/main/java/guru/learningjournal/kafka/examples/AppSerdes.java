package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.types.HadoopRecord;
import guru.learningjournal.kafka.examples.types.Notification;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

public class AppSerdes extends Serdes {


    public static Serde<PosInvoice> PosInvoice() {
        Serde<PosInvoice> serde = new SpecificAvroSerde<>();

        Map<String, String> serdeConfigs = new HashMap<>();
        serdeConfigs.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfigs.schemaRegistryServers);
        //serdeConfigs.put("schema.registry.url", "http://192.168.99.100:8081");
        serde.configure(serdeConfigs, false);

        return serde;
    }

    public static Serde<Notification> Notification() {
        Serde<Notification> serde = new SpecificAvroSerde<>();

        Map<String, String> serdeConfigs = new HashMap<>();
        serdeConfigs.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfigs.schemaRegistryServers);

        //serdeConfigs.put("schema.registry.url", "http://192.168.99.100:8081");
        serde.configure(serdeConfigs, false);

        return serde;
    }

    public static Serde<HadoopRecord> HadoopRecord() {
        Serde<HadoopRecord> serde = new SpecificAvroSerde<>();

        Map<String, String> serdeConfigs = new HashMap<>();
        serdeConfigs.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, AppConfigs.schemaRegistryServers);
        //serdeConfigs.put("schema.registry.url", "http://192.168.99.100:8081");
        serde.configure(serdeConfigs, false);

        return serde;
    }

   
}
