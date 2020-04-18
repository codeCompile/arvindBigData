package guru.com.sparkStreaming;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
//import static org.apache.spark.sql.avro
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;

import guru.learningjournal.kafka.examples.types.PosInvoice;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
//import kafka.utils.VerifiableProperties;
import static org.apache.spark.sql.functions.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
public class JsondatastructuredStreaming {

    public static void main(String[] args) throws StreamingQueryException {
        System.setProperty("hadoop.home.dir", "C:/hadoop");
        org.apache.log4j.Logger.getLogger("org.apache").setLevel(org.apache.log4j.Level.WARN);
        org.apache.log4j.Logger.getLogger("org.apache.spark.storage").setLevel(org.apache.log4j.Level.ERROR);

        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("sparkStructuredStreaming")
                .getOrCreate();

        session.conf().set("spark.sql.shuffle.partitions", "10");

        String bootstrapServers = "192.168.99.100:9092,192.168.99.100:9093,192.168.99.100:9094";
        String kafKaTopic = "pos";
        Dataset<Row> kafkaDataSet = session
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", kafKaTopic)
                .load()
                .selectExpr("CAST(value as String)"); //Will convert to POJO String otherwise it will show as binary
        kafkaDataSet.createOrReplaceTempView("json_invoice_table");
        kafkaDataSet.printSchema();


/*
        //KafkaAvroDecoder decoder = new KafkaAvroDecoder(new VerifiableProperties(decoderProps) );
        UDF1<String, Row> decodeUDF = new UDF1<String, Row>() {
            private static final long serialVersionUID = -2270429248925766705L;
            @Override
            public Row call(final String value) throws Exception {
                System.out.println("kafKaTopic,value" + kafKaTopic + "===" + value);

                return RowFactory.create(pos.PosID,pos.CashierID);
            }
        };
*/


//        session.sqlContext().udf().register("deserializeAvro", decodeUDF,
//                DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("PosID", DataTypes.StringType, true),
//                        DataTypes.createStructField("CashierID", DataTypes.StringType, true))));

//             Dataset<Row> results1 = kafkaDataSet.select(callUDF("deserializeAvro", col("value")));
        Dataset<Row> results1 = session.sql("select * from json_invoice_table");
        results1.printSchema();

/*
        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client);
        Dataset<String> results2 = kafkaDataSet.map(m-> {
            GenericData.Record data = (GenericData.Record)deserializer.deserialize(kafKaTopic,(byte[]) m.get(1));
            return "sddasdadasdsadas";
        }, Encoders.STRING());
*/

        StreamingQuery query = results1.writeStream()
                .format("console")
                .outputMode(OutputMode.Update())
                .option("truncate",false)
                .option("numRows",50)
                .start();

        query.awaitTermination();
    }
}
