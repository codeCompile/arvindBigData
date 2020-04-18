package guru.com.sparkStreaming;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;

import guru.com.sparkRDD.avroTestetc;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
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
public class sparkStructuredStreaming {

	private static Schema.Field field;

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "C:/hadoop");
		org.apache.log4j.Logger.getLogger("org.apache").setLevel(org.apache.log4j.Level.WARN);
		org.apache.log4j.Logger.getLogger("org.apache.spark.storage").setLevel(org.apache.log4j.Level.ERROR);

		//SparkConf conf = new SparkConf().setAppName("sparkRDDSampling").setMaster("local[*]");
		//SparkContext spContext  = new SparkContext(conf);

		SparkSession session = SparkSession.builder()
				.master("local[*]")
				.appName("sparkStructuredStreaming")
				.getOrCreate();

		session.conf().set("spark.sql.shuffle.partitions", "10");

		String bootstrapServers = "192.168.99.100:9092,192.168.99.100:9093,192.168.99.100:9094";
		String kafKaTopic = "pos";
		Dataset<Row> kafkaDataSet = session.readStream().format("kafka")
				.option("kafka.bootstrap.servers", bootstrapServers)
				.option("subscribe", kafKaTopic)
				.load()
				.selectExpr("CAST(value as String)");


		//Dataset<PosInvoice> results2 = kafkaDataSet.select(col("value")).as(Encoders.bean(PosInvoice.class));

		//.selectExpr("CAST(value as String)");
		kafkaDataSet.createOrReplaceTempView("invoice_table");
		kafkaDataSet.printSchema();

		//add settings for schema registry url, used to get deser
		String schemaRegUrl = "http://192.168.99.100:8081";
		CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(schemaRegUrl, 100);
		//KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client);
		SchemaMetadata latestSchemaMetadata = client.getLatestSchemaMetadata("pos-value");
		int schemaId = latestSchemaMetadata.getId();
		String schemaString = latestSchemaMetadata.getSchema();

		Schema avroSchema = Schema.parse(schemaString);

		//AvroSchemaConverter
		//		KafkaAvroDecoder decoder = new KafkaAvroDecoder(client);

		UDF1<byte[], Object> decodeUDF = new UDF1<byte[], Object>() {
			private static final long serialVersionUID = -2270429444444445L;
			@Override
			public Object call(final byte[] value) throws Exception {
				System.out.println("kafKaTopic,value" + kafKaTopic + "===" + value);

				//KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(client);
				//Object pos =  deserializer.deserialize(kafKaTopic,value);
				KafkaAvroDecoder decoder = new KafkaAvroDecoder(client);

				Object fromBytes = decoder.fromBytes(value, avroSchema);

				return fromBytes;//RowFactory.create(pos.PosID,pos.CashierID);
			}
		};
		avroTestetc  helper = new avroTestetc();
		Schema deliverySchema = helper.getInvoiceAvroSchema("delivery");
		Schema lineItemSchema = helper.getInvoiceAvroSchema("lineitem");
		Schema invoiceSchema = helper.getInvoiceAvroSchema("posinvoice");
		StructType stt = new StructType();
		//invoiceSchema.getFields().forEach(t-> stt.add(t.name(),DataTypes.StringType));
		//session.sqlContext().udf().register("deserializeAvro", decodeUDF,stt);

		//DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("PosID", DataTypes.StringType, true),
		//DataTypes.createStructField("CashierID", DataTypes.StringType, true))));


		//Dataset<Row> results1 = kafkaDataSet.select(callUDF("deserializeAvro", col("value")));
		//results1 = kafkaDataSet.select(col("value") );

		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(session.sparkContext());
		//Broadcast<KafkaAvroDecoder> broadcast = jsc.broadcast(new KafkaAvroDecoder(client));
		//Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(avroSchema);
		//Broadcast<Injection<GenericRecord, byte[]>> broadcast = jsc.broadcast(recordInjection);


		Dataset<String> results2 = kafkaDataSet.map(m-> {
			//GenericData.Record data = (GenericData.Record)deserializer.deserialize(kafKaTopic,(byte[]) m.get(1));
			System.out.println("Arvind........................");
			//byte[] rawValue = (byte[]) m.get(0);

			try
			{
				byte[] rawValue = String.valueOf(m).getBytes();
				//KafkaAvroDecoder decoder = broadcast.getValue();
				//GenericData.Record data = (GenericData.Record)decoder.fromBytes(rawValue, avroSchema);
				//return String.valueOf(data);

				Injection<GenericRecord, byte[]> recordInjection = GenericAvroCodecs.toBinary(avroSchema);
				GenericRecord record = null;
				record = recordInjection.invert(rawValue).get();

				return String.valueOf(record);

			}
			catch (Exception e) {
				System.out.println("Application error:" + e.getMessage());

			}
			//deserializer.deserialize(kafKaTopic,(byte[]) m.get(1));
			return  "sssssssssssssssssssss";
		}, Encoders.STRING());

		//Dataset<Row> selectExpr = kafkaDataSet.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");


		//		Dataset<Row> results = session.sql("select deserializeAvro(value) AS message, value from invoice_table");
		//		Dataset<PosInvoice> resultsmap = kafkaDataSet.map( t ->  {
		//			return (PosInvoice) deserializer.deserialize(kafKaTopic, ((byte[])t.get(1)) },
		//				Encoders.bean(PosInvoice.class));

		//		resultsmap.printSchema();
		StreamingQuery query = results2.writeStream()
				.format("console")
				.outputMode(OutputMode.Update())
				.option("truncate",false)
				.option("numRows",50)
				.start();

		query.awaitTermination();
	}
}
