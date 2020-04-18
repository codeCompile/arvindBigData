package guru.com.sparkStreaming;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import guru.learningjournal.kafka.examples.types.PosInvoice;
////////////https://stackoverflow.com/questions/26435299/write-pojos-to-parquet-file-using-reflection
//https://stackoverflow.com/questions/26435299/write-pojos-to-parquet-file-using-reflection
///Creates a Generic record from existing object
public class AvroGenericRecord<T> {

	Class type;
	private  Schema avroSchema;
	private  ReflectDatumWriter<T> reflectDatumWriter;
	private  GenericDatumReader<Object> genericRecordReader;
	public AvroGenericRecord(Class tt)
	{
		type= tt;
		avroSchema = ReflectData.AllowNull.get().getSchema(type);
		reflectDatumWriter = new ReflectDatumWriter<>(avroSchema);
		genericRecordReader = new GenericDatumReader<>(avroSchema);
	}

	public GenericRecord toAvroGenericRecord(T pos) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		reflectDatumWriter.write(pos, EncoderFactory.get().directBinaryEncoder(bytes, null));
		return (GenericRecord) genericRecordReader.read(null, DecoderFactory.get().binaryDecoder(bytes.toByteArray(), null));
	}
}
