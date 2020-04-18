package guru.com.sparkRDD;

import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;

import scala.collection.Seq;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class avroTestetc {

	public static void main(String[] args) {
		avroTestetc test = new avroTestetc();
		test.loadAvroData();
	}

	public void loadAvroData()
	{
		StructType structureSchema = new StructType()
				.add("name",new StructType()
						.add("firstname",StringType)
						.add("middlename",StringType)
						.add("lastname",StringType))
				.add("id",StringType)
				.add("gender",StringType)
				.add("salary",IntegerType);
		/*   StructField[] structFields = new StructField[]{
            new StructField("intColumn", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("stringColumn", DataTypes.StringType, true, Metadata.empty())
    };

    StructType structType = new StructType(structFields);*/
		//SparkConf conf = new SparkConf().setAppName("sparkRDDSampling").setMaster("local[*]");
		//JavaSparkContext sc = new JavaSparkContext(conf);
		SparkSession session = SparkSession.builder()
				.master("local[*]")
				.appName("sparkJsontoSchema")
				.getOrCreate();

		StructType schemaFromJson =getInvoiceSchema();
		Row[] rowCol = new Row[] {
				RowFactory.create(RowFactory.create("James ","","Smith"),"36636","M",3100),
				RowFactory.create(RowFactory.create("Michael ","Rose",""),"40288","M",4300),
				RowFactory.create(RowFactory.create("Robert ","","Williams"),"42114","M",1400),
				RowFactory.create(RowFactory.create("Maria ","Anne","Jones"),"39192","F",5500),
				RowFactory.create(RowFactory.create("Jen","Mary","Brown"),"","F",-1)};
		List<Row> list = Arrays.asList(rowCol);
		//Seq<Row> structureData = scala.collection.JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
		Dataset<Row> dataFrame = session.createDataFrame(list,schemaFromJson);
		dataFrame.printSchema();

	}

	public StructType getInvoiceSchema()
	{
		//examplePersonJsonSchema.json
		File file = new File(
				getClass().getClassLoader().getResource("data/PosInvoice.json").getFile());

		String jsonStringfromFile = "";
		try {
			JSONParser parser = new JSONParser();
			Object data = parser.parse(new FileReader(file.getPath()));//path to the JSON file.

			InputStream is = new FileInputStream(file.getPath());
			BufferedReader buf = new BufferedReader(new InputStreamReader(is));
			String line = buf.readLine(); 
			StringBuilder sb = new StringBuilder(); 
			while(line != null){
				sb.append(line).append("\n"); 
				line = buf.readLine(); 
			} 
			jsonStringfromFile = sb.toString();
			System.out.println(jsonStringfromFile);
		} catch (Exception e) {
			e.printStackTrace();
		}

		StructType schemaFromJson = (StructType) DataType.fromJson(jsonStringfromFile);
		return schemaFromJson;
	}

	public Schema getInvoiceAvroSchema(String name)
	{
		String filePath = null;
		if(name.startsWith("posinvoice"))
			filePath = "data/PosInvoice.avsc";
		else if(name.startsWith("delivery"))
			filePath = "data/DeliveryAddress.avsc";
		else if(name.startsWith("lineitem"))
			filePath = "data/LineItem.avsc";
		else return null;

		File file = new File(
				getClass().getClassLoader().getResource(filePath).getFile());
		
		Schema parse = null;
		try {
			parse = new Schema.Parser().parse(file);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return parse;
	}
}
