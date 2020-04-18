package guru.com;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.avg;

public class fileStockDataReadingStreaming {

    public static void main(String[] args) throws StreamingQueryException {
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

        StructType userSchema = new StructType().add("date","String").add("value","float");
        Dataset<Row> stockData = session.readStream().option("sep",",")
                .schema(userSchema)
                .csv("C:\\arvGitRepo\\arvindBigData\\datasets\\STOCKMARKET_csv_2\\data");


        Dataset<Row> resultDf = stockData.groupBy("date").agg(avg(stockData.col("value")));

        StreamingQuery query = resultDf.writeStream().outputMode("complete").format("console")
                .option("truncate",false)
                .option("numRows",50000)
                .start();

        query.awaitTermination();


    }
}
