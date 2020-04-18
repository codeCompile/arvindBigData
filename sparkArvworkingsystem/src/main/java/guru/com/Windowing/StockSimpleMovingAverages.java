package guru.com.Windowing;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.aggregate.Average;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONArray;
import org.json.JSONObject;

import guru.com.Utils.HelperUtilities;
import guru.com.Windowing.types.AverageTuple;
import scala.Tuple2;
/*
 * Problem To Solve : Calculate the simple moving average closing price of stocks
 *  in a 5-minute sliding window for the last 10 minutes.
 */
public class StockSimpleMovingAverages {

	public static void main(String[] args) throws Exception {

		System.setProperty("hadoop.home.dir", "C:/hadoop");
		org.apache.log4j.Logger.getLogger("org.apache").setLevel(org.apache.log4j.Level.WARN);
		org.apache.log4j.Logger.getLogger("org.apache.spark.storage").setLevel(org.apache.log4j.Level.ERROR);

		SparkSession session = SparkSession.builder()
				.master("local[*]")
				.appName("sparkStructuredStreaming")
				.getOrCreate();

		//session.conf().set("spark.sql.shuffle.partitions", "10");
		JavaSparkContext jsc = new JavaSparkContext(session.sparkContext());

		JavaStreamingContext jStreamingContext = new JavaStreamingContext(jsc,new Duration(10000));

		String resourceFilePath = HelperUtilities.Instance.getResourceFilePath("readme.txt");
		String resultOutputPathtmp = HelperUtilities.Instance.getResourceFilePath("readme.txt");//results/out.txt");
		resourceFilePath = resourceFilePath.substring(0,resourceFilePath.lastIndexOf("\\")) + java.io.File.separator + "stream_inputs";
		resultOutputPathtmp = resultOutputPathtmp.substring(0,resultOutputPathtmp.lastIndexOf("\\")) + java.io.File.separator + "results";
		final String resultOutputPath = resultOutputPathtmp;

		JavaDStream<String> javaDStream = jStreamingContext.textFileStream(resourceFilePath);

		JavaPairDStream<String, AverageTuple> pairDStream = javaDStream.flatMapToPair(new PairFlatMapFunction<String, String, AverageTuple>() {

			private static final long serialVersionUID = 8945380065501472918L;

			@Override
			public Iterator<Tuple2<String, AverageTuple>> call(String t) throws Exception {

				List<Tuple2<String, AverageTuple>> list = new ArrayList<Tuple2<String, AverageTuple>>();
				JSONArray js1 = new JSONArray(t);
				for (int i = 0; i < js1.length(); i++) {
					String symbol = js1.getJSONObject(i).get("symbol").toString();
					JSONObject pricedata = new JSONObject(js1.getJSONObject(i).get("priceData").toString());

					list.add(new Tuple2<String, AverageTuple>(symbol,new AverageTuple(1,pricedata.getDouble("close"))));

				}
				return list.iterator();
			}
		});

		JavaPairDStream<String, AverageTuple> reduceByKeyAndWindow = pairDStream.reduceByKeyAndWindow(new Function2<AverageTuple, AverageTuple, AverageTuple>() {

			private static final long serialVersionUID = 1L;

			@Override
			public AverageTuple call(AverageTuple result, AverageTuple value) throws Exception {
				result.setAverage(result.getAverage() + value.getAverage());
				result.setCount(result.getCount() + value.getCount());
				return result;
			}
		}, new Duration(20000), new Duration(10000));


		reduceByKeyAndWindow.foreachRDD(new VoidFunction<JavaPairRDD<String,AverageTuple>>() {
			private static final long serialVersionUID = -3495790616441207699L;

			@Override
			public void call(JavaPairRDD<String, AverageTuple> t) throws Exception {

				JavaPairRDD<String, AverageTuple> output = t.coalesce(1);
				//output.saveAsTextFile(resultOutputPath+ java.io.File.separator + System.currentTimeMillis());
			}
		});
		reduceByKeyAndWindow.print();
		jStreamingContext.start();
		jStreamingContext.awaitTermination();
	}
}
