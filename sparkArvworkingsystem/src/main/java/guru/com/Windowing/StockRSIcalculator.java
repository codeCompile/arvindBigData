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
import guru.com.Windowing.types.RelativeTuple;
import scala.Tuple2;

/*
 * The Relative Strength Index is a momentum indicator that measures the magnitude of recent price changes to analyze overbought or oversold conditions. 
 * It is primarily used to attempt to identify overbought or oversold conditions in the trading of an asset.

Problem To Solve : Calculate the Relative Strength Index or RSI of the four stocks in a 5-minute sliding window for the last 10 minutes. RSI is considered overbought when above 70 and oversold when below 30.
 */
public class StockRSIcalculator {

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
		javaDStream.print();
		JavaPairDStream<String, RelativeTuple> pairDStream = javaDStream.flatMapToPair(new PairFlatMapFunction<String, String, RelativeTuple>() {

			private static final long serialVersionUID = 8945380065501472918L;

			@Override
			public Iterator<Tuple2<String, RelativeTuple>> call(String t) throws Exception {

				List<Tuple2<String, RelativeTuple>> list = new ArrayList<Tuple2<String, RelativeTuple>>();
				JSONArray js1 = new JSONArray(t);
				for (int i = 0; i < js1.length(); i++) {
					String symbol = js1.getJSONObject(i).get("symbol").toString();
					JSONObject pricedata = new JSONObject(js1.getJSONObject(i).get("priceData").toString());

					Double closingPrice=pricedata.getDouble("close");
					Double openingPrice=pricedata.getDouble("open");
					RelativeTuple relativeTuple=null;
					if(closingPrice>openingPrice)
					{
						relativeTuple=new RelativeTuple(1, 0, closingPrice, 0);
					}
					else
					{
						relativeTuple=new RelativeTuple(0, 1, openingPrice, 0);
					}


					list.add(new Tuple2<String, RelativeTuple>(symbol,relativeTuple));

				}
				return list.iterator();
			}
		});

		JavaPairDStream<String, RelativeTuple> reduceByKeyAndWindow = pairDStream.reduceByKeyAndWindow(new Function2<RelativeTuple, RelativeTuple, RelativeTuple>() {

			private static final long serialVersionUID = 1L;

			@Override
			public RelativeTuple call(RelativeTuple result, RelativeTuple value) throws Exception {
				result.setUpward(result.getUpward() + value.getUpward());
				result.setUpwardCount(result.getUpwardCount() + value.getUpwardCount());
				result.setDownward(result.getDownward() + value.getDownward());
				result.setDownwardCount(result.getDownwardCount() + value.getDownwardCount());
				return result;
			}
		}, new Duration(20000), new Duration(10000));

		//ArrayList<String> list = new ArrayList<String>();
		reduceByKeyAndWindow.foreachRDD(new VoidFunction<JavaPairRDD<String,RelativeTuple>>() {
			private static final long serialVersionUID = -3495790616441207699L;

			@Override
			public void call(JavaPairRDD<String, RelativeTuple> t) throws Exception {

				JavaPairRDD<String, RelativeTuple> output = t.coalesce(1);
				t.foreach(new VoidFunction<Tuple2<String,RelativeTuple>>() {

					@Override
					public void call(Tuple2<String, RelativeTuple> t) throws Exception {
						//list.add(t._2.toString());
						System.out.println(t._1.toString() + t._2.toString());						
					}
				});
				//output.saveAsTextFile(resultOutputPath+ java.io.File.separator + System.currentTimeMillis());
			}
		});
		//System.out.println(list);
		reduceByKeyAndWindow.print();
		jStreamingContext.start();
		jStreamingContext.awaitTermination();
	}
}
