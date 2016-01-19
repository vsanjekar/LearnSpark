package WordCount;

/**
 * Created by vsanjekar on 1/13/16.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCount {

    public static void main(final String[] args) {
        System.out.println("Hellow World. This is Apache Spark WordCount example");

        final SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("SparkWordCountJava");
        sparkConf.setMaster("local[2]"); // Two thread

        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        final List<String> data =Arrays.asList("This is some text to use for word count example", "My name is Vinay", "This is spark example");
        final JavaRDD<String> javaRDDLines = javaSparkContext.parallelize(data);
        final JavaRDD<String> javaRDDWords = javaRDDLines.flatMap(line -> Arrays.asList(line.split(" ")));
        final JavaPairRDD<String, Integer> pairRDD = javaRDDWords.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
        final JavaPairRDD<String, Integer> wordCounts = pairRDD.reduceByKey((a, b) -> a+b);

        wordCounts.collect().forEach(System.out::println);
        javaSparkContext.stop();
    }
}
