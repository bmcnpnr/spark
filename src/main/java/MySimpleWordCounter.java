import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class MySimpleWordCounter {
    public static void main(String[] args) {
        String appName = args[0];
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(args[1]);
        System.err.println(textFile.first()); //Spark only retrieved the first line of the file(laziness)
        JavaRDD<String> tokanizedFileData = textFile.flatMap(item -> Arrays.asList(item.split(" ")).iterator());
        JavaPairRDD<String, Integer> countPrep = tokanizedFileData.mapToPair(item -> new Tuple2<>(item, 1));
        JavaPairRDD<String, Integer> counts = countPrep.reduceByKey((accumValue, newValue) -> accumValue + newValue);
        JavaPairRDD<String, Integer> sortedCounts = counts.sortByKey();
        sortedCounts.saveAsTextFile("/root/IdeaProjects/spark/target");

    }
}
