import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class MySimpleWordCounter {
    public static void main(String[] args) {
        String appName = "Word Counter";
        SparkConf conf = new SparkConf()
//                .setMaster("local[2]")
                .setAppName(appName)
                .setJars(new String[]{"/root/IdeaProjects/spark/target/spark-project-1.0.jar"});

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> textFile = sc.textFile(args[0]);
        System.err.println(textFile.first()); //Spark only retrieved the first line of the file(laziness)
        JavaRDD<String> tokenizedFileData = textFile.flatMap(item -> Arrays.asList(item.split(" ")).iterator());
        JavaPairRDD<String, Integer> countPrep = tokenizedFileData.mapToPair(item -> new Tuple2<>(item, 1));
        JavaPairRDD<String, Integer> counts = countPrep.reduceByKey((accumValue, newValue) -> accumValue + newValue);
        JavaPairRDD<String, Integer> sortedCounts = counts.sortByKey();
        sortedCounts.saveAsTextFile("/root/IdeaProjects/spark/target/output");


        //second method: countByValue
        Map<String, Long> stringLongMap = tokenizedFileData.countByValue();
    }
}
