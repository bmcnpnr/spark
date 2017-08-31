import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.streaming.StreamXmlRecordReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.hadoop.io.*;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MySparkExample {
    public static void main(String[] args) {
        String appName = "Spark Concepts Example";
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName(appName)
                .setJars(new String[]{"/root/IdeaProjects/spark/target/spark-project-1.0.jar"});
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> integers1to100 = sc.parallelize(IntStream.rangeClosed(1, 100).
                                             boxed().collect(Collectors.toList()));
        integers1to100.collect();

        /*this takes the directory as its path, so it has the potential to load from more than
         *one file, even accepting wildcard paths.*/
        JavaPairRDD<String, String> textFiles = sc.wholeTextFiles("/opt/idea-IU-172.3757.52/bin");

        /*it accepts path, class of key and class of value. Classes has to be an extension of Writable class
         *from hadoop.io.Writable */ /*This code will throw an exception since path didn't take a sequence file*/

        /*List<Tuple2<String, String>> collect = sc.sequenceFile("/opt/idea-IU-172.3757.52/bin/idea.sh", Integer.class, String.class)
                .mapToPair(kv -> new Tuple2<>(kv._1.toString(), kv._2))
                .collect();*/

        /*Same functionality and not a sequence file exception*/
        sc.objectFile("/opt/idea-IU-172.3757.52/bin/idea.sh").collect();

        /*Support any haddop file*/
//        sc.hadoopFile("/opt/idea-IU-172.3757.52/bin/idea.sh",String.class, Integer.class, String.class);
//        sc.newAPIHadoopFile("/opt/idea-IU-172.3757.52/bin/idea.sh",String.class, Integer.class, String.class);

    }
}
