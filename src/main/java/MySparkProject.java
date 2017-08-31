import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.streaming.StreamInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class MySparkProject {

    public static void main(String[] args) {
        String appName = "My Spark Project";
        SparkConf conf = new SparkConf()
//                .setMaster("local[2]")
                .setAppName(appName)
                .setJars(new String[]{"/root/IdeaProjects/spark/target/spark-project-1.0.jar"});

        JavaSparkContext sc = new JavaSparkContext(conf);
        JobConf jobConf = new JobConf();
        jobConf.set("stream.recordreader.class", "org.apache.org.hadoop.streaming.StreamXmlRecordReader");
        jobConf.set("stream.recordreader.begin","<page>");
        jobConf.set("stream.recordreader.end","</page>");
        FileInputFormat.addInputPaths(jobConf, "filePath");

        sc.hadoopRDD(jobConf, StreamInputFormat.class, Text.class, Text.class);


    }
}
