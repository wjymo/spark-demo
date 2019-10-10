package com.wjy.sql.log;

import com.wjy.util.DateUtils;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkFormatJob {
    public static void main(String[] args) {
        SparkSession.Builder formatJob = SparkSession.builder().appName("SparkFormatJob");
        formatJob.master("local[2]");
        formatJob.config("spark.driver.host","localhost");
        SparkSession sparkSession = formatJob.getOrCreate();

        SparkContext sparkContext =sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("D:\\devtools2\\nginx\\nginx-1.16.0\\logs\\access.log");
        stringJavaRDD.map(line->{
            String[] s = line.split(" ");
            String ip=s[0];
            String time=s[3]+" "+s[4];
            String url=s[11].replaceAll("\"","");
            String traffic=s[9];
            return DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip;
        }).saveAsTextFile("D:\\devtools2\\nginx\\nginx-1.16.0\\logs\\result");

        sparkSession.stop();
    }
}
