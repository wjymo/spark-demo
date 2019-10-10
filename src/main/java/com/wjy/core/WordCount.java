package com.wjy.core;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WordCount {
    private static Logger logger= LoggerFactory.getLogger(WordCount.class);
    public static void main(String[] args) {
//        SparkConf sparkConf=new SparkConf();
//        sparkConf.setMaster("local[2]");
//        sparkConf.setAppName("wc");
//        sparkConf.set("spark.sql.warehouse.dir","D:\\test");
//        sparkConf.set("spark.driver.host","localhost");
//        sparkConf.set("spark.driver.extraJavaOptions","-Dfile.encoding=utf-8");
//        sparkConf.set("spark.executor.extraJavaOptions","-Dfile.encoding=utf-8");

//        JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);

        SparkSession.Builder builder = SparkSession.builder();
        builder.master("local[2]");
        builder.appName("wc");
//        builder.config("spark.sql.warehouse.dir", "/test");
        builder.config("spark.driver.host","localhost");
        SparkSession sparkSession = builder.getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);

        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("hdfs://wang-109:8020/tmp/wc.txt");
        JavaRDD<String> flatMap = stringJavaRDD.flatMap(line -> {
//            TimeUnit.SECONDS.sleep(9);
            return     Arrays.asList(line.split(" ")).iterator();
        });
        JavaPairRDD<String, Integer> pairRDD = flatMap.mapToPair(word -> {
//            TimeUnit.SECONDS.sleep(7);
            return     new Tuple2<>(word, 1);
        });
        JavaPairRDD<String, Integer> reduceByKey = pairRDD.reduceByKey((v1, v2) -> {
//            TimeUnit.SECONDS.sleep(7);
            return v1 + v2;
        });
        reduceByKey.foreach(word->logger.info(word._1+" : "+word._2));

        sparkSession.stop();
    }
}
