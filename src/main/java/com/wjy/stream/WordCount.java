package com.wjy.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .set("spark.driver.host","localhost")
                .setAppName("WordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));

        JavaReceiverInputDStream<String> dStream = jssc.socketTextStream("wang-109", 44444);

        JavaDStream<String> flatMap = dStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> mapToPair = flatMap.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
        JavaPairDStream<String, Integer> reduceByKey = mapToPair.reduceByKey((v1, v2) -> v1 + v2);
        reduceByKey.print();

        // 首先对JavaSteamingContext进行一下后续处理
        // 必须调用JavaStreamingContext的start()方法，整个Spark Streaming Application才会启动执行
        // 否则是不会执行的
        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
}
