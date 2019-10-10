package com.wjy.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class UpdateStateByKeyWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .set("spark.driver.host","localhost")
                .setAppName("UpdateStateByKeyWordCount");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));
        jssc.checkpoint("G:\\BaiduNetdiskDownload\\checkpoint");
        JavaSparkContext javaSparkContext = jssc.sparkContext();
        javaSparkContext.setLogLevel("ERROR");


        JavaReceiverInputDStream<String> dStream = jssc.socketTextStream("wang-109", 44444);

        JavaDStream<String> flatMap = dStream.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        JavaPairDStream<String, Integer> mapToPair = flatMap.mapToPair(word -> new Tuple2<String, Integer>(word, 1));
        JavaPairDStream<String, Integer> reduceByKey = mapToPair.updateStateByKey((values, state) -> {
            Integer newValue = 0;
            if(state.isPresent()) {
                newValue = state.get();
            }
            // 接着，将本次新出现的值，都累加到newValue上去，就是一个key目前的全局的统计
            // 次数
            for(Integer value : values) {
                newValue += value;
            }
            return Optional.of(newValue);
        });
        reduceByKey.print();

        // 首先对JavaSteamingContext进行一下后续处理
        // 必须调用JavaStreamingContext的start()方法，整个Spark Streaming Application才会启动执行
        // 否则是不会执行的
        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }
}
