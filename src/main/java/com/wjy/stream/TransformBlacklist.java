package com.wjy.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TransformBlacklist {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .set("spark.driver.host","localhost")
                .setAppName("TransformBlacklist");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));
//        jssc.checkpoint("G:\\BaiduNetdiskDownload\\checkpoint");
        JavaSparkContext javaSparkContext = jssc.sparkContext();
        javaSparkContext.setLogLevel("ERROR");

        // 先做一份模拟的黑名单RDD
        List<Tuple2<String, Boolean>> blacklist = new ArrayList<Tuple2<String, Boolean>>();
        blacklist.add(new Tuple2<String, Boolean>("tom", true));
        final JavaPairRDD<String, Boolean> blacklistRDD = jssc.sparkContext().parallelizePairs(blacklist);

        JavaReceiverInputDStream<String> dStream = jssc.socketTextStream("wang-109", 44444);

        JavaPairDStream<String, String> mapToPair = dStream.mapToPair(adsClickLog ->
                new Tuple2<String, String>(adsClickLog.split(" ")[1], adsClickLog));
//        mapToPair.print();
        JavaDStream<String> validAdsClickLogDStream = mapToPair.transform(
                new Function<JavaPairRDD<String,String>, JavaRDD<String>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD)  {
                // 这里为什么用左外连接？
                // 因为，并不是每个用户都存在于黑名单中的
                // 所以，如果直接用join，那么没有存在于黑名单中的数据，会无法join到
                // 就给丢弃掉了
                // 所以，这里用leftOuterJoin，就是说，哪怕一个user不在黑名单RDD中，没有join到
                // 也还是会被保存下来的
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD =
                        userAdsClickLogRDD.leftOuterJoin(blacklistRDD);

                // 连接之后，执行filter算子
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filteredRDD =
                        joinedRDD.filter(tuple->{
                            if(tuple._2._2().isPresent() &&
                                    tuple._2._2.get()) {
                                return false;
                            }
                            return true;
                        });

                // 此时，filteredRDD中，就只剩下没有被黑名单过滤的用户点击了
                // 进行map操作，转换成我们想要的格式
                JavaRDD<String> validAdsClickLogRDD = filteredRDD.map(tuple->tuple._2._1);
                return validAdsClickLogRDD;
            }
        });

        validAdsClickLogDStream.print();

        // 首先对JavaSteamingContext进行一下后续处理
        // 必须调用JavaStreamingContext的start()方法，整个Spark Streaming Application才会启动执行
        // 否则是不会执行的
        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }
}
