package com.wjy.stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AsyncCommit {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf=new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("AsyncCommit");
        sparkConf.set("spark.driver.host", "localhost");
        JavaStreamingContext javaStreamingContext=new JavaStreamingContext(sparkConf, Durations.seconds(4));


        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers","wang-108:9092");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("enable.auto.commit","false");
        kafkaParams.put("auto.offset.reset","earliest");
        kafkaParams.put("group.id","xiaoer");

        List<String> topics= Arrays.asList("geng3");

        JavaInputDStream<ConsumerRecord<String, String>> directStream =
                KafkaUtils.createDirectStream(javaStreamingContext, LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams));
        JavaDStream<String> valueRDD = directStream.map(consumerRecord -> {
            String value = consumerRecord.value();
            return value;
        });
        valueRDD.print();


        //手动提交offset
        directStream.foreachRDD(rdd->{
            HasOffsetRanges hasOffsetRanges = (HasOffsetRanges) rdd.rdd();
            OffsetRange[] offsetRanges = hasOffsetRanges.offsetRanges();
            CanCommitOffsets canCommitOffsets = (CanCommitOffsets) directStream.inputDStream();
            canCommitOffsets.commitAsync(offsetRanges);
        });



        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
        javaStreamingContext.stop();
    }
}
