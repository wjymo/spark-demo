package com.wjy.stream;

import com.wjy.stream.entity.ClickLog;
import com.wjy.stream.entity.CourseClickCount;
import com.wjy.util.HbaseUtil;
import org.apache.commons.collections.IteratorUtils;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.*;

public class FlumeKafkaTest {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local[2]");
        sparkConf.setAppName("FlumeKafkaTest");
        sparkConf.set("spark.driver.host", "localhost");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(60));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "wang-108:9092");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("enable.auto.commit", "false");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("group.id", "fangya2");

        List<String> topics = Arrays.asList("log");

        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(javaStreamingContext, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));
        JavaDStream<String> logs = directStream.map(consumerRecord -> consumerRecord.value());
        JavaDStream<ClickLog> cleanData = logs.map(line -> {
            String[] split = line.split("\t");
            String url = split[1];
            int courseId = 0;
            if (url.startsWith("class")) {
                courseId = Integer.parseInt(url.split("/")[1]);
            }
            return ClickLog.builder().courseId(courseId).time(split[0]).ip(split[2])
                    .statusCode(Integer.parseInt(split[4])).referer(split[3])
                    .build();
        }).filter(clickLog -> !clickLog.getCourseId().equals(0));
        cleanData.print();
        cleanData.mapToPair(clickLog -> new Tuple2<>(clickLog.getTime().replaceAll("-","")
                .substring(0, 8) + "_" + clickLog.getCourseId(), 1))
                .reduceByKey((v1, v2) -> v1 + v2).foreachRDD(rdd -> {
                    rdd.foreachPartition(partitionRecords->{
//                        List<CourseClickCount> list=new ArrayList<>();
                        HbaseUtil instance = HbaseUtil.getInstance();
                        Table table = instance.getTable("imooc_course_clickcount");
                        if (partitionRecords.hasNext()) {
                            Tuple2<String, Integer> next = partitionRecords.next();
                            String rowkey = next._1;
                            Long count = Long.parseLong(next._2.toString());
                            table.incrementColumnValue(rowkey.getBytes(),"info".getBytes(),"click_count".getBytes(),count);
                        }


                    });
                });


        //手动提交offset
        directStream.foreachRDD(rdd -> {
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
