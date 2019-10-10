package com.wjy.stream;

import com.wjy.util.RedisUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OffsetOfRedis {
    private static String TOPIC_REDIS_KEY="geng3";


    public static void main(String[] args) throws InterruptedException {
        Map<String, String> map = RedisUtil.hgetAll(TOPIC_REDIS_KEY);
        if(map==null||map.isEmpty()){
            map.put("0","0");
            map.put("1","0");
        }
//        jedis.hset(REDIS_KEY,map);
        Map<TopicPartition,Long> topicPartitionLongMap=new HashMap<>();
        List<TopicPartition> topicPartitionLongList =new ArrayList<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String partition = entry.getKey();
            TopicPartition topicPartition = new TopicPartition(TOPIC_REDIS_KEY, Integer.parseInt(partition));
            topicPartitionLongList.add(topicPartition);
            String offset = entry.getValue();
            topicPartitionLongMap.put(topicPartition,Long.parseLong(offset));
        }

        SparkConf sparkConf=new SparkConf();
        sparkConf.setAppName("OffsetOfRedis");
        sparkConf.setMaster("local[2]");
        sparkConf.set("spark.driver.host", "localhost");
        sparkConf.set("spark.streaming.kafka.maxrateperpartition","10");
        JavaStreamingContext javaStreamingContext=new JavaStreamingContext(sparkConf, Durations.seconds(5));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers","wang-108:9092");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
//        kafkaParams.put("enable.auto.commit","false");
        kafkaParams.put("auto.offset.reset","latest");
        kafkaParams.put("group.id","huyao");


        LocationStrategy locationStrategy = LocationStrategies.PreferConsistent();
        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                javaStreamingContext,locationStrategy ,
                ConsumerStrategies.Assign(topicPartitionLongList,kafkaParams,topicPartitionLongMap)
        );

        JavaDStream<String> javaDStream = directStream.map(consumerRecord -> {
            String value = consumerRecord.value();
            return value;
        });
        javaDStream.foreachRDD(rdd->{
//            rdd.foreachPartition(iterator->{
//                while (iterator.hasNext()){
//                    String next = iterator.next();
//                }
//            });
            List<String> collect = rdd.collect();
            collect.forEach(System.out::println);
        });

        directStream.foreachRDD(rdd->{
            System.out.println("*****************处理完业务*****************");
            HasOffsetRanges hasOffsetRanges = (HasOffsetRanges) (rdd.rdd());
            OffsetRange[] offsetRanges = hasOffsetRanges.offsetRanges();
            for (OffsetRange offsetRange : offsetRanges) {
                System.out.println(offsetRange.topic() + ":" + offsetRange.partition() + ":" + offsetRange.fromOffset() + ":" + offsetRange.untilOffset());
            }
            for (OffsetRange offsetRange : offsetRanges) {
                String topic = offsetRange.topic();
                int partition = offsetRange.partition();
                long untilOffset = offsetRange.untilOffset();
                RedisUtil.hset(topic,String.valueOf(partition),String.valueOf(untilOffset));
            }
        });


        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();
    }
}
