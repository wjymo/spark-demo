package com.wjy.stream.kafka;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.*;

public class KafkaConsumerDemo implements Runnable {
    private String topic;

    public KafkaConsumerDemo(String topic) {
        this.topic = topic;
    }


//    private ConsumerConnector createConnector(){
//        Properties properties = new Properties();
//        properties.put("zookeeper.connect", KafkaProperties.ZK);
//        properties.put("group.id",KafkaProperties.GROUP_ID);
//        return Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
//    }

    @Override
    public void run() {
//        ConsumerConnector consumer = createConnector();
        Properties properties = new Properties();
        properties.put("zookeeper.connect", KafkaProperties.ZK);
        properties.put("group.id",KafkaProperties.GROUP_ID);
        properties.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer kafkaConsumer=new KafkaConsumer(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        ConsumerRecords<Integer, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
        Iterator<ConsumerRecord<Integer, String>> iterator1 = records.iterator();
        while (iterator1.hasNext()){
            ConsumerRecord<Integer, String> next = iterator1.next();
            System.out.println("rec: " + next.value());
        }
        try {
            kafkaConsumer.commitSync();
        } catch (CommitFailedException e) {
            e.printStackTrace();
        }finally {
            kafkaConsumer.close();
        }


//        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//        topicCountMap.put(topic, 1);
//        topicCountMap.put(topic2, 1);
//        topicCountMap.put(topic3, 1);

        // String: topic
        // List<KafkaStream<byte[], byte[]>>  对应的数据流
//        Map<String, List<KafkaStream<byte[], byte[]>>> messageStream =  consumer.createMessageStreams(topicCountMap);
//
//        KafkaStream<byte[], byte[]> stream = messageStream.get(topic).get(0);   //获取我们每次接收到的暑假
//
//        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
//
//
//        while (iterator.hasNext()) {
//            String message = new String(iterator.next().message());
//            System.out.println("rec: " + message);
//        }
    }
}
