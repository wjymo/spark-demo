package com.wjy.stream.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class NewKafkaConsumer {
    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.put("bootstrap-server","wang-108:9092");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);


    }
}
