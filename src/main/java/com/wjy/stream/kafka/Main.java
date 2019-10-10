package com.wjy.stream.kafka;

public class Main {
    public static void main(String[] args) {
        KafkaProducerDemo kafkaProducerDemo=new KafkaProducerDemo("geng");
        new Thread(kafkaProducerDemo).start();
//        KafkaConsumerDemo kafkaConsumerDemo=new KafkaConsumerDemo("test2");
//        new Thread(kafkaConsumerDemo).start();
    }
}
