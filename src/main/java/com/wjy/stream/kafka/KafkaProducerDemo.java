package com.wjy.stream.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerDemo implements Runnable {
    private String topic;

    private Producer<Integer, String> producer;

    public KafkaProducerDemo(String topic) {
        this.topic = topic;

        Properties properties = new Properties();

        properties.put("bootstrap.servers", KafkaProperties.BROKER_LIST);
//        properties.put("metadata.broker.list",KafkaProperties.BROKER_LIST);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("request.required.acks","1");

        producer = new KafkaProducer<Integer, String>(properties);
    }
    @Override
    public void run() {
        int messageNo = 1;

        while(true) {
            String message = "message_" + messageNo;
            producer.send(new ProducerRecord<>(topic, message));
            System.out.println("Sent: " + message);

            messageNo ++ ;

            try{
                Thread.sleep(2000);
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
