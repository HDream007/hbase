package com.rimi.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 生产者
 */
public class MyProducer {
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hd01:9092");
        properties.setProperty("acks","all");
        properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        Producer<String,String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("test","hello"+i));
        }

        producer.close();


    }
}
