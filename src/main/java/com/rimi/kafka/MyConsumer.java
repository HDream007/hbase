package com.rimi.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;


/**
 * 消费者
 */
public class MyConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","hd01:9092");
        properties.setProperty("group.id","test");
        properties.setProperty("enable.auto.commit","true");
        properties.setProperty("auto.offset.reset","earliest");


        //序列化
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        Consumer<String,String> consumer = new KafkaConsumer<String,String>(properties);

        consumer.subscribe(Arrays.asList("test"));

        while(true){
            //读取
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(1000));

            Iterable<ConsumerRecord<String, String>> iterable = records.records("test");

            for (ConsumerRecord<String, String> record : iterable) {
                System.out.println(record.value());
            }
        }



    }
}
