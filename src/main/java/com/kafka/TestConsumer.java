package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class TestConsumer {

    private static Properties kafkaProps = new Properties();

    private static void kafkaInit() {
        kafkaProps.put("bootstrap.servers", "fake:9092");
        // group id for each consumer
        kafkaProps.put("group.id", "test");
        // if value legal, auto add offset
        kafkaProps.put("enable.auto.commit", "true");
        // set how long time to udpate the offset value
        kafkaProps.put("auto.commit.interval.ms", "1000");
        // set session response time
        kafkaProps.put("session.timeout.ms", "30000");
        //定义的key和value反序列化器
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }

    public static void main(String[] args) {
        kafkaInit();
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
        kafkaConsumer.subscribe(Collections.singletonList("test666"));
        System.out.println("Subscribed to topic:" + "test666");

        int i = 0;
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100); // ?
            for (ConsumerRecord<String, String> record : records) {
                // print the offset, key and value for the consumer records
                System.out.printf("Offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
            }
        }
    }
}