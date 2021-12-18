package com.yumeng.playground.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerTemplate {


    public static void main(String[] args) {
        Properties properties = getConsumerProperties("groupId", "broker1:9092,broker2:9092");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList("topic"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    String value = record.value();
                    System.out.println(value);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            kafkaConsumer.commitAsync();
            kafkaConsumer.close();
        }

    }

    public static Properties getConsumerProperties(String groupId, String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty("session.timeout.ms", "60000");
        properties.setProperty("request.timeout.ms", "65000");
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("client.id", String.format("consumer-%s-%d", groupId, System.currentTimeMillis()));
        properties.setProperty("group.id", groupId);
        properties.setProperty("max.partition.fetch.bytes", "3145728");
        properties.setProperty("auto.offset.reset", "latest");
        properties.setProperty("heartbeat.interval.ms", "10000");
        properties.setProperty("flink.partition-discovery.interval-millis", "60000");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        return properties;
    }
}
