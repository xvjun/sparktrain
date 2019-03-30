package com.xvjun.spark.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class kafkaConsumer extends Thread{

    private String topic;

    private KafkaConsumer consumer;

    public kafkaConsumer(String topic){
        this.topic=topic;

        final Properties props = new Properties();
        props.put("bootstrap.servers", "aliyun:9092");
        props.put("group.id", "0");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);

    }

    @Override
    public void run() {
        consumer.subscribe(Arrays.asList(this.topic));
        while (true) {
            System.out.println("asd");
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.offset() + "  " + record.key() + "  " + record.value()+
                        " "+record.partition()+ " "+record.serializedKeySize()+ " "+record.serializedValueSize()+
                        " "+record.timestamp()+ " "+record.headers()+ " "+record.timestampType()+
                        " "+record.topic()+ " "+record.toString());
            }
        }
    }
}