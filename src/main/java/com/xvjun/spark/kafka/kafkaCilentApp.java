package com.xvjun.spark.kafka;

public class kafkaCilentApp {

    public static void main(String[] args){
        new kafkaProducer("hello_topic").run();

//        new kafkaConsumer("streaming_topic").run();
    }
}
