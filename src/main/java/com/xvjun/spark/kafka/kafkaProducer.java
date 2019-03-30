package com.xvjun.spark.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class kafkaProducer extends Thread{

    private String topic;

    private Producer<String,String > producer;

    kafkaProducer(String topic){
        this.topic = topic;
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop00-1:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("auto.offset.reset","latest");

        producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("hello_topic", Integer.toString(i), Integer.toString(i)));
            System.out.println("sent:" + i);
        }
//        producer.close();
    }

//    @Override
//    public void run(){
//
//        int messageNo = 1;
//
//        while(true){
//            String message = "massage" + messageNo;
//            producer.send(new ProducerRecord<String, String>(topic,"a",message));
//            System.out.println("sent" + message);
//            messageNo++;
//            try{
//                Thread.sleep(2);
//            }catch(Exception e){
//                e.printStackTrace();
//            }
//        }
//
//    }

}
