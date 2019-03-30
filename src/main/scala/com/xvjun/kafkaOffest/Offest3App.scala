package com.xvjun.kafkaOffest

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable.Map

object Offest3App extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Offest3App")



  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "hadoop00-1:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "0"
//    "auto.offset.reset" -> "earliest",
//    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  val topic = Array("hello_topic")

  val ssc = new StreamingContext(sparkConf,Seconds(10))
  val stream = KafkaUtils.createDirectStream[String, String](ssc,PreferConsistent,Subscribe[String, String](topic, kafkaParams))

  stream.foreachRDD(rdd => {
    if(!rdd.isEmpty()){
      println("offest:"+rdd.count())
    }
  })




  ssc.start();
  ssc.awaitTermination();

}
