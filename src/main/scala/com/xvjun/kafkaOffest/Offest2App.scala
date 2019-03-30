package com.xvjun.kafkaOffest

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable.Map

object Offest2App extends App {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  val sparkConf = new SparkConf().setMaster("local[2]").setAppName("kafkaStreaming")



  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "hadoop00-1:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "0",
//    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  val topic = Array("hello_topic")

  val checkpointDirectory = "hdfs://hadoop00-1:8020/offest-kafka"

  def functionToCreateContext(): StreamingContext = {

    val ssc = new StreamingContext(sparkConf,Seconds(10))
    val stream = KafkaUtils.createDirectStream[String, String](ssc,PreferConsistent,Subscribe[String, String](topic, kafkaParams))

    stream.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        println("offest:"+rdd.count())
      }
    })

    ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
    stream.checkpoint(Duration(10*1000))
    ssc
  }

  // Get StreamingContext from checkpoint data or create a new one
  val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)


//  val stream = KafkaUtils.createDirectStream[String, String](ssc,PreferConsistent,
//    Subscribe[String, String](topic, kafkaParams))


//  stream.map(record => (record.offset(),record.key, record.value)).print()
  //  stream.map(x => x.value()).flatMap(_.split(" ")).map((_,1)).reduceByKey((_+_)).print()


  ssc.start();
  ssc.awaitTermination();

}

//stream.foreachRDD(rdd => {
//if(!rdd.isEmpty()){
//println("offest:"+rdd.count())
//}
//})
//
//ssc.checkpoint(checkpointDirectory)   // set checkpoint directory
//stream.checkpoint(Duration(10*1000))
//ssc
//}
//
//// Get StreamingContext from checkpoint data or create a new one
//val ssc = StreamingContext.getOrCreate(checkpointDirectory, functionToCreateContext _)


//  val stream = KafkaUtils.createDirectStream[String, String](ssc,PreferConsistent,
//    Subscribe[String, String](topic, kafkaParams))


//  stream.map(record => (record.offset(),record.key, record.value)).print()
//  stream.map(x => x.value()).flatMap(_.split(" ")).map((_,1)).reduceByKey((_+_)).print()

