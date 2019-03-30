package com.xvjun.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import scala.collection.mutable.Map

object kafkaStreaming extends App{

  if(args.length != 2){
    System.err.println("Usage: kafkaStreaming <bootstrap.servers>  <topic>")
    System.exit(-1)
  }
  val Array(bootstrapServers,topics) = args

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val sparkConf = new SparkConf().setMaster("local[2]").setAppName("kafkaStreaming")

  val ssc = new StreamingContext(sparkConf,Seconds(5))

  //TODO...  begin kafka

//  val kafkaParams = Map[String,Object]()
////  kafkaParams.
//  kafkaParams += (("bootstrap.servers", "hadoop00-1:9092"))
//  kafkaParams += (("group.id", "0"))
//  kafkaParams += (("enable.auto.commit", (true: java.lang.Boolean)))
//  kafkaParams += (("auto.offset.reset","latest"))
//  kafkaParams += (("auto.commit.interval.ms", "1000"))
//  kafkaParams += (("key.deserializer", classOf[StringDeserializer]))
//  kafkaParams.put("value.deserializer", classOf[StringDeserializer])




  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> bootstrapServers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "0",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  val topic = Array(topics)

  val stream = KafkaUtils.createDirectStream[String, String](ssc,PreferConsistent,
    Subscribe[String, String](topic, kafkaParams))
  stream.map(record => (record.offset(),record.key, record.value)).print()
//  stream.map(x => x.value()).flatMap(_.split(" ")).map((_,1)).reduceByKey((_+_)).print()
//TODO...  end

  ssc.start()
  ssc.awaitTermination()
}
