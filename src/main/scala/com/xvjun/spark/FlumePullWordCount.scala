package com.xvjun.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils

object FlumePullWordCount extends App{

  if(args.length != 2){
    System.err.println("usage: FlumePushWordCount <hostname> <port>")
    System.exit(-1)
  }

  val Array(hostname,port) = args


  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val sc = new SparkConf()//.setMaster("local[2]").setAppName("FlumePushWordCount")
  val ssc = new StreamingContext(sc,Seconds(5))

  //TODO... sparkStreaming 整合 flume
  val flumeStream = FlumeUtils.createPollingStream(ssc, hostname, port.toInt)
  val rs = flumeStream.map(x => new String(x.event.getBody.array()).trim).flatMap(_.split(" ")).map((_,1)).reduceByKey((_+_))
  rs.print()

  ssc.start()
  ssc.awaitTermination()

}
