package com.xvjun.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
黑名单判断，通过transform来将rdd转换成DStream
 */

object TransformApp extends App {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

  val ssc = new StreamingContext(sparkConf,Seconds(5))

  //TODO...

  val blacks = List("zs","ls")
  val blackSRDD = ssc.sparkContext.parallelize(blacks).map(x => (x,true))

  val lines = ssc.socketTextStream("hadoop00-1",6789)
  val clicklog = lines.map(x => (x.split(",")(1),x)).transform( rdd => {
    rdd.leftOuterJoin(blackSRDD).filter(x => x._2._2.getOrElse(false) != true).map(x => x._2._1)
  })

  clicklog.print()
  ssc.start()
  ssc.awaitTermination()

}
