package com.xvjun.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileWordCount {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("NetworkWordCount")

    val ssc = new StreamingContext(sparkConf,Seconds(5))

    val lines = ssc.textFileStream("file:///D://test-streaming/")
    val result = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
