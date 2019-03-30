package com.xvjun.spark


import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object statefulWordCount {

  def main(args: Array[String]): Unit = {

//    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

//    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("statefulWordCount")
    val sparkConf = new SparkConf()
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    ssc.checkpoint("file:///root/data")

    val lines = ssc.socketTextStream("192.168.13.138",6789)
    val result  = lines.flatMap(_.split(" ")).map((_,1))
    val state = result.updateStateByKey[Int](updateFunction _)

    state.print()

    ssc.start()
    ssc.awaitTermination()


  }

  def updateFunction(currentValues:Seq[Int],preValues:Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current+pre)
  }

}
