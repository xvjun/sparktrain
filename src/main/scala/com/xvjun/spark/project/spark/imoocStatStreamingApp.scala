package com.xvjun.spark.project.spark

import com.sql.hbase.HbaseUtils
import com.xvjun.spark.project.dao.{CourseSearchClickCountDAO, DayCourseIdCountDAO}
import com.xvjun.spark.project.domain.{ClickLog, CourseIdCount, CourseSearchClickCount}
import com.xvjun.spark.project.utils.TimeUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.{ListBuffer, Map}

object imoocStatStreamingApp extends App{

  if(args.length != 2){
    System.err.println("Usage: imoocStatStreamingApp <bootstrap.servers>  <topic>")
    System.exit(-1)
  }
  val Array(bootstrapServers,topics) = args

  val topicMap = topics.split(",").map((_,1)).toMap

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val sparkConf = new SparkConf().setAppName("imoocStatStreamingApp")
  //val sparkConf = new SparkConf().setMaster("local[2]").setAppName("imoocStatStreamingApp")

  val ssc = new StreamingContext(sparkConf,Seconds(60))

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
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  val topic = Array(topics)

  val stream = KafkaUtils.createDirectStream[String, String](ssc,PreferConsistent,
    Subscribe[String, String](topic, kafkaParams))
//  stream.map(record => (record.offset(),record.key, record.value)).print()
  //  stream.map(x => x.value()).flatMap(_.split(" ")).map((_,1)).reduceByKey((_+_)).print()
//  stream.map(x => x.toString).print()
//    stream.map(_._2).print()
  //TODO...  end
  val valueMap = stream.map(_.value)
  val cleanData = valueMap.map(line => {

    val infos = line.split("\t")
    val TimeUtils = new TimeUtils()
//    print(a.parseToMinute("2017-10-22 14:45:12"))
    val time_h = TimeUtils.parseToMinute(infos(0))
    val time = infos(0)

    val ip = infos(1)

    val G_P = infos(2).split(" ")(0).substring(1)
    val url = infos(2).split(" ")(1)
    val HTTPS = infos(2).split(" ")(2).substring(0,infos(2).split(" ")(2).length-1)
    val IDs = url.split("/")

    val types = IDs(1)
    var ID = 0
    if(IDs(2).endsWith("html")){
      ID = IDs(2).substring(0,IDs(2).lastIndexOf(".")).toInt
    }else{
      ID = IDs(2).toInt
    }

    var StatusCode = 0
    if(infos(4) != "-"){
      StatusCode = infos(4).toInt
    }
    val refer = infos(3)
    val traffic = infos(5).toInt
    val Access_side = infos(6)
    val Response_time = infos(7).toDouble.formatted("%.2f").toDouble
    ClickLog(ip,time,time_h,G_P,url,types,ID,HTTPS,StatusCode,refer,traffic,Access_side,Response_time)
//    val hbase = new HbaseUtils

  }).filter(ClickLog => ClickLog.StatusCode != 500).filter(ClickLog => ClickLog.courseid != 0 )
  cleanData.print()

//  stream.map(record => (record.offset(),record.value)).print()


  //TODO... 统计今天到现在为止实战课程的访问量
  val DayCourseIdCountDAO = new DayCourseIdCountDAO

  cleanData.map(x => {
    (x.time_h.substring(0,8)+"_"+x.types+"_"+x.courseid,1)
  }).reduceByKey(_+_).foreachRDD(rdd => {
    rdd.foreachPartition(partitionRecords => {

      val list = new ListBuffer[CourseIdCount]
      partitionRecords.foreach(pair => {
        list.append(CourseIdCount(pair._1,pair._2))
      })

      DayCourseIdCountDAO.save(list)

    })
  })

  //TODO... 统计今天到现在为止实战课程来源网站的访问量
  val CourseSearchClickCountDAO = new CourseSearchClickCountDAO
  cleanData.map(x => {
    val refers = x.refer.replaceAll("//", "/").split("/")
    var search_url = ""
    if (refers.length >= 2) {
      search_url = refers(1)
    }
    (x.time_h.substring(0, 8), search_url, x.types, x.courseid)
  }).filter(_._2 != "").map(x => {

    (x._1+"_"+x._2+"_"+x._3+"_"+x._4,1)
  }).reduceByKey(_+_).foreachRDD(rdd => {
    rdd.foreachPartition(partitionRecords => {

      val list = new ListBuffer[CourseSearchClickCount]
      partitionRecords.foreach(pair => {
        list.append(CourseSearchClickCount(pair._1,pair._2))
      })

      CourseSearchClickCountDAO.save(list)

    })
  })



  ssc.start()
  ssc.awaitTermination()

}
//url,cmsType,cmsId,traffic,ip,province,city,time,h,G_P,Access_side,Response_time
