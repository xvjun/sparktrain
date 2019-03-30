package com.xvjun.spark



import java.sql.ResultSet

import com.xvjun.databaseUtils.connectionPool.{MyPoolImpl, PoolManager, PooledConnection}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object ForeachRDDApp extends App{

  //    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

//      val sparkConf = new SparkConf().setMaster("local[2]").setAppName("ForeachRDDApp")
  val sparkConf = new SparkConf()
  val ssc = new StreamingContext(sparkConf,Seconds(5))

  ssc.checkpoint("file:///root/log/sparkcleck")
//
  val lines = ssc.socketTextStream("hadoop00-1",6789)
  val result  = lines.flatMap(_.split(" ")).map((_,1))
  val state = result.updateStateByKey[Int](updateFunction _)


  //TODO... 使用window获得指定时间段的数据
//val state = result.reduceByKeyAndWindow((a:Int,b:Int) => (a+b),Seconds(30),Seconds(10))
  state.print()

//  state.foreachRDD{ rdd =>
//    val connection = MySQLUtils.getConnection()
//    rdd.foreach { record =>
//
//      var list = ArrayBuffer[Object]()
//
//      list.+=(record._1)
//      list.+=(record._2)
//
//      MySQLUtils.carryIUD("insert into wordcount values(?,?)",list)
//
//    }
//
//  }


  //TODO... 将数据写入mysql(使用标准jdbc)

//  state.foreachRDD ( rdd => {
//    rdd.foreachPartition ( partitionOfRecords => {
//      val MySQL = new MySQLUtils()
//      MySQL.getConnection()
//
//      partitionOfRecords.foreach( record => {
//        var list = new ArrayBuffer[Object]()
//        list.append(record._1)
//        list.append(Integer.valueOf(record._2))
//        MySQL.carryIUD("insert into wordcount values(?,?)",list)
//      })
//      MySQL.release()
//    })
//  })


  //TODO... 将数据写入mysql(使用jdbc连接池)
  val poolImpl:MyPoolImpl = PoolManager.getInstce()
    state.foreachRDD ( rdd => {
      rdd.foreachPartition ( partitionOfRecords => {
        val conn:PooledConnection = poolImpl.getConnection()

        partitionOfRecords.foreach( record => {

          if(isReal(conn,record)) cover(conn,record) else add(conn,record)

        })
        conn.close()
      })
    })

  ssc.start()
  ssc.awaitTermination()

  private def isReal(conn:PooledConnection,record:(String,Int)): Boolean ={
    var list = new ArrayBuffer[Object]()
    list.append(record._1)
    val rs:ResultSet = conn.carryQuiry("select * from wordcount where word = ?",list)
    rs.last()
    val Row = rs.getRow
    if(Row>0)true else false
  }

  private def cover(conn:PooledConnection,record:(String,Int)): Unit ={
    var list = new ArrayBuffer[Object]()
    list.append(Integer.valueOf(record._2))
    list.append(record._1)
    conn.carryIUDOne("update wordcount set wordcount = ? where word = ?",list)
  }

  private def add(conn:PooledConnection,record:(String,Int)): Unit ={
    var list = new ArrayBuffer[Object]()
    list.append(record._1)
    list.append(Integer.valueOf(record._2))
    conn.carryIUDOne("insert into wordcount values(?,?)",list)
  }

  def updateFunction(currentValues:Seq[Int],preValues:Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current+pre)
  }
}
