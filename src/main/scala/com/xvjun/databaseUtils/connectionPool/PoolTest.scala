package com.xvjun.databaseUtils.connectionPool

import scala.collection.mutable.ArrayBuffer

object PoolTest extends App {



  val poolImpl:MyPoolImpl = PoolManager.getInstce()

  def Test(): Unit ={
    this.synchronized({
      val conn:PooledConnection = poolImpl.getConnection()
      val rs = conn.carryQuiry("select * from wordcount",new ArrayBuffer[Object]())
      rs.last()
      val a = rs.getRow()
      print(a)
      while (rs.next()){
//        println(rs.getString(1)+":"+rs.getInt(2))


      }

      conn.close()
    })
  }

  Test()

//  for(i <- 1 to 2000){
//    new Thread(new Runnable {
//      override def run(): Unit = {
//        println("---------第" + i + "次----------")
//        Test()
//
//      }
//    }).start()
//  }

}
