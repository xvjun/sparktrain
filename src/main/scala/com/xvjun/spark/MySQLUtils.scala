package com.xvjun.spark

import java.io._
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util._

import scala.collection.mutable.ArrayBuffer


/**
  * MySQL操作工具类
  */
class MySQLUtils {
//  var conn: Connection = null

//  def main(args: Array[String]): Unit = {
//    println(getConnection())
//  }

  var conn:Connection = null
  var pstmt:PreparedStatement = null
  /**
    * 获取数据库连接
    */

  def getConnection() ={
    val prop = new Properties()

    val res = this.getClass.getClassLoader.getResourceAsStream("mysql_logon_initial.properties")
    prop.load(res)
//    prop.load(new FileInputStream("src/main/resources/mysql_logon_initial.properties"))
//    val drivers = prop.getProperty("drivers")
    val url = prop.getProperty("url")
    val username = prop.getProperty("username")
    val password = prop.getProperty("password")
//    Class.forName(drivers) //加载驱动程序，JDK7以上版本可以不用

    conn = DriverManager.getConnection(url, username, password)
    conn
  }

  def carryQuiry(sql:String,list:ArrayBuffer[Object]) :ResultSet = {
    try{
      pstmt = conn.prepareStatement(sql)
      var i = 0
      for(ele <- list){
        i = i+1
        pstmt.setObject(i,ele)
      }
      val rs:ResultSet = pstmt.executeQuery()
      return rs
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return null
      }
    }
  }

  def carryIUD(sql:String,list:ArrayBuffer[Object]): Int ={
    try{
      pstmt = conn.prepareStatement(sql)
      var i = 0
      for(ele <- list){
        i = i+1
        pstmt.setObject(i,ele)
      }
      val sum:Int = pstmt.executeUpdate()
      return sum
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return 0
      }
    }
  }



  /**
    * 释放数据库资源
    */
  def release(): Unit ={
    try{
      if(pstmt != null){pstmt.close()}
    }catch{
      case e: Exception => e.printStackTrace()
    }finally{
      if(conn != null){conn.close()}
    }
  }

}

object MySQLUtils{

  def main(args: Array[String]): Unit = {
    val a = new MySQLUtils()
    print(a.getConnection())
  }


}

