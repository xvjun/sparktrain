package com.xvjun.databaseUtils.connectionPool

import java.sql.{Connection, PreparedStatement, ResultSet}

import scala.collection.mutable.ArrayBuffer

class PooledConnection(connection: Connection,state:Boolean) {

  private var conn:Connection = connection

  private var State:Boolean = state

  def close(): Unit ={
    this.State = false
  }

  def getConn(): Connection ={
    conn
  }

  def setConn(connection: Connection): Unit ={
    this.conn=connection
  }

  def isBusy(): Boolean ={
    State
  }

  def setBusy(state:Boolean): Unit ={
    this.State=state
  }

  def carryQuiry(sql:String,list:ArrayBuffer[Object]) :ResultSet = {
    try{
      var pstmt:PreparedStatement = conn.prepareStatement(sql)
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

  def carryIUDOne(sql:String,list:ArrayBuffer[Object]): Int ={
    try{
      var pstmt:PreparedStatement = conn.prepareStatement(sql)

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

  def carryIUDBatch(sql:String,list:ArrayBuffer[ArrayBuffer[Object]]): Array[Int] ={
    try{
      var pstmt:PreparedStatement = conn.prepareStatement(sql)
      conn.setAutoCommit(false)

      for(eles <-list){
        var i = 0
        for(ele <- eles){
          i = i+1
          pstmt.setObject(i,ele)
        }
        pstmt.addBatch()
      }

      val sum:Array[Int] = pstmt.executeBatch()
      conn.commit()
      return sum
    } catch {
      case e: Exception => {
        e.printStackTrace()
        return null
      }
    }
  }



}
