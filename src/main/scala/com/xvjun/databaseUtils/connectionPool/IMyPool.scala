package com.xvjun.databaseUtils.connectionPool

trait IMyPool {

  def createConnection(count:Int)

  def getConnection():PooledConnection

}
