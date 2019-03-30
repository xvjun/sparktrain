package com.xvjun.databaseUtils.connectionPool

object PoolManager {

  def getInstce(): MyPoolImpl ={
    val poolImpl:MyPoolImpl = new MyPoolImpl()
    poolImpl
  }

}
