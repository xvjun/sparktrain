package com.xvjun.databaseUtils.connectionPool

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.{LinkedList, Properties}

import scala.collection.mutable.ArrayBuffer

class MyPoolImpl extends IMyPool {

//  var prd:PreparedStatement = null
  private val props = getProperties()
//  private var current_num = 0      //当前连接池已产生的连接数
  private val pooledConnections = new ArrayBuffer[PooledConnection]()   //连接池

  private val stepSize = Integer.parseInt(props.getProperty("stepSize"))
  private val poolMaxSize = Integer.parseInt(props.getProperty("max_connection"))     //最大连接数
  private val initCount = Integer.parseInt(props.getProperty("init_connection_num")) //初始化产生连接数
//  private val driver = props.getProperty("driver")
  private val url = props.getProperty("url")
  private val user = props.getProperty("username")
  private val pwd = props.getProperty("password")


  private def getProperties(): Properties ={
    val props = new Properties()
    val in=this.getClass.getClassLoader.getResourceAsStream("mysql_logon_initial.properties")
    props.load(in)
    props
  }

  initConnectionPool()

  private def initConnectionPool(): ArrayBuffer[PooledConnection] = {
//    Class.forName(driver)
    AnyRef.synchronized({
      if (pooledConnections.isEmpty) {
         createConnection(initCount)
      }
      pooledConnections
    })
  }





  override def createConnection(count: Int): Unit = {
    if(poolMaxSize<=0 || pooledConnections.size+count > poolMaxSize){
      System.out.println("创建连接失败，超过最大连接数")
      throw new RuntimeException("创建连接失败，超过最大连接数")
    }
    try {
      //循环创建连接
      for(i <- 1 to count){
        //创建连
        val connection:Connection = DriverManager.getConnection(url, user, pwd)
        //实例化连接池中的连
        val pooledConnection:PooledConnection = new PooledConnection(connection, false)
        //存入连接池容器
        pooledConnections.+=(pooledConnection)
      }
    } catch {
      case e : Exception => {e.printStackTrace()}
    }

  }

  override def getConnection(): PooledConnection = {
    if(pooledConnections.size<=0){
      System.out.println("获取连接失败，连接池为空");
      throw new RuntimeException("获取连接失败，连接池为空");
    }
     val connection:PooledConnection = getRealConnection();
    //判断是否为空
    while(connection == null){
      //创建connection，步进数
      createConnection(stepSize);
      //重新获取连接，有可能获取的还为空,采用while循环判断
      getRealConnection();
      //防止其他线程过来拿连接
      try {
        Thread.sleep(300);
      } catch {
        case e : InterruptedException => {e.printStackTrace()}
        case e : Exception => {e.printStackTrace()}
      }
    }
    connection
  }

  private def getRealConnection():PooledConnection = {
    this.synchronized({
      //先判断连接池是不是有我们需要的空闲连接对象
      for(connection <- pooledConnections){
        //未处于繁忙状态
        if(!connection.isBusy()){
           val conn:Connection = connection.getConn()
          try {
            //判断这个连接是不是有效，isValid就是创建了一个statement，执行sql语句，看是否成功
            if(!conn.isValid(2000)){
              val validConn:Connection = DriverManager.getConnection(url, user, pwd);
              connection.setConn(validConn);
            }
          } catch {
            case e : Exception => {e.printStackTrace()}
          }
          //设置为繁忙
          connection.setBusy(true)
          return connection
        }
      }
      null
    })

  }



}
