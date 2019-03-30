package com.xvjun.spark.project.dao

import com.sql.hbase.HbaseUtils
import com.xvjun.spark.project.domain.{CourseIdCount, CourseSearchClickCount}
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/**
  * 统计当天的指定课程来源的统计
  */
class CourseSearchClickCountDAO {

  val tableName = "xvjun_search_course_clickcount"
  val cf = "CountInfo"
  val qualifer = "click_count"

  /**
    *保存数据到hbase
    * @param List 数据集合
    */
  def save(List:ListBuffer[CourseSearchClickCount]): Unit ={

    val table = HbaseUtils.getInstance().getTable(tableName)
    for(ele <- List){
      table.incrementColumnValue(Bytes.toBytes(ele.Day_Search_Course_ID),Bytes.toBytes(cf),Bytes.toBytes(qualifer),ele.Count)
    }


  }

  /**
    * 根据rowkey查询count
    * @param Day_Search_Course_ID
    */
  def count(Day_Search_Course_ID:String): Long ={
    val table = HbaseUtils.getInstance().getTable(tableName)
    val get = new Get(Bytes.toBytes(Day_Search_Course_ID))
    val value = table.get(get).getValue(cf.getBytes,qualifer.getBytes)

    if(value == null){0l}
    else {
      Bytes.toLong(value)
    }


  }

}

object CourseSearchClickCountDAO{

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[CourseSearchClickCount]

    list.append(CourseSearchClickCount("20180102_www.baidu.com_hadoop_456",20))
    list.append(CourseSearchClickCount("20180102_www.sogou.com_spark_456",10))
    list.append(CourseSearchClickCount("20180102_www.sogou.com_spark_456",10))
    list.append(CourseSearchClickCount("20180102_www.sogou.com_spark_456",10))
    val test = new CourseSearchClickCountDAO()


    test.save(list)
    println(test.count("20180102_www.sogou.com_spark_456"))
  }

}