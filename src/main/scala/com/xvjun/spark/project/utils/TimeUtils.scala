package com.xvjun.spark.project.utils

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat


class TimeUtils {

  val YYYYMMDDHHMMSS_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val Target_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss")
  val Target_FORMAT_day = FastDateFormat.getInstance("yyyyMMdd")

  def getTime(time:String) = {
    YYYYMMDDHHMMSS_FORMAT.parse(time).getTime
  }

  def parseToMinute(time:String): String ={
    Target_FORMAT.format(new Date(getTime(time)))
  }

  def parseToDay(time:String): String ={
    Target_FORMAT_day.format(new Date(getTime(time)))
  }


}

object TimeUtils{
  def main(args: Array[String]): Unit = {
    val a = new TimeUtils()
    print(a.parseToMinute("2017-10-22 14:45:12"))
  }
}
