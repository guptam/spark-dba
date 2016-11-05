
/*
* Author: Manish Gupta
*/

package com.guptam.spark.dba.common

import org.apache.log4j.{ Level, Logger }
import scala.collection.mutable.ListBuffer
import java.util.Date

// scalastyle:off println
class CustomLogger {

  // Case class to represent a single log record
  case class CustomLogRecord(
      LogTime: String,
      LogCategory: String,
      ThreadName: String,
      LogMessage: String) {

    // Custom toString (Tab Separated)
    override def toString(): String = {
      LogTime +
        Constants.TAB + LogCategory.toString().replaceAll(Constants.TAB, " ") +
        Constants.TAB + ThreadName.replaceAll(Constants.TAB, " ") +
        Constants.TAB + LogMessage.toString().replaceAll(Constants.TAB, " ")
    }
  }

  // Get Logger
  val log = Logger.getRootLogger()
  val completeLogs = new ListBuffer[CustomLogRecord]()
  completeLogs += new CustomLogRecord("LogTime", "LogCategory", "ThreadName", "LogMessage")

  // Return complete Log as List of String
  // Used by code to flush it in defrag log
  // also sort by header, Parent, Child thread, timestamp
  def getCompleteLogs(): ListBuffer[String] = {
    completeLogs.sortBy(c => {
      if (c.LogTime.equalsIgnoreCase("LogTime") &&
        c.LogCategory.equalsIgnoreCase("LogCategory")) {
        "1"
      } else {
        if (c.ThreadName.equalsIgnoreCase(Constants.PARENT)) "2"
        else "3"
      } + c.ThreadName + c.LogTime
    }).map(x => x.toString())
  }

  // custom logger
  def logme(message: Any, category: String = Constants.INFO, ThreadName: String = Constants.PARENT): Unit = {
    synchronized {
      log.info(message)
      completeLogs += new CustomLogRecord(
        new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").format(new Date()),
        category.toString().replaceAll(Constants.TAB, " "),
        ThreadName.replaceAll(Constants.TAB, " "),
        message.toString().replaceAll(Constants.TAB, " "))
      if (category.equalsIgnoreCase(Constants.ERROR)) completeLogs.foreach(println)
    }

  }

  // Has any error occured
  def containsError(): Boolean = {
    completeLogs.find(x => x.LogCategory.equalsIgnoreCase(Constants.ERROR)) match {
      case Some(l) => true
      case None => false
    }
  }

}
