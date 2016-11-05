
/*
* Author: Manish Gupta
*/

package com.guptam.spark.dba.hdfsutil

import com.guptam.spark.dba.common._

trait HDFSStringUtil {

  // fix the path string
  def removeLastSlash(url: String): String = {
    if (url.endsWith("/")) {
      url.substring(0, url.lastIndexOf("/"))
    } else {
      url
    }
  }

  // Generate a string that is unique and can be sorted on time
  def getUniqueHashAndTime(): String = {
    System.currentTimeMillis().toString() +
      Constants.UNDERSCORE +
      hashCode().toString() +
      Constants.UNDERSCORE +
      util.Random.nextInt().abs.toString()
  }
}
