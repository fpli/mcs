/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.utils

import java.security.SecureRandom
import java.util.UUID.randomUUID
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame

object Utils {

  def uuid = {
    randomUUID.toString
  }

  lazy val TIME_MASK = 0xFFFFFFl << 53l  // can represent to year 2255
  lazy val rand = new SecureRandom

  /** Simple uid generator based on timestamp + random number **/
  def simpleUid(): Long = {
    val time = System.currentTimeMillis()
    ((time & ~TIME_MASK) << 10l) | rand.nextInt(1024)
  }

  def getClassLoader() = {
    var classLoader = Thread.currentThread().getContextClassLoader()
    if (classLoader == null) {
      classLoader = getClass.getClassLoader
    }
    classLoader
  }

  def now(): Long = {
    System.currentTimeMillis()
  }

  /**
    * Union two DataFrames
    */
  def unionDf(df1: DataFrame, df2: DataFrame) = {
    if (df2 != null) {
      if (df1 == null) df2 else df1.union(df2)
    } else df1
  }

  def writeIERequest(requestId: Int, responsePath: String, fs: FileSystem, path: String) = {
    val out = fs.create(new Path(path), true)
    try {
      out.writeInt(requestId)
      out.writeInt(responsePath.length)
      out.writeBytes(responsePath)
    } finally {
      out.close()
    }
  }

  def readIERequest(fs: FileSystem, path: String): (Int, String) = {
    val in = fs.open(new Path(path))
    try {
      val requestId = in.readInt()
      val length = in.readInt()
      val bytes = new Array[Byte](length)
      in.readFully(bytes)
      (requestId, new String(bytes))
    } finally {
      in.close()
    }
  }

  def showString(
                  df: DataFrame ,
                  numRows: Int,
                  truncate: Boolean): String = {
    if (truncate) {
      "\n" + showString(df, numRows)
    } else {
      "\n" + showString(df, numRows, truncate = 0)
    }
  }

  def showString(
                  df: DataFrame ,
                  _numRows: Int,
                  truncate: Int = 20,
                  vertical: Boolean = false): String = {
    val showString = classOf[org.apache.spark.sql.DataFrame].getDeclaredMethod("showString", classOf[Int], classOf[Int], classOf[Boolean])
    showString.setAccessible(true)
    showString.invoke(df, _numRows.asInstanceOf[Object], truncate.asInstanceOf[Object], vertical.asInstanceOf[Object]).asInstanceOf[String]
  }
}