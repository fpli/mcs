package com.ebay.traffic.chocolate.hdfs

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object FileUtil {

  /**
    * If it exists file in the current directory.
    *
    * @param file
    * @return true when it exist file in the current directory.
    */
  def isExistFile(file: String, uri: String): Boolean = {
    val fsStatus = getFS(uri).listStatus(new Path(file))
    val files = fsStatus.filter(status => status.getPath.getName != "_SUCCESS")
    if (fsStatus.size > 0) {
      return true
    } else {
      return false
    }
  }

  def getFS(uri: String): FileSystem = {
    var conf = new Configuration()
    if (!uri.equals(""))
      FileSystem.get(URI.create(uri), conf)
    else
      FileSystem.get(conf)
  }

}