package com.ebay.traffic.chocolate.hdfs

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by lxiong1 on 27/11/18.
  */
object FileSystemReader {

  /**
    * Read data from hdfs.
    *
    * @param file
    * @param spark
    * @return
    */
  def read(file: String, uri: String, spark: SparkSession): DataFrame = {
    if (FileUtil.getFS(uri).exists(new Path(file)) && FileUtil.isExistFile(file, uri)) {
      return spark.read.csv(file)
    } else {
      return null
    }
  }

  /**
    * Count the file in the current directory.
    *
    * @param inputDir
    * @return
    */
  def getFileNum(inputDir: String, uri: String): Int = {
    if (FileUtil.getFS(uri).exists(new Path(inputDir))) {
      val fileStatus = FileUtil.getFS(uri).listStatus(new Path(inputDir))
      val files = fileStatus.filter(status => status.getPath.getName != "_SUCCESS")

      return files.size
    } else {
      return 0
    }
  }

}
