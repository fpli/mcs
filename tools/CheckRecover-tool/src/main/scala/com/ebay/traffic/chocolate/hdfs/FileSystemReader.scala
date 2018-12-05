package com.ebay.traffic.chocolate.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FsStatus, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by lxiong1 on 27/11/18.
  */
object FileSystemReader {

  /**
    * The hadoop conf
    */
  @transient lazy val hadoopConf = {
    new Configuration()
  }

  /**
    * The file system
    */
  @transient lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  def read(file: String, spark: SparkSession): DataFrame = {
    if (fs.exists(new Path(file)) && FileUtil.isExistFile(file)) {
      return spark.read.csv(file);
    } else {
      return null
    }
  }

  def getFileNum(inputDir: String): Int = {
    if(fs.exists(new Path(inputDir))){
      val fileStatus = fs.listStatus(new Path(inputDir));
      val files = fileStatus.filter(status => status.getPath.getName != "_SUCCESS");

      return files.size;
    }else{
      return 0;
    }

  }

}
