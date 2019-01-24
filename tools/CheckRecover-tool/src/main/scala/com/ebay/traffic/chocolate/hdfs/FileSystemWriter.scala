package com.ebay.traffic.chocolate.hdfs

import com.ebay.traffic.chocolate.task.CountData
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

/**
  * Created by lxiong1 on 27/11/18.
  */
object FileSystemWriter {

  /**
    * Write the data to hdfs.
    *
    * @param dataCountDir
    * @param countData
    * @param spark
    * @return
    */
  def write(dataCountDir: String, uri: String, countData: CountData, spark: SparkSession): Unit = {
    val temp = dataCountDir + "_temp"
    val allDF = spark.sqlContext.createDataFrame(Seq(countData))

    allDF.write.csv(temp)

    if (FileUtil.getFS(uri).exists(new Path(dataCountDir))) {
      FileUtil.getFS(uri).delete(new Path(dataCountDir))
    }
    FileUtil.getFS(uri).rename(new Path(temp), new Path(dataCountDir))
  }

}

