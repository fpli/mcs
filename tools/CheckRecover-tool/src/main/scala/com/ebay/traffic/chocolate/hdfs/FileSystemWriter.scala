package com.ebay.traffic.chocolate.hdfs

import com.ebay.traffic.chocolate.hdfs.FileSystemReader.fs
import com.ebay.traffic.chocolate.task.CountData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

/**
  * Created by lxiong1 on 27/11/18.
  */
object FileSystemWriter {

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

  def write(dataCountDir: String, countData: CountData, spark: SparkSession) = {
    val temp = dataCountDir + "_temp";
    val allDF = spark.sqlContext.createDataFrame(Seq(countData));

    allDF.write.csv(temp);

    if (fs.exists(new Path(dataCountDir))) {
      fs.delete(new Path(dataCountDir));
    }
    fs.rename(new Path(temp), new Path(dataCountDir));
  }


}

