package com.ebay.traffic.chocolate.sparknrt.mergeUtpSmallFiles

import com.ebay.traffic.chocolate.spark.BaseSparkJobV2
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
 * This job only used for partitioned tables, and the number of partition levels is one
 * For example
 * table: im_tracking.utp_event; partition: dt;
 *
 * Author yli19
 * since 2022/04/11
 */

object MergeUtpSmallFilesJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new MergeUtpSmallFilesJob(params)
    job.run()
    job.stop()
  }
}

class MergeUtpSmallFilesJob(params: Parameter, override val enableHiveSupport: Boolean = true)
  extends BaseSparkJobV2(params.appName, params.mode, true) {

  lazy val table: String = params.table
  lazy val partitionName: String = params.partitionName
  lazy val partition: String = params.partition
  lazy val partitionNum: Int = params.partitionNum
  lazy val outputDir: String = params.outputDir
  lazy val mergedDir: String = outputDir + "/" + partitionName + "=" + partition

  def mergeFile(): Unit = {
    logger.info("Begin merge data:" + partition)
    val dataDf = readTable().repartition(partitionNum)
    saveDFToFiles(dataDf, mergedDir)
    logger.info("Finish merge data:" + partition)
  }

  def readTable(): DataFrame = {
    val sql = "select * from %s where %s='%s'".format(table, partitionName, partition)
    logger.info("select sql: " + sql)
    val sourceDf = sqlsc.sql(sql)
    sourceDf
  }

  def saveDFToUtp(df: DataFrame): Unit = {
    val writer = df.write.mode(SaveMode.Overwrite)
    writer.partitionBy("hour")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    writer.format("parquet").save(outputDir)
  }

  def checkParamValid(): Boolean = {
    if (StringUtils.isBlank(table) || StringUtils.isBlank(partitionName) || StringUtils.isBlank(partition) || StringUtils.isBlank(outputDir)) {
      false
    } else {
      true
    }
  }

  def beforeRun(mergedDir: String): Unit = {
    val mergedPath = new Path(mergedDir)
    if (fs.exists(mergedPath)) {
      fs.delete(mergedPath, true)
    }
    fs.mkdirs(mergedPath)
  }

  /**
   * Entry of this spark job
   */
  override def run(): Unit = {
    if (!checkParamValid()) {
      throw new RuntimeException("Params error")
    }
    beforeRun(mergedDir)
    mergeFile()
  }
}

