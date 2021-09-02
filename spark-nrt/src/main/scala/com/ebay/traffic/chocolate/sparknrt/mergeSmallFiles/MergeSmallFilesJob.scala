package com.ebay.traffic.chocolate.sparknrt.mergeSmallFiles

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

/**
 * This job only used for partitioned tables, and the number of partition levels is one
 * For example
 * table: im_tracking.imk_rvr_trckng_event_v2; partition: dt; (Good case)
 * table: im_tracking.utp_event; partition: dt,hour; (Bad case)
 *
 * Author yli19
 * since 2021/09/01
 */

object MergeSmallFilesJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new MergeSmallFilesJob(params)
    job.run()
    job.stop()
  }
}

class MergeSmallFilesJob(params: Parameter, override val enableHiveSupport: Boolean = true)
  extends BaseSparkJob(params.appName, params.mode, true) {

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
  }

  def readTable(): DataFrame = {
    val sql = "select * from %s where %s='%s'".format(table, partitionName, partition)
    logger.info("select sql: " + sql)
    val sourceDf = sqlsc.sql(sql)
    sourceDf
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
    if (checkParamValid()) {
    } else {
      throw new RuntimeException("Params error")
    }
    beforeRun(mergedDir)
  }
}
