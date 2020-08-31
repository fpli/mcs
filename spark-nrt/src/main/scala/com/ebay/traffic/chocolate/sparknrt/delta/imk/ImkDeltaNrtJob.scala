/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.delta.imk

import java.time.{Instant, ZoneId, ZonedDateTime}

import org.apache.spark.sql.{DataFrame, SaveMode}
import com.ebay.traffic.chocolate.sparknrt.delta.{BaseDeltaLakeNrtJob, Parameter}

/**
  * @author Xiang Li
  * @since 2020/08/18
  * Imk NRT job to extract data from master table and sink into IMK table
  */
object ImkDeltaNrtJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new ImkDeltaNrtJob(params)

    job.run()
    job.stop()
  }
}

/**
  * IMK NRT job
  * @param params input parameters
  * @param enableHiveSupport enable hive support for spark sql table query
  */
class ImkDeltaNrtJob(params: Parameter, override val enableHiveSupport: Boolean = true)
  extends BaseDeltaLakeNrtJob(params, enableHiveSupport) {


  import spark.implicits._

  /**
    * Read everything need from the source table
    * @param inputDateTime input date time
    */
  override def readSource(inputDateTime: ZonedDateTime): DataFrame = {
    val fromDateTime = getLastDoneFileDateTimeAndDelay(inputDateTime, deltaDoneFileDir)._1
    val fromDateString = fromDateTime.format(dtFormatter)
    val startTimestamp = fromDateTime.toEpochSecond * 1000
    val sql = "select snapshotid, eventtimestamp, channeltype, channelaction, dt from " + inputSource + " where dt >= '" + fromDateString + "' and eventtimestamp >='" + startTimestamp +"'"
    val sourceDf = sqlsc.sql(sql)
    sourceDf
  }

  /**
    * Function to write file to output dir
    * @param df dataframe to write
    * @param dtString date partition
    */
  override def writeToOutput(df: DataFrame, dtString: String): Unit = {
    // save to final output
    this.saveDFToFiles(df, outputDir + "/"
      + dt + "=" + dtString, writeMode = SaveMode.Append)
  }

  /**
    * Entry of this spark job
    */
  override def run(): Unit = {
    val now = ZonedDateTime.now(defaultZoneId)
    updateDelta(now)
    updateOutput(now)
  }
}
