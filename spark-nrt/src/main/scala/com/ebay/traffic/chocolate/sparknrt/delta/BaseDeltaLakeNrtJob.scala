/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.delta

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.temporal.ChronoUnit

import com.ebay.traffic.chocolate.sparknrt.basenrt.BaseNrtJob
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import com.ebay.traffic.chocolate.spark.DataFrameFunctions._
import com.ebay.traffic.chocolate.sparknrt.utils.Utils.simpleUid

import scala.collection.immutable

/**
  * @author Xiang Li
  * @since 2020/08/18
  * @param params input parameters
  * @param enableHiveSupport enable hive support for spark sql table query
  */
class BaseDeltaLakeNrtJob (params: Parameter, override val enableHiveSupport: Boolean = true)
  extends BaseNrtJob(params.appName, params.mode, true){
  lazy val inputSource: String = params.inputSource
  lazy val deltaDir: String = params.deltaDir
  lazy val outputDir: String = params.outPutDir
  lazy val deltaDoneFileDir: String = params.deltaDoneFileDir
  lazy val outputDoneFileDir: String = params.outputDoneFileDir
  lazy val doneFilePrefix: String = params.doneFilePrefix
  lazy val jobDir: String = params.jobDir + simpleUid() + "/"
  lazy val doneFilePostfix = "00000000"
  lazy val snapshotid = "snapshotid"
  lazy val deltaSnapshotid = "delta_snapshotid"
  lazy val eventTimestamp = "eventtimestamp"
  lazy val dt = "dt"
  lazy val multiplierForMs = 1000L

  @transient lazy val defaultZoneId: ZoneId = ZoneId.systemDefault()
  @transient lazy val dayFormatterInDoneFileName: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(defaultZoneId)
  @transient lazy val doneFileDatetimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(defaultZoneId)
  @transient lazy val dtFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  implicit def dateTimeOrdering: Ordering[ZonedDateTime] = Ordering.fromLessThan(_ isBefore  _)

  import spark.implicits._

  def comparableTimestamp(): Long = {
    1
  }

  /**
    * Get done file by date
    * @param dateTime input date time
    * @param doneDir the done file dir
    * @return done file dir by date
    */
  def getDoneDir(dateTime: ZonedDateTime, doneDir: String): String = {
    doneDir + "/" + dateTime.format(dayFormatterInDoneFileName)
  }

  /**
    * Get last done file date time from a list of file
    * @param fileStatus input file status
    * @return date time
    */
  def getLastDoneFileDatetimeFromDoneFiles(fileStatus: Array[FileStatus]): ZonedDateTime = {
    fileStatus
      .map(status => status.getPath.getName)
      .map(fileName =>  {
        val str = fileName.substring(doneFilePrefix.length, fileName.length - doneFilePostfix.length)
        ZonedDateTime.parse(str, doneFileDatetimeFormatter)
      })
      .max(dateTimeOrdering)
  }

  /**
    * Get last done file date time. Limitation: when there is delay cross 2 days, this will fail.
    * @param dateTime input date time
    * @return last date time the delta table ever touched done file and the delay hours
    */
  def getLastDoneFileDateTimeAndDelay(dateTime: ZonedDateTime, doneDir: String): (ZonedDateTime, Long) = {

    val doneDateHour = dateTime.truncatedTo(ChronoUnit.HOURS)

    val todayDoneDir = new Path(getDoneDir(doneDateHour, doneDir))
    val yesterdayDoneDir = new Path(getDoneDir(doneDateHour.minusDays(1),doneDir))

    logger.info("doneDateHour {}, todayDoneDir {}, yesterdayDoneDir {}", doneDateHour, todayDoneDir, yesterdayDoneDir)

    var lastDoneFileDatetime: ZonedDateTime = doneDateHour
    var delays = 0L

    // if today done file dir already exist, just check today's done, otherwise, check yesterday
    if (fs.exists(todayDoneDir) && fs.listStatus(todayDoneDir).length != 0) {
      lastDoneFileDatetime = getLastDoneFileDatetimeFromDoneFiles(fs.listStatus(todayDoneDir))
    } else {
      fs.mkdirs(todayDoneDir)
      lastDoneFileDatetime = getLastDoneFileDatetimeFromDoneFiles(fs.listStatus(yesterdayDoneDir))
    }
    delays = ChronoUnit.HOURS.between(lastDoneFileDatetime, doneDateHour)

    (lastDoneFileDatetime, delays)
  }

  /**
    * Read everything need from the source table. Override this function for domain specific tables.
    * The input is the date time of done file. So that we need to plus one hour to get correct timestamp.
    * @param inputDateTime input date time
    */
  def readSource(inputDateTime: ZonedDateTime): DataFrame = {
    //plus 1 hour as done file logic
    val fromDateTime = getLastDoneFileDateTimeAndDelay(inputDateTime, deltaDoneFileDir)._1.plusHours(1)
    val fromDateString = fromDateTime.format(dtFormatter)
    val startTimestamp = fromDateTime.toEpochSecond * multiplierForMs
    val sql = "select snapshotid, eventtimestamp, channeltype, channelaction, dt from " + inputSource + " where dt >= '" + fromDateString + "' and eventtimestamp >='" + startTimestamp +"'"
    logger.info("sqlToSelectSource: " + sql)
    val sourceDf = sqlsc.sql(sql)
    sourceDf
  }

  /**
    * Construct done file name
    * @param doneFileDatetime done file datetime
    * @return done file name eg. imk_rvr_trckng_event_hourly.done.201904251100000000
    */
  def getDoneFileName(doneFileDatetime: ZonedDateTime, doneFileDir: String): String = {
    getDoneDir(doneFileDatetime, doneFileDir) + "/" + doneFilePrefix + doneFileDatetime.format(doneFileDatetimeFormatter) + doneFilePostfix
  }

  /**
    * Generate done files of delta table
    * @param diffDf the input source from master table
    * @param lastDoneAndDelay last done datetime and the delayed hours
    * @param inputDateTime input datetime, it should be now
    */
  def generateDeltaDoneFile(diffDf: DataFrame, lastDoneAndDelay: (ZonedDateTime, Long), inputDateTime: ZonedDateTime): Unit = {
    // generate done file
    if(!diffDf.isEmpty) {
      val minTs = diffDf.agg(min(eventTimestamp)).head().getLong(0)
      logger.info("minTs: " + minTs)
      val instant = Instant.ofEpochMilli(minTs)
      val minDateTime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault())

      val delays = lastDoneAndDelay._2
      val times: immutable.Seq[ZonedDateTime] = (0L until delays)
        .map(delay => inputDateTime.minusHours(delay))
        .reverse
        .filter(dateTime => dateTime.plusHours(1).isBefore(minDateTime))

      times.foreach(dateTime => {
        val file = getDoneFileName(dateTime, deltaDoneFileDir)
        logger.info("touch delta done file {}", file)
        val out = fs.create(new Path(file), true)
        out.close()
      })
    }
  }

  /**
    * Generate done files of output table
    * @param lastDeltaDoneAndDelay last delta done datetime and the delayed hours
    * @param lastOutputDoneAndDelay last output done datetime and the delayed hours
    */
  def generateOutputDoneFile(lastDeltaDoneAndDelay: (ZonedDateTime, Long), lastOutputDoneAndDelay: (ZonedDateTime, Long)): Unit = {
    val delays = lastOutputDoneAndDelay._2 - lastDeltaDoneAndDelay._2
    val times: immutable.Seq[ZonedDateTime] = (0L until delays)
      .map(delay => lastDeltaDoneAndDelay._1.minusHours(delay))
      .reverse

    times.foreach(dateTime => {
      val file = getDoneFileName(dateTime, outputDoneFileDir)
      logger.info("touch output done file {}", file)
      val out = fs.create(new Path(file), true)
      out.close()
    })
  }

  /**
    * Update the delta lake table
    * @param inputDateTime input date time. It should be now.
    */
  def updateDelta(inputDateTime: ZonedDateTime): Unit = {

    val lastDoneAndDelay = getLastDoneFileDateTimeAndDelay(inputDateTime, deltaDoneFileDir)

    val deltaTable = DeltaTable.forPath(spark, deltaDir)

    // get new upserted records dataframe
    // get last done timestamp
    val lastDoneTimestamp = lastDoneAndDelay._1.plusHours(1).toEpochSecond * multiplierForMs
    // delta table after last done timestamp
    val deltaDfAfterLastDone = deltaTable.toDF
      .filter(col(eventTimestamp).>=(lastDoneTimestamp))
      .filter(col(dt).>=(lastDoneAndDelay._1.plusHours(1).format(dtFormatter)))
      .withColumnRenamed(snapshotid, deltaSnapshotid)

    // source df from master table after last done timestamp, don't need cache, since it won't change
    val sourceDf = readSource(lastDoneAndDelay._1)

    // diff diff, must cache!!
    val diffDf = sourceDf
      .join(deltaDfAfterLastDone, col(snapshotid).===(col(deltaSnapshotid)), "left_anti")
      .cache(this, params.jobDir + "/diffDf")

    // when there are new records, upsert the records
    deltaTable.as("delta")
      .merge(sourceDf.as("updates"),
        s"delta.${snapshotid} = updates.${snapshotid} and delta.${dt} = updates.${dt}")
      .whenNotMatched()
      .insertAll()
      .execute()

    generateDeltaDoneFile(diffDf, lastDoneAndDelay, inputDateTime)
  }

  /**
    * Function to write file to output dir. Using partition column for writer to write to specific partition.
    * @param df dataframe to write
    * @param dtString date partition
    */
  def writeToOutput(df: DataFrame, dtString: String): Unit = {
    // save to final output
    this.saveDFToFiles(df, outputDir, writeMode = SaveMode.Append, partitionColumn = dt)
  }

  /**
    * Update final target table. Only when target output done file and delta table done file has diff. Doing this
    * to provide pure insert for output table.
    * @param inputDateTime input date time. It should be now.
    */
  def updateOutput(inputDateTime: ZonedDateTime): Unit = {
    val lastOutputDoneAndDelay = getLastDoneFileDateTimeAndDelay(inputDateTime, outputDoneFileDir)
    val lastDeltaDoneAndDelay = getLastDoneFileDateTimeAndDelay(inputDateTime, deltaDoneFileDir)

    // if delta has less delay than output. Which means there is at least one hour complete data in delta can be merged
    // into out put table
    if (lastDeltaDoneAndDelay._2 < lastOutputDoneAndDelay._2) {
      // update output
      val deltaTable = DeltaTable.forPath(spark, deltaDir)
      val lastOutputDoneTimestamp = lastOutputDoneAndDelay._1.plusHours(1).toEpochSecond * multiplierForMs
      val lastDeltaDoneTimestamp = lastDeltaDoneAndDelay._1.plusHours(1).toEpochSecond * multiplierForMs

      val deltaDfAfterLastOuputDone = deltaTable.toDF
        .filter(col(dt).>=(lastOutputDoneAndDelay._1.plusHours(1).format(dtFormatter)))
        .filter(col(eventTimestamp).>=(lastOutputDoneTimestamp))
        .filter(col(eventTimestamp).<(lastDeltaDoneTimestamp))
      writeToOutput(deltaDfAfterLastOuputDone, lastOutputDoneAndDelay._1.format(dtFormatter))

      // generate done file for output table
      generateOutputDoneFile(lastDeltaDoneAndDelay, lastOutputDoneAndDelay)
    }
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
