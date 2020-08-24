/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.imk

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
  * Imk NRT job to extract data from master table and sink into IMK table
  */
object ImkNrtJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new ImkNrtJob(params)

    job.run()
    job.stop()
  }
}

/**
  * IMK NRT job
  * @param params input parameters
  * @param enableHiveSupport enable hive support for spark sql table query
  */
class ImkNrtJob(params: Parameter, override val enableHiveSupport: Boolean = true)
  extends BaseNrtJob(params.appName, params.mode, true) {

  lazy val inputSource: String = params.inputSource
  lazy val imkDeltaDir: String = params.deltaDir
  lazy val imkOutputDir: String = params.outPutDir
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

  lazy val defaultZoneId: ZoneId = ZoneId.systemDefault()
  lazy val dayFormatterInDoneFileName: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(defaultZoneId)
  lazy val doneFileDatetimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(defaultZoneId)
  lazy val dtFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  implicit def dateTimeOrdering: Ordering[ZonedDateTime] = Ordering.fromLessThan(_ isBefore  _)

  import spark.implicits._

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
    * Read everything need from the source table
    * @param inputDateTime input date time
    */
  def readSource(inputDateTime: ZonedDateTime): DataFrame = {
    val fromDateTime = getLastDoneFileDateTimeAndDelay(inputDateTime, deltaDoneFileDir)._1
    val fromDateString = fromDateTime.format(dtFormatter)
    val startTimestamp = fromDateTime.toEpochSecond * 1000
    val sql = "select snapshotid, eventtimestamp, channeltype, channelaction, dt from " + inputSource + " where dt >= '" + fromDateString + "' and eventtimestamp >='" + startTimestamp +"'"
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
    val minTs = diffDf.agg(min(eventTimestamp)).head().getLong(0)
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

    val imkDelta = DeltaTable.forPath(spark, imkDeltaDir)

    // get new upserted records dataframe
    // get last done timestamp
    val lastDoneTimestamp = lastDoneAndDelay._1.toEpochSecond * multiplierForMs
    // delta table after last done timestamp
    val imkDeltaAfterLastDone = imkDelta.toDF
      .filter(col(eventTimestamp).>=(lastDoneTimestamp))
      .withColumnRenamed(snapshotid, deltaSnapshotid)

    // source df after last done timestamp, don't need cache, since it won't change
    val sourceDf = readSource(lastDoneAndDelay._1)

    // diff diff, must cache!!
    val diffDf = sourceDf
      .join(imkDeltaAfterLastDone, col(snapshotid).===(col(deltaSnapshotid)), "left_anti")
      .cache(this, params.jobDir + "/diffDf")

    // when there are new records, upsert the records
    imkDelta.as("delta")
      .merge(sourceDf.as("updates"),
      s"delta.${snapshotid} = updates.${snapshotid} and delta.${dt} = updates.${dt}")
      .whenNotMatched()
      .insertAll()
      .execute()

    generateDeltaDoneFile(diffDf, lastDoneAndDelay, inputDateTime)
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
      val imkDelta = DeltaTable.forPath(spark, imkDeltaDir)

      val lastOutputDoneTimestamp = lastOutputDoneAndDelay._1.toEpochSecond * multiplierForMs
      val lastDeltaDoneTimestamp = lastDeltaDoneAndDelay._1.toEpochSecond * multiplierForMs

      // same day
      if (lastOutputDoneAndDelay._1.getDayOfYear.equals(lastDeltaDoneAndDelay._1.getDayOfYear)) {
        // delta df between 2 done file
        val imkDeltaAfterLastOuputDone = imkDelta.toDF
          .filter(col(eventTimestamp).>=(lastOutputDoneTimestamp))
          .filter(col(eventTimestamp).<(lastDeltaDoneTimestamp))

        // save to final output
        this.saveDFToFiles(imkDeltaAfterLastOuputDone, imkOutputDir + "/"
          + dt + "=" + lastOutputDoneAndDelay._1.format(dtFormatter), writeMode = SaveMode.Append)
      }
      // cross day
      else {
        val startTimestampOfTomorrow = lastOutputDoneAndDelay._1.plusDays(1)
          .toLocalDate.atStartOfDay(defaultZoneId)
          .toEpochSecond * multiplierForMs

        val imkDeltaAfterLastOuputDoneSameDay = imkDelta.toDF
          .filter(col(eventTimestamp).>=(lastOutputDoneTimestamp))
          .filter(col(eventTimestamp).<(startTimestampOfTomorrow))
        // save to final output, same day
        this.saveDFToFiles(imkDeltaAfterLastOuputDoneSameDay, imkOutputDir + "/"
          + dt + "=" + lastOutputDoneAndDelay._1.format(dtFormatter), writeMode = SaveMode.Append)

        val imkDeltaAfterLastOuputDoneCrossDay = imkDelta.toDF
          .filter(col(eventTimestamp)>=startTimestampOfTomorrow)
          .filter(col(eventTimestamp).<(lastDeltaDoneTimestamp))
        // save to final output, cross day
        this.saveDFToFiles(imkDeltaAfterLastOuputDoneCrossDay, imkOutputDir + "/"
          + dt + "=" + lastDeltaDoneAndDelay._1.format(dtFormatter), writeMode = SaveMode.Append)
      }

      // generate done file for output table
      generateOutputDoneFile(lastDeltaDoneAndDelay, lastOutputDoneAndDelay)
    }

  }

  /**
    * Entry of this spark job
    */
  override def run(): Unit = {

  }
}
