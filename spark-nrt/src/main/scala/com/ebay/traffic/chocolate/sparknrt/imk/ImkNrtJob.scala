/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.imk

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.time.temporal.ChronoUnit

import com.ebay.traffic.chocolate.sparknrt.basenrt.BaseNrtJob
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.ebay.traffic.chocolate.spark.DataFrameFunctions._

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

class ImkNrtJob(params: Parameter, override val enableHiveSupport: Boolean = true)
  extends BaseNrtJob(params.appName, params.mode, true) {

  lazy val inputSource: String = params.inputSource
  lazy val imkDeltaDir: String = params.deltaDir
  lazy val imkOutputDir: String = params.outPutDir
  lazy val doneFileDir: String = params.doneFileDir
  lazy val doneFilePrefix: String = params.doneFilePrefix
  lazy val doneFilePostfix = "00000000"
  lazy val snapshotid = "snapshotid"
  lazy val dt = "dt"

  lazy val defaultZoneId: ZoneId = ZoneId.systemDefault()
  lazy val dayFormatterInDoneFileName: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(defaultZoneId)
  lazy val doneFileDatetimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(defaultZoneId)
  lazy val dtFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")

  implicit def dateTimeOrdering: Ordering[ZonedDateTime] = Ordering.fromLessThan(_ isBefore  _)

  import spark.implicits._

  /**
    * Get done file by date
    * @param dateTime input date time
    * @return done file dir by date
    */
  def getDoneDir(dateTime: ZonedDateTime): String = {
    doneFileDir + "/" + dateTime.format(dayFormatterInDoneFileName)
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
    * @return last date time the delta table ever touched done file and the delay hours
    */
  def getLastDoneFileDateTimeAndDelay(dateTime: ZonedDateTime): (ZonedDateTime, Long) = {

    val doneDateHour = dateTime.truncatedTo(ChronoUnit.HOURS)

    val todayDoneDir = new Path(getDoneDir(doneDateHour))
    val yesterdayDoneDir = new Path(getDoneDir(doneDateHour.minusDays(1)))

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
    val fromDateTime = getLastDoneFileDateTimeAndDelay(inputDateTime)._1
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
  def getDoneFileName(doneFileDatetime: ZonedDateTime): String = {
    getDoneDir(doneFileDatetime) + "/" + doneFilePrefix + doneFileDatetime.format(doneFileDatetimeFormatter) + doneFilePostfix
  }

  /**
    * Generate done files of delta table
    * @param diffDf the input source from master table
    * @param lastDoneAndDelay last done datetime and the delayed hours
    * @param inputDateTime input datetime, it should be now
    */
  def generateDoneFile(diffDf: DataFrame, lastDoneAndDelay: (ZonedDateTime, Long), inputDateTime: ZonedDateTime): Unit = {
    // generate done file
    diffDf.show()
    val minTs = diffDf.agg(min("eventtimestamp")).head().getLong(0)
    val instant = Instant.ofEpochMilli(minTs)
    val minDateTime = ZonedDateTime.ofInstant(instant, ZoneId.systemDefault())

    val delays = lastDoneAndDelay._2
    val times: immutable.Seq[ZonedDateTime] = (0L until delays)
      .map(delay => inputDateTime.minusHours(delay))
      .reverse
      .filter(dateTime => dateTime.plusHours(1).isBefore(minDateTime))

    times.foreach(dateTime => {
      val file = getDoneFileName(dateTime)
      logger.info("touch done file {}", file)
      val out = fs.create(new Path(file), true)
      out.close()
    })
  }

  /**
    * Update the delta lake table
    * @param inputDateTime input date time. It should be now.
    */
  def updateDelta(inputDateTime: ZonedDateTime): Unit = {

    val lastDoneAndDelay = getLastDoneFileDateTimeAndDelay(inputDateTime)

    val imkDelta = DeltaTable.forPath(spark, imkDeltaDir)
    imkDelta.toDF.show()

    // get new upserted records dataframe
    // get last done timestamp
    val lastDoneTimestamp = lastDoneAndDelay._1.toEpochSecond * 1000L
    // delta table after last done timestamp, must cache!!
    val imkDeltaAfterLastDone = imkDelta.toDF
      .filter($"eventtimestamp" >= lastDoneTimestamp)
      .withColumnRenamed("snapshotid", "delta_snapshotid")
      .cache()
    println("imkDeltaAfterLastDone")
    imkDeltaAfterLastDone.show()

    // source df after last done timestamp, don't need cache, since it won't change
    val sourceDf = readSource(lastDoneAndDelay._1).cache()
    println("sourceDf")
    sourceDf.show()

    // diff diff, must cache!!
    val diffDf = sourceDf
      .join(imkDeltaAfterLastDone, $"snapshotid" === $"delta_snapshotid", "left_anti")
      .cache(this, params.jobDir + "/diffDf")
    println("diffDf")
    diffDf.show()

    // when there are new records, upsert the records
    imkDelta.as("delta")
      .merge(sourceDf.as("updates"),
      s"delta.${snapshotid} = updates.${snapshotid} and delta.${dt} = updates.${dt}")
      .whenNotMatched()
      .insertAll()
      .execute()


    generateDoneFile(diffDf, lastDoneAndDelay, inputDateTime)
  }

  /**
    * Update final target table
    */
  def updateOutput(): Unit = {

  }

  /**
    * Entry of this spark job
    */
  override def run(): Unit = {

  }
}
