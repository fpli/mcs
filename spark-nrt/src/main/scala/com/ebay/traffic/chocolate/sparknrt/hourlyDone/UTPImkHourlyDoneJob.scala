/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.hourlyDone

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{ZoneId, ZonedDateTime}

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.spark.DataFrameFunctions._
import com.ebay.traffic.chocolate.sparknrt.utils.Utils.simpleUid
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object UTPImkHourlyDoneJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new UTPImkHourlyDoneJob(params)
    job.run()
    job.stop()
  }
}

class UTPImkHourlyDoneJob(params: Parameter, override val enableHiveSupport: Boolean = true)
  extends BaseSparkJob(params.appName, params.mode, true) {
  lazy val inputSource: String = params.inputSource
  lazy val cacheTable: String = params.cacheTable
  lazy val cacheDir: String = params.cacheDir
  lazy val doneFileDir: String = params.doneFileDir
  lazy val doneFilePrefix: String = params.doneFilePrefix
  lazy val jobDir: String = params.jobDir + simpleUid() + "/"
  lazy val doneFilePostfix = "00000000"
  lazy val rvrId = "rvr_id"
  lazy val cacheRvrId = "cache_rvr_id"

  @transient lazy val defaultZoneId: ZoneId = ZoneId.systemDefault()
  @transient lazy val dayFormatterInDoneFileName: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(defaultZoneId)
  @transient lazy val doneFileDatetimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(defaultZoneId)
  @transient lazy val cachePrefixDatetimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm").withZone(defaultZoneId)
  @transient lazy val dtFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  @transient lazy val eventTsFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

  implicit def dateTimeOrdering: Ordering[ZonedDateTime] = Ordering.fromLessThan(_ isBefore  _)

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
      .filter(fileName => fileName.contains(doneFilePrefix))
      .map(fileName =>  {
        val str = fileName.substring(doneFilePrefix.length, fileName.length - doneFilePostfix.length)
        ZonedDateTime.parse(str, doneFileDatetimeFormatter)
      })
      .max(dateTimeOrdering)
  }

  /**
    * Get last done file date time. Limitation: when there is delay cross 2 days, this will fail.
    * @param dateTime input date time
    * @return last date time the table ever touched done file and the delay hours
    */
  def getLastDoneFileDateTimeAndDelay(dateTime: ZonedDateTime, doneDir: String): (ZonedDateTime, Long) = {
    // the done if there is no delay,
    val idealDone = dateTime.truncatedTo(ChronoUnit.HOURS).minusHours(1)

    val todayDoneDir = new Path(getDoneDir(idealDone, doneDir))
    val yesterdayDoneDir = new Path(getDoneDir(idealDone.minusDays(1),doneDir))

    logger.info("idealDone {}, todayDoneDir {}, yesterdayDoneDir {}", idealDone, todayDoneDir, yesterdayDoneDir)

    var lastDone: ZonedDateTime = idealDone
    var delays = 0L

    // if today done file doesn't exist, check yesterday
    if (fs.exists(todayDoneDir) && fs.listStatus(todayDoneDir).length != 0) {
      val todayDoneFiles = fs.listStatus(todayDoneDir)
        .map(status => status.getPath.getName)
        .filter(fileName => fileName.contains(doneFilePrefix))
      if (!todayDoneFiles.isEmpty) {
        lastDone = getLastDoneFileDatetimeFromDoneFiles(fs.listStatus(todayDoneDir))
      } else {
        fs.mkdirs(todayDoneDir)
        lastDone = getLastDoneFileDatetimeFromDoneFiles(fs.listStatus(yesterdayDoneDir))
      }
    } else {
      fs.mkdirs(todayDoneDir)
      lastDone = getLastDoneFileDatetimeFromDoneFiles(fs.listStatus(yesterdayDoneDir))
    }
    delays = ChronoUnit.HOURS.between(lastDone, idealDone)

    logger.info("lastDone {}, delays {}", lastDone, delays)

    (lastDone, delays)
  }

  /**
    * Read everything need from the source table. Override this function for domain specific tables.
    * The input is the date time of done file. So that we need to plus one hour to get correct timestamp.
    * @param lastDone input date time
    */
  def readSource(lastDone: ZonedDateTime): DataFrame = {
    //plus 1 hour as done file logic
    val fromDateTime = lastDone.plusHours(1)
    val fromDateString = fromDateTime.format(dtFormatter)
    val fromEventTsString = fromDateTime.format(eventTsFormatter)
    val sql = "select rvr_id, dt, event_ts, rvr_chnl_type_cd, rvr_cmnd_type_cd from %s where dt >= '%s' and event_ts >='%s'".format(inputSource, fromDateString, fromEventTsString)
    logger.info("sqlToSelectSource: " + sql)
    val sourceDf = sqlsc.sql(sql)
    sourceDf
  }

  def readCache(): DataFrame = {
    val sql = "select rvr_id, dt, event_ts, rvr_chnl_type_cd, rvr_cmnd_type_cd from %s".format(cacheTable)
    logger.info("sqlToSelectCache: " + sql)
    val sourceDf = sqlsc.sql(sql)
    sourceDf
  }

  def updateCache(currentDf: DataFrame, table: String, path: String, inputDateTime: ZonedDateTime): Unit = {
    val newSnapshotPath = path + "/" + cachePrefixDatetimeFormatter.format(inputDateTime) + "_" + inputDateTime.toInstant.toEpochMilli
    val tmpPath = (new Path(newSnapshotPath)).toString + "tmp"
    saveDFToFiles(currentDf.repartition(params.partitions), tmpPath)
    fs.delete(new Path(newSnapshotPath), true)
    fs.rename(new Path(tmpPath), new Path(newSnapshotPath))

    val sql = "alter table %s set location '%s'".format(table, newSnapshotPath)
    logger.info("sqlToExecute: " + sql)
    sqlsc.sql(sql)
  }

  def clearExpiredCache(latestDone: Option[ZonedDateTime], cacheDir: String): Unit = {
    if (latestDone.isEmpty) {
      return
    }
    fs.listStatus(new Path(cacheDir))
      .map(status => status.getPath)
      .map(path =>  {
        val cacheDatetime = ZonedDateTime.parse(path.getName.split("_")(0), cachePrefixDatetimeFormatter)
        if (latestDone.get.minusDays(3).isAfter(cacheDatetime)) {
          logger.info("delete expired cache " + path)
          fs.delete(path, true)
        }
      })
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
    * Generate done files
    * @param diffDf the input source from master table
    * @param lastDoneAndDelay last done datetime and the delayed hours
    * @param inputDateTime input datetime, it should be now
    */
  def generateHourlyDoneFile(diffDf: DataFrame, lastDoneAndDelay: (ZonedDateTime, Long), inputDateTime: ZonedDateTime): Option[ZonedDateTime] = {
    // generate done file
    if (diffDf.rdd.isEmpty) {
      return None
    }
    val minEventTs = diffDf.agg(min("event_ts")).head().getString(0)
    val minDateTime = ZonedDateTime.parse(minEventTs, eventTsFormatter.withZone(defaultZoneId))
    val lastDoneThisTime = minDateTime.truncatedTo(ChronoUnit.HOURS).minusHours(1)
    logger.info("minEventTs {}, minDateTime {}, lastDoneThisTime {}", minEventTs, minDateTime, lastDoneThisTime)
    val times = (1L to ChronoUnit.HOURS.between(lastDoneAndDelay._1, lastDoneThisTime))
      .map(delay => lastDoneAndDelay._1.plusHours(delay))
    if (times.isEmpty) {
      return None
    }
    val doneFilesShouldBeGenerated = times.map(dateTime => getDoneFileName(dateTime, doneFileDir))
    logger.info("below done file should be generated {} ", doneFilesShouldBeGenerated)
    doneFilesShouldBeGenerated.foreach(file => {
      logger.info("touch hourly done file {}", file)
      val out = fs.create(new Path(file), true)
      out.close()
    })
    Some(times.last)
  }

  /**
    * Update done files
    * @param inputDateTime input date time. It should be now.
    */
  def updateDoneFiles(inputDateTime: ZonedDateTime): Unit = {
    val lastDoneAndDelay = getLastDoneFileDateTimeAndDelay(inputDateTime, doneFileDir)

    // df cached by last job
    val cacheDf = readCache().withColumnRenamed(rvrId, cacheRvrId)

    // source df after last done timestamp
    val sourceDf = readSource(lastDoneAndDelay._1)

    // diff diff, must cache!!
    val diffDf = sourceDf
      .join(cacheDf, col(rvrId).===(col(cacheRvrId)), "left_anti")
      .cache(this, params.jobDir + "/diffDf")

    val latestDone = generateHourlyDoneFile(diffDf, lastDoneAndDelay, inputDateTime)

    updateCache(sourceDf, cacheTable, cacheDir, inputDateTime)

    clearExpiredCache(latestDone, cacheDir)
  }

  /**
    * Entry of this spark job
    */
  override def run(): Unit = {
    val now = ZonedDateTime.now(defaultZoneId)
    updateDoneFiles(now)
  }
}
