/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.hourlyDone

import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneId, ZonedDateTime}

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.spark.DataFrameFunctions._
import com.ebay.traffic.chocolate.sparknrt.utils.Utils
import com.ebay.traffic.chocolate.sparknrt.utils.Utils.simpleUid
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.yaml.snakeyaml.Yaml

import scala.collection.mutable

object UTPHourlyDoneJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new UTPHourlyDoneJob(params)
    job.run()
    job.stop()
  }
}

class UTPHourlyDoneJob(params: Parameter, override val enableHiveSupport: Boolean = true)
  extends BaseSparkJob(params.appName, params.mode, true) {
  lazy val inputSource: String = params.inputSource
  lazy val cacheTable: String = params.cacheTable
  lazy val cacheDir: String = params.cacheDir
  lazy val doneFileDir: String = params.doneFileDir
  lazy val doneFilePrefix: String = params.doneFilePrefix
  lazy val jobDir: String = params.jobDir + simpleUid() + "/"
  lazy val doneFilePostfix = "00000000"
  lazy val eventId = "eventId"
  lazy val cacheEventId = "cacheEventId"

  @transient lazy val defaultZoneId: ZoneId = ZoneId.systemDefault()
  @transient lazy val dayFormatterInDoneFileName: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(defaultZoneId)
  @transient lazy val doneFileDatetimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(defaultZoneId)
  @transient lazy val cachePrefixDatetimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm").withZone(defaultZoneId)
  @transient lazy val dtFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(defaultZoneId)
  @transient lazy val datetimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(defaultZoneId)
  @transient lazy val yaml = new Yaml()
  @transient lazy val doneFileConfig: java.util.List[java.util.Map[String, String]] = yaml.load(getClass.getClassLoader.getResourceAsStream("done_file_config.yaml")).asInstanceOf[java.util.List[java.util.Map[String,String]]]

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
  def getLastDoneFileDatetimeFromDoneFiles(fileStatus: Array[FileStatus], doneFilePrefix: String): ZonedDateTime = {
    fileStatus
      .map(status => status.getPath.getName)
      .filter(fileName => fileName.contains(doneFilePrefix))
      .map(fileName => {
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
    @Deprecated
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
        lastDone = getLastDoneFileDatetimeFromDoneFiles(fs.listStatus(todayDoneDir), doneFilePrefix)
      } else {
        fs.mkdirs(todayDoneDir)
        lastDone = getLastDoneFileDatetimeFromDoneFiles(fs.listStatus(yesterdayDoneDir), doneFilePrefix)
      }
    } else {
      fs.mkdirs(todayDoneDir)
      lastDone = getLastDoneFileDatetimeFromDoneFiles(fs.listStatus(yesterdayDoneDir), doneFilePrefix)
    }
    delays = ChronoUnit.HOURS.between(lastDone, idealDone)

    logger.info("lastDone {}, delays {}", lastDone, delays)

    (lastDone, delays)
  }

  /**
    * get each last done time , delay, condition by done_file_config
    * @param dateTime
    * @param doneDir
    * @return
    */
  def getEachLastDone(dateTime: ZonedDateTime, doneDir: String): mutable.Map[String, (ZonedDateTime, Long)] = {
    // the done if there is no delay,
    val idealDone = dateTime.truncatedTo(ChronoUnit.HOURS).minusHours(1)

    val todayDoneDir = new Path(getDoneDir(idealDone, doneDir))
    val yesterdayDoneDir = new Path(getDoneDir(idealDone.minusDays(1),doneDir))

    logger.info("idealDone {}, todayDoneDir {}, yesterdayDoneDir {}", idealDone, todayDoneDir, yesterdayDoneDir)

    var lastDone: ZonedDateTime = idealDone
    var delays = 0L

    val iterator = doneFileConfig.iterator()
    val result: mutable.Map[String, (ZonedDateTime, Long)] = mutable.Map()
    while (iterator.hasNext) {
      val doneConfig = iterator.next()
      val prefix = doneConfig.get("doneFilePrefix")

      // if today done file doesn't exist, check yesterday
      if (fs.exists(todayDoneDir) && fs.listStatus(todayDoneDir).length != 0) {
        val todayDoneFiles = fs.listStatus(todayDoneDir)
          .map(status => status.getPath.getName)
          .filter(fileName => {
            fileName.contains(prefix)
          })
        if (!todayDoneFiles.isEmpty) {
          lastDone = getLastDoneFileDatetimeFromDoneFiles(fs.listStatus(todayDoneDir), prefix)
        } else {
          fs.mkdirs(todayDoneDir)
          lastDone = getLastDoneFileDatetimeFromDoneFiles(fs.listStatus(yesterdayDoneDir), prefix)
        }
      } else {
        fs.mkdirs(todayDoneDir)
        lastDone = getLastDoneFileDatetimeFromDoneFiles(fs.listStatus(yesterdayDoneDir), prefix)
      }
      delays = ChronoUnit.HOURS.between(lastDone, idealDone)
      logger.info("doneName : {}, lastDone : {}, delays {}", doneConfig.get("name"), lastDone.toString, delays.toString)
      result += (doneConfig.get("name") -> (lastDone, delays))
    }
    result
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
    val fromProducerEventTs = fromDateTime.toInstant.toEpochMilli
    val sql = "select eventId, dt, hour, producerEventTs, channelType, actionType from %s where dt >= '%s' and producerEventTs >= %d".format(inputSource, fromDateString, fromProducerEventTs)
    logger.info("sqlToSelectSource: " + sql)
    val sourceDf = sqlsc.sql(sql)
    sourceDf
  }

  def readCache(): DataFrame = {
    val sql = "select eventId, dt, hour, producerEventTs, channelType, actionType from %s".format(cacheTable)
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

  def getDoneFileNameByConfig(doneFileDatetime: ZonedDateTime, doneFileDir: String, prefix: String): String = {
    getDoneDir(doneFileDatetime, doneFileDir) + "/" + prefix + doneFileDatetime.format(doneFileDatetimeFormatter) + doneFilePostfix
  }

  private val datetimeUdf = udf((timestamp: Long) => datetime(timestamp))

  def datetime(timestamp: Long): String = {
    ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), defaultZoneId).format(datetimeFormatter)
  }

  /**
    * Generate done files
    * @param diffDf the input source from master table
    * @param lastDoneAndDelay last done datetime and the delayed hours
    * @param inputDateTime input datetime, it should be now
    */
    @Deprecated
  def generateHourlyDoneFile(diffDf: DataFrame, lastDoneAndDelay: (ZonedDateTime, Long), inputDateTime: ZonedDateTime): Option[ZonedDateTime] = {
    // generate done file
    if (diffDf.rdd.isEmpty) {
      return None
    }
    val stat = diffDf.groupBy("channelType", "actionType").agg(min("producerEventTs").as("minProducerEventTs"))
      .select("channelType", "actionType", "minProducerEventTs")
      .withColumn("minProducerEventTime", datetimeUdf(col("minProducerEventTs")))
    logger.info("current status {}", Utils.showString(stat, 100, truncate = false))

    val minRow = diffDf.orderBy(col("producerEventTs").asc).limit(1)
      .withColumn("minProducerEventTime", datetimeUdf(col("producerEventTs")))
    logger.info("min event {}", Utils.showString(minRow, 100, truncate = false))

    val minProducerEventTs = minRow.collectAsList().get(0).getAs[Long]("producerEventTs")
    val minProducerEventTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(minProducerEventTs), defaultZoneId)
    val lastDoneThisTime = minProducerEventTime.truncatedTo(ChronoUnit.HOURS).minusHours(1)
    logger.info("minProducerEventTs %d, minProducerEventTime %s, lastDoneThisTime, %s".format(minProducerEventTs, minProducerEventTime, lastDoneThisTime))
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

  def genHourlyDoneFile(diffDf: DataFrame, lastDoneAndDelay: mutable.Map[String, (ZonedDateTime, Long)]): Option[ZonedDateTime] = {
    // generate done file
    if (diffDf.rdd.isEmpty) {
      return None
    }
    val stat = diffDf.groupBy("channelType", "actionType").agg(min("producerEventTs").as("minProducerEventTs"))
      .select("channelType", "actionType", "minProducerEventTs")
      .withColumn("minProducerEventTime", datetimeUdf(col("minProducerEventTs")))
    logger.info("current status {}", Utils.showString(stat, 100, truncate = false))

    val it = doneFileConfig.iterator()
    var time = Option.empty[ZonedDateTime]
    while (it.hasNext) {
      val conf = it.next()
      time = genDoneFileForEachTeam(diffDf, lastDoneAndDelay, conf)
    }
    time
  }

  def genDoneFileForEachTeam(diffDf: DataFrame, lastDoneAndDelay: mutable.Map[String, (ZonedDateTime, Long)],
                             config: java.util.Map[String, String]): Option[ZonedDateTime] = {

    val minRowByChannel = diffDf.groupBy("channelType").agg(min("producerEventTs").as("minProducerEventTs"))
      .select("channelType", "minProducerEventTs").where(config.get("condition"))
      .orderBy(col("minProducerEventTs"))
      .withColumn("minProducerEventTime", datetimeUdf(col("minProducerEventTs")))

    if (minRowByChannel.rdd.isEmpty()) {
      return None;
    }
    val minEventTs = minRowByChannel.collectAsList().get(0).getAs[Long]("minProducerEventTs")

    val minProducerEventTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(minEventTs), defaultZoneId)

    val lastDoneThisTime = minProducerEventTime.truncatedTo(ChronoUnit.HOURS).minusHours(1)

    logger.info("Config.name : {}, minProducerEventTs : {}, minProducerEventTime : {}, lastDoneThisTime : {}"
      , config.get("name"), minEventTs.toString, minProducerEventTime.toString, lastDoneThisTime.toString)

    val times = (1L to ChronoUnit.HOURS.between(lastDoneAndDelay(config.get("name"))._1, lastDoneThisTime))
      .map(delay => lastDoneAndDelay(config.get("name"))._1.plusHours(delay))
    if (times.isEmpty) {
      return None
    }
    val doneFilesShouldBeGenerated = times.map(dateTime => getDoneFileNameByConfig(dateTime, doneFileDir, config.get("doneFilePrefix")))
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
    val eachLastDone = getEachLastDone(inputDateTime, doneFileDir)

    // df cached by last job
    val cacheDf = readCache().withColumnRenamed(eventId, cacheEventId)

    // source df after last done timestamp
    val sourceDf = readSource(eachLastDone("all")._1)

    // diff diff, must cache!!
    val diffDf = sourceDf
      .join(cacheDf, col(eventId).===(col(cacheEventId)), "left_anti")
      .cache(this, params.jobDir + "/diffDf")

    val latestDone = genHourlyDoneFile(diffDf, eachLastDone)

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
