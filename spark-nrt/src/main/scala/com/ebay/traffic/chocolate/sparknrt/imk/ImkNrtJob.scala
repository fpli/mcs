/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.imk

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZonedDateTime}
import java.time.temporal.ChronoUnit

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.basenrt.BaseNrtJob
import com.ebay.traffic.chocolate.sparknrt.imkETL.Parameter
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.functions._


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

class ImkNrtJob(params: Parameter) extends BaseNrtJob(params.appName, params.mode) {

  lazy val inputSource: String = params.inputSource
  lazy val imkDeltaDir: String = params.deltaDir + "imk"
  lazy val imkOutputDir: String = params.outPutDir
  lazy val doneFileDir: String = params.doneFileDir
  lazy val doneFilePrefix: String = params.doneFilePrefix
  lazy val doneFilePostfix = "00000000"

  lazy val defaultZoneId: ZoneId = ZoneId.systemDefault()
  lazy val dayFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(defaultZoneId)
  lazy val doneFileDatetimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(defaultZoneId)

  implicit def dateTimeOrdering: Ordering[ZonedDateTime] = Ordering.fromLessThan(_ isBefore  _)

  /**
    * Get done file by date
    * @param dateTime input date time
    * @return done file dir by date
    */
  def getDoneDir(dateTime: ZonedDateTime): String = {
    doneFileDir + "/" + dateTime.format(dayFormatter)
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
    * Get last done file date time
    * @return last date time the delta table ever touched done file
    */
  def getLastDoneFileDateTime(dateTime: ZonedDateTime): ZonedDateTime = {

    val doneDateHour = dateTime.truncatedTo(ChronoUnit.HOURS).minusHours(1)

    val todayDoneDir = new Path(getDoneDir(doneDateHour))
    val yesterdayDoneDir = new Path(getDoneDir(doneDateHour.minusDays(1)))

    logger.info("doneDateHour {}, todayDoneDir {}, yesterdayDoneDir {}", doneDateHour, todayDoneDir, yesterdayDoneDir)

    var lastDoneFileDatetime: ZonedDateTime = doneDateHour

    // if today done file dir already exist, just check today's done
    if (fs.exists(todayDoneDir) && fs.listStatus(todayDoneDir).length != 0) {
      lastDoneFileDatetime = getLastDoneFileDatetimeFromDoneFiles(fs.listStatus(todayDoneDir))
    } else {
      fs.mkdirs(todayDoneDir)
      lastDoneFileDatetime = getLastDoneFileDatetimeFromDoneFiles(fs.listStatus(yesterdayDoneDir))
    }

    lastDoneFileDatetime
  }

  /**
    * Read everything need from the source table
    */
  def readSource(): Unit = {

  }

  /**
    * Update the delta lake table
    */
  def updateDelta(): Unit = {

  }

  /**
    * Generate done files of delta table
    */
  def generateDoneFile(): Unit = {

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

    val file = new File("/tmp/delta")

    val path = file.getCanonicalPath
    println(path)
    val data = spark.range(0,5)
    data.printSchema()
    data.toDF()

    saveDFToFiles(data.toDF(), path, outputFormat = "delta")

    val imkDelta = DeltaTable.forPath(spark, path)
    imkDelta.toDF.show()
    val dfImk = imkDelta
      .update(
        col("id") < 5,
        Map("id" -> lit(1)))
    DeltaTable.forPath(spark, path).toDF.show()
  }
}
