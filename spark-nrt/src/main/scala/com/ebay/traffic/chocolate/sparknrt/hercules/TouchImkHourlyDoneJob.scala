package com.ebay.traffic.chocolate.sparknrt.hercules

import java.io.ByteArrayOutputStream
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDateTime}
import java.util.TimeZone

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import org.apache.hadoop.fs.{FileStatus, Path}

import scala.collection.immutable

/**
  * Touch imk done if all data has been copied to hdfs successfully.
  *
  * @author Zhiyuan Wang
  * @since 2019-06-19
  */
object TouchImkHourlyDoneJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new TouchImkHourlyDoneJob(params)
    job.run()
    job.stop()
  }
}

class TouchImkHourlyDoneJob(params: Parameter)
  extends BaseSparkNrtJob(params.appName, params.mode) {

  lazy val dayFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  lazy val doneFileDatetimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH")

  lazy val doneFilePrefix = "imk_rvr_trckng_event_hourly.done."
  lazy val doneFilePostfix = "00000000"

  lazy val doneDir: String = params.doneDir

  lazy val lagDir: String = params.lagDir

  override def run(): Unit = {
    val time = LocalDateTime.now()

    val now = time.truncatedTo(ChronoUnit.HOURS)

    val todayDoneDir = new Path(getDoneDir(now))
    val yesterdayDoneDir = new Path(getDoneDir(now.minusDays(1)))

    var delays = 0L

    if (fs.exists(todayDoneDir) && fs.listStatus(todayDoneDir).length != 0) {
      val todayLastDoneFileDatetime = getLastDoneFileDatetime(fs.listStatus(todayDoneDir))
      delays = ChronoUnit.HOURS.between(todayLastDoneFileDatetime, now)
    } else {
      fs.mkdirs(todayDoneDir)
      val yesterdayLastDoneFileDatetime = getLastDoneFileDatetime(fs.listStatus(yesterdayDoneDir))
      delays = ChronoUnit.HOURS.between(yesterdayLastDoneFileDatetime, now)
    }

    val times: immutable.Seq[LocalDateTime] = (0L until delays).map(delay => now.minusHours(delay))

    val watermark = getEventWatermark

    times.filter(dateTime => dateTime.isBefore(watermark)).foreach(dateTime => {
      val file = getDoneFileName(dateTime)
      val out = fs.create(new Path(file), true)
      out.close()
    })
  }

  def getDoneDir(dateTime: LocalDateTime): String = {
    doneDir + "/" + dateTime.format(dayFormatter)
  }

  def getLastDoneFileDatetime(fileStatus: Array[FileStatus]): LocalDateTime = {
    implicit def dateTimeOrdering: Ordering[LocalDateTime] = Ordering.fromLessThan(_ isBefore  _)
    fileStatus
      .map(status => status.getPath.getName)
      .map(fileName =>  {
        val str = fileName.substring(doneFilePrefix.length, fileName.length - doneFilePostfix.length)
        LocalDateTime.parse(str, doneFileDatetimeFormatter)
      })
      .max(dateTimeOrdering)
  }

  /**
    * The watermark means that there should be no more events with timestamps older or equal to the watermark
    * @return watermark
    */
  def getEventWatermark: LocalDateTime = {
    implicit def dateTimeOrdering: Ordering[LocalDateTime] = Ordering.fromLessThan(_ isBefore  _)
    fs.listStatus(new Path(lagDir))
      .map(status => status.getPath)
      .map(path => readFileContent(path).toLong)
      .map(ts => LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), TimeZone.getDefault.toZoneId))
      .min(dateTimeOrdering)
  }

  /**
    * Construct done file name
    * @param doneFileDatetime done file datetime
    * @return done file name eg. imk_rvr_trckng_event_hourly.done.201904251100000000
    */
  def getDoneFileName(doneFileDatetime: LocalDateTime): String = {
    getDoneDir(doneFileDatetime) + "/" + doneFilePrefix + doneFileDatetime.format(doneFileDatetimeFormatter) + doneFilePostfix
  }

  /**
    * Read hdfs file and return file content
    * @param path hdfs path
    * @return file content
    */
  def readFileContent(path: Path): String = {
    val in = fs.open(path)
    val out = new ByteArrayOutputStream()
    val buffer = new Array[Byte](1024)
    var n = 0
    while(n > -1) {
      n = in.read(buffer)
      if(n > 0) {
        out.write(buffer, 0, n)
      }
    }
    in.close()
    out.toString
  }
}
