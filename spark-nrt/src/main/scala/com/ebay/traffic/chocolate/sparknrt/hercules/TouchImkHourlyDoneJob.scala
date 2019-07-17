package com.ebay.traffic.chocolate.sparknrt.hercules

import java.io.ByteArrayOutputStream
import java.net.URI
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Instant, ZoneId, ZonedDateTime}

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

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

  @transient lazy val lvsFs: FileSystem = {
    val fs = FileSystem.get(URI.create(params.lagDir), hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  @transient override lazy val fs: FileSystem = {
    val fs = FileSystem.get(URI.create(params.workDir), hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  lazy val defaultZoneId: ZoneId = ZoneId.systemDefault()

  lazy val dayFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(defaultZoneId)
  lazy val doneFileDatetimeFormatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(defaultZoneId)

  lazy val doneFilePrefix = "imk_rvr_trckng_event_hourly.done."
  lazy val doneFilePostfix = "00000000"

  lazy val doneDir: String = params.doneDir

  lazy val lagDir: String = params.lagDir

  implicit def dateTimeOrdering: Ordering[ZonedDateTime] = Ordering.fromLessThan(_ isBefore  _)

  override def run(): Unit = {
    // if current time is 6 o'clock, try to generate 5 o'clock done
    val doneDateHour = ZonedDateTime.now(defaultZoneId).truncatedTo(ChronoUnit.HOURS).minusHours(1)

    val todayDoneDir = new Path(getDoneDir(doneDateHour))
    val yesterdayDoneDir = new Path(getDoneDir(doneDateHour.minusDays(1)))

    logger.info("doneDateHour {}, todayDoneDir {}, yesterdayDoneDir {}", doneDateHour, todayDoneDir, yesterdayDoneDir)

    var delays = 0L

    if (fs.exists(todayDoneDir) && fs.listStatus(todayDoneDir).length != 0) {
      val todayLastDoneFileDatetime = getLastDoneFileDatetime(fs.listStatus(todayDoneDir))
      delays = ChronoUnit.HOURS.between(todayLastDoneFileDatetime, doneDateHour)
      logger.info("todayLastDoneFileDatetime {}", todayLastDoneFileDatetime)
    } else {
      fs.mkdirs(todayDoneDir)
      val yesterdayLastDoneFileDatetime = getLastDoneFileDatetime(fs.listStatus(yesterdayDoneDir))
      delays = ChronoUnit.HOURS.between(yesterdayLastDoneFileDatetime, doneDateHour)
      logger.info("yesterdayLastDoneFileDatetime {}", yesterdayLastDoneFileDatetime)
    }

    val watermark = getEventWatermark

    logger.info("delays {}, watermark {}", delays, watermark)

    val times: immutable.Seq[ZonedDateTime] = (0L until delays).map(delay => doneDateHour.minusHours(delay)).reverse.filter(dateTime => dateTime.plusHours(1).isBefore(watermark))

    times.foreach(dateTime => {
      val file = getDoneFileName(dateTime)
      logger.info("touch done file {}", file)
      val out = fs.create(new Path(file), true)
      out.close()
    })
  }

  def getDoneDir(dateTime: ZonedDateTime): String = {
    doneDir + "/" + dateTime.format(dayFormatter)
  }

  def getLastDoneFileDatetime(fileStatus: Array[FileStatus]): ZonedDateTime = {
    fileStatus
      .map(status => status.getPath.getName)
      .map(fileName =>  {
        val str = fileName.substring(doneFilePrefix.length, fileName.length - doneFilePostfix.length)
        ZonedDateTime.parse(str, doneFileDatetimeFormatter)
      })
      .max(dateTimeOrdering)
  }

  /**
    * The watermark means that there should be no more events with timestamps older or equal to the watermark
    * @return watermark
    */
  def getEventWatermark: ZonedDateTime = {
    lvsFs.listStatus(new Path(lagDir))
      .map(status => status.getPath)
      .map(path => readFileContent(path).toLong)
      .map(ts => ZonedDateTime.ofInstant(Instant.ofEpochMilli(ts), defaultZoneId))
      .min(dateTimeOrdering)
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
    * Read hdfs file and return file content
    * @param path hdfs path
    * @return file content
    */
  def readFileContent(path: Path): String = {
    val in = lvsFs.open(path)
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
    out.toString.trim
  }
}
