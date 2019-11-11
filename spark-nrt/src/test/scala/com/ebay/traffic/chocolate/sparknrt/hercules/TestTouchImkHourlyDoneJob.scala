package com.ebay.traffic.chocolate.sparknrt.hercules

import java.io.File
import java.time.{Duration, Instant, Period, ZonedDateTime}
import java.time.temporal.ChronoUnit

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * @author Zhiyuan Wang
  * @since 2019-06-19
  */
class TestTouchImkHourlyDoneJob extends BaseFunSuite {
  private val tmpPath = createTempDir()
  private val workDir = tmpPath + "/workDir"
  private val lagDir = tmpPath + "/calCrabTransformWatermark"
  private val doneDir = tmpPath + "/doneDir"

  var job: TouchImkHourlyDoneJob = _

  @transient private lazy val hadoopConf = {
    new Configuration()
  }

  private lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  override def beforeAll(): Unit = {
    val args = Array(
      "--mode", "local[8]",
      "--workDir", workDir,
      "--lagDir", lagDir,
      "--doneDir", doneDir
    )
    val params = Parameter(args)
    job = new TouchImkHourlyDoneJob(params)
  }

  test("testRun") {
    // create temp done file, last done was generated yesterday 12 am
    val now = ZonedDateTime.now(job.defaultZoneId)
    val doneDateHour = now.truncatedTo(ChronoUnit.HOURS).minusHours(1)

    val yesterdayDoneDir = new Path(job.getDoneDir(doneDateHour.minusDays(1)))

    fs.mkdirs(yesterdayDoneDir)

    val lastDoneDateTime = doneDateHour.minusDays(1).withHour(12)
    val doneOut = fs.create(new Path(yesterdayDoneDir + "/" + "imk_rvr_trckng_event_hourly.done." + lastDoneDateTime.format(job.doneFileDatetimeFormatter) + "00000000"), true)
    doneOut.close()

    // create temp lag file, lag ts is today 12:10
    fs.mkdirs(new Path(lagDir))
    val lagOut = fs.create(new Path(lagDir + "/" + "imkCrabTransformWatermark"), true)
    lagOut.writeBytes(String.valueOf(doneDateHour.withHour(12).withMinute(10).toInstant.toEpochMilli))
    lagOut.close()

    job.run()

    val hours = Duration.between(lastDoneDateTime, doneDateHour).toHours.toInt + 1
    for (i <- 1 until hours)  {
      val time = lastDoneDateTime.plusHours(i)
      val file = job.getDoneDir(time) + "/" + "imk_rvr_trckng_event_hourly.done." + time.format(job.doneFileDatetimeFormatter) + "00000000"
      val bool = fs.exists(new Path(file))
      assert(bool)
    }

    fs.delete(new Path(lagDir), true)
    fs.delete(new Path(doneDir), true)
  }

  test("testGetDoneDir") {
    val time = ZonedDateTime.of(2019, 6, 19, 0, 0, 0, 0, job.defaultZoneId)
    assert(job.getDoneDir(time).split("/").last.equals("20190619"))
  }

  test("testGetDoneFileName") {
    val time = ZonedDateTime.of(2019, 6, 19, 1, 0, 0, 0, job.defaultZoneId)
    assert(job.getDoneFileName(time).split("/").last.equals("imk_rvr_trckng_event_hourly.done.201906190100000000"))
  }

  test("testGetLastDoneFileDatetime") {
    fs.delete(new Path(doneDir), true)

    val file1 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906191900000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(doneDir + "/imk_rvr_trckng_event_hourly.done.201906191900000000"))
    val file2 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906192000000000")
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(doneDir + "/imk_rvr_trckng_event_hourly.done.201906192000000000"))

    val actual = job.getLastDoneFileDatetime(fs.listStatus(new Path(doneDir)))
    val expect = ZonedDateTime.of(2019, 6, 19, 20, 0, 0, 0, job.defaultZoneId).truncatedTo(ChronoUnit.HOURS)
    assert(actual.equals(expect))

    fs.delete(new Path(lagDir), true)
    fs.delete(new Path(doneDir), true)
  }

  test("testGetEventWatermark") {
    val file1 = new File("src/test/resources/touchImkHourlyDone.data/calCrabTransformWatermark/imkCrabTransformWatermark")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(lagDir + "/imkCrabTransformWatermark"))
    val file2 = new File("src/test/resources/touchImkHourlyDone.data/calCrabTransformWatermark/crabTransformWatermark")
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(lagDir + "/crabTransformWatermark"))

    val actual = job.getEventWatermark
    val expect = ZonedDateTime.ofInstant(Instant.ofEpochMilli(1560859857002L), job.defaultZoneId)
    assert(actual.equals(expect))

    fs.delete(new Path(lagDir), true)
    fs.delete(new Path(doneDir), true)
  }

  test("testReadFileContent") {
    val file = new File("src/test/resources/touchImkHourlyDone.data/calCrabTransformWatermark/imkCrabTransformWatermark")
    fs.copyFromLocalFile(new Path(file.getAbsolutePath), new Path(lagDir + "/imkCrabTransformWatermark"))
    assert(job.readFileContent(new Path(lagDir + "/imkCrabTransformWatermark")).equals("1560946257001"))

    fs.delete(new Path(lagDir), true)
    fs.delete(new Path(doneDir), true)
  }
}