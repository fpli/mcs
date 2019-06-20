package com.ebay.traffic.chocolate.sparknrt.hercules

import java.io.File
import java.time.LocalDateTime
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
  private val lagDir = tmpPath + "/lagDir"
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

  test("testGetDoneDir") {
    val time = LocalDateTime.of(2019, 6, 19, 0, 0, 0)
    assert(job.getDoneDir(time).split("/").last.equals("2019-06-19"))
  }

  test("testGetDoneFileName") {
    val time = LocalDateTime.of(2019, 6, 19, 1, 0, 0)
    assert(job.getDoneFileName(time).split("/").last.equals("imk_rvr_trckng_event_hourly.done.201906190100000000"))
  }

  test("testGetLastDoneFileDatetime") {
    val file1 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906191900000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(doneDir + "/imk_rvr_trckng_event_hourly.done.201906191900000000"))
    val file2 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906192000000000")
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(doneDir + "/imk_rvr_trckng_event_hourly.done.201906192000000000"))

    val actual = job.getLastDoneFileDatetime(fs.listStatus(new Path(doneDir)))
    val expect = LocalDateTime.of(2019, 6, 19, 20, 0, 0).truncatedTo(ChronoUnit.HOURS)
    assert(actual.equals(expect))
  }

  test("testGetEventWatermark") {
    val file1 = new File("src/test/resources/touchImkHourlyDone.data/lag/0")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(lagDir + "/0"))
    val file2 = new File("src/test/resources/touchImkHourlyDone.data/lag/1")
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(lagDir + "/1"))

    val actual = job.getEventWatermark
    val expect = LocalDateTime.of(2019, 6, 18, 20, 10, 57, 2000000)
    assert(actual.equals(expect))
  }

  test("testReadFileContent") {
    val file = new File("src/test/resources/touchImkHourlyDone.data/lag/0")
    fs.copyFromLocalFile(new Path(file.getAbsolutePath), new Path(lagDir + "/0"))
    assert(job.readFileContent(new Path(lagDir + "/0")).equals("1560946257001"))
  }

  test("testDoneFilePostfix") {

  }

}