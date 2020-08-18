/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.imk


import java.io.File
import java.time.{ZoneId, ZonedDateTime}
import java.time.temporal.ChronoUnit

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.imk.ImkNrtJob
import com.ebay.traffic.chocolate.sparknrt.imk.Parameter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class TestImkNrtJob extends BaseFunSuite{

  private val tmpPath = createTempDir()
  private val deltaDir = tmpPath + "/apps/delta/tracking-events"
  private val outPutDir = tmpPath + "/apps/tracking-events"
  private val doneDir = tmpPath + "/doneDir"

  var job: ImkNrtJob = _

  @transient private lazy val hadoopConf = {
    new Configuration()
  }

  private lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  override def beforeAll(): Unit = {
    fs.mkdirs(new Path(deltaDir))
    fs.mkdirs(new Path(outPutDir))

    job = new ImkNrtJob(Parameter(Array(
      "--mode", "local[8]",
      "--inputSource", "choco_data.tracking_event",
      "--deltaDir", deltaDir,
      "--outPutDir", outPutDir,
      "--doneFileDir", doneDir,
      "--doneFilePrefix", "imk_rvr_trckng_event_hourly.done.",
      "--partitions", "1"
    )))
  }

  test("testGetLastDoneFileDatetimeFromFileList") {
    fs.delete(new Path(doneDir), true)

    val file1 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906191900000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(doneDir + "/imk_rvr_trckng_event_hourly.done.201906191900000000"))
    val file2 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906192000000000")
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(doneDir + "/imk_rvr_trckng_event_hourly.done.201906192000000000"))

    val actual = job.getLastDoneFileDatetimeFromDoneFiles(fs.listStatus(new Path(doneDir)))
    val expect = ZonedDateTime.of(2019, 6, 19, 20, 0, 0, 0, job.defaultZoneId).truncatedTo(ChronoUnit.HOURS)
    assert(actual.equals(expect))

    fs.delete(new Path(doneDir), true)
  }

  test("testGetLastDoneFileDatetime") {
    fs.delete(new Path(doneDir), true)

    // test delay in one day
    var now = ZonedDateTime.of(2019, 6, 19, 22, 0, 0, 0, ZoneId.systemDefault())

    fs.mkdirs(new Path(doneDir + "/20190619"))

    val file1 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906191900000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(doneDir + "/20190619/imk_rvr_trckng_event_hourly.done.201906191900000000"))
    val file2 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906192000000000")
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(doneDir + "/20190619/imk_rvr_trckng_event_hourly.done.201906192000000000"))

    var actual: ZonedDateTime = job.getLastDoneFileDateTime(now)
    var expect: ZonedDateTime = ZonedDateTime.of(2019, 6, 19, 20, 0, 0, 0, job.defaultZoneId).truncatedTo(ChronoUnit.HOURS)
    assert(actual.equals(expect))
    println(actual)

    fs.delete(new Path(doneDir), true)

    // test delay cross day
    now = ZonedDateTime.of(2019, 6, 20, 22, 0, 0, 0, ZoneId.systemDefault())

    fs.mkdirs(new Path(doneDir + "/20190619"))

    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(doneDir + "/20190619/imk_rvr_trckng_event_hourly.done.201906191900000000"))
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(doneDir + "/20190619/imk_rvr_trckng_event_hourly.done.201906192000000000"))

    actual = job.getLastDoneFileDateTime(now)
    expect = ZonedDateTime.of(2019, 6, 19, 20, 0, 0, 0, job.defaultZoneId).truncatedTo(ChronoUnit.HOURS)
    assert(actual.equals(expect))
    println(actual)
    fs.delete(new Path(doneDir), true)
  }

  test("test imk etl job for parquet output") {
    val job = new ImkNrtJob(Parameter(Array(
      "--mode", "local[8]",
      "--deltaDir", deltaDir,
      "--outPutDir", outPutDir,
      "--partitions", "1"
    )))

    job.run()
    job.stop()
  }
}
