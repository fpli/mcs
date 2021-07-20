/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.hourlyDone

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterEach

import java.io.File
import java.text.DecimalFormat
import java.time.temporal.ChronoUnit
import java.time.{ZoneId, ZonedDateTime}

class TestUTPImkHourlyDoneJob extends BaseFunSuite {

  private val tmpPath = createTempDir()
  private val cacheDir = tmpPath + "/cache"
  private val doneDir = tmpPath + "/doneDir"
  private val jobDir = tmpPath + "/jobDir"

  var job: UTPImkHourlyDoneJob = _

  @transient private lazy val hadoopConf = {
    new Configuration()
  }

  private lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  protected val cacheTable = "utp_imk_hourly_done_cache"

  override def beforeAll(): Unit = {
    // init a spark session to enable hive support before BaseSparkJob, when try to get spark session in BaseSparkJob,
    // this one will be returned
    val sparkSession = SparkSession.builder().appName("TestUTPImkHourlyDoneJob").master("local[8]")
      .appName("SparkUnitTesting")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir"))
      .enableHiveSupport()
      .getOrCreate()

    val sql =
      s"""CREATE TABLE IF NOT EXISTS $cacheTable
(
    `rvr_id`                bigint,
    `dt`             string,
    `event_ts`              string,
    `rvr_chnl_type_cd`      string,
    `rvr_cmnd_type_cd`      string
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        WITH SERDEPROPERTIES (
        'serialization.format' = '1'
        )
    STORED AS
        INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION '$cacheDir'"""
    sparkSession.sql(sql)

    job = new UTPImkHourlyDoneJob(Parameter(Array(
      "--mode", "local[8]",
      "--inputSource", "imk_rvr_trckng_event",
      "--cacheTable", cacheTable,
      "--cacheDir", cacheDir,
      "--doneFileDir", doneDir,
      "--jobDir", jobDir,
      "--doneFilePrefix", "imk_rvr_trckng_event_hourly.done.",
      "--partitions", "1"
    )))

    // prepare imk table
    val sourceFile = new File("src/test/resources/hourlyDone.data/imk/part-9-9325")

    val inputDf = job.readFilesAsDF(sourceFile.getAbsolutePath).withColumnRenamed("event_dt", "dt")
    inputDf.createOrReplaceTempView("imk_rvr_trckng_event")
  }



  test("testGetLastDoneFileDatetimeFromFileList") {
    fs.delete(new Path(doneDir), true)

    val file1 = new File("src/test/resources/hourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906191900000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(doneDir + "/imk_rvr_trckng_event_hourly.done.201906191900000000"))
    val file2 = new File("src/test/resources/hourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906192000000000")
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

    val file1 = new File("src/test/resources/hourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906191900000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(doneDir + "/20190619/imk_rvr_trckng_event_hourly.done.201906191900000000"))
    val file2 = new File("src/test/resources/hourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906192000000000")
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(doneDir + "/20190619/imk_rvr_trckng_event_hourly.done.201906192000000000"))

    var actual: ZonedDateTime = job.getLastDoneFileDateTimeAndDelay(now, doneDir)._1
    var expect: ZonedDateTime = ZonedDateTime.of(2019, 6, 19, 20, 0, 0, 0, job.defaultZoneId).truncatedTo(ChronoUnit.HOURS)
    assert(actual.equals(expect))
    println(actual)

    fs.delete(new Path(doneDir), true)

    // test delay cross day
    now = ZonedDateTime.of(2019, 6, 20, 22, 0, 0, 0, ZoneId.systemDefault())

    fs.mkdirs(new Path(doneDir + "/20190619"))

    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(doneDir + "/20190619/imk_rvr_trckng_event_hourly.done.201906191900000000"))
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(doneDir + "/20190619/imk_rvr_trckng_event_hourly.done.201906192000000000"))

    actual = job.getLastDoneFileDateTimeAndDelay(now, doneDir)._1
    expect = ZonedDateTime.of(2019, 6, 19, 20, 0, 0, 0, job.defaultZoneId).truncatedTo(ChronoUnit.HOURS)
    assert(actual.equals(expect))
    println(actual)
    fs.delete(new Path(doneDir), true)
  }

  test("test read source table") {
    // prepare done file
    fs.mkdirs(new Path(doneDir + "/20210125"))
    val file1 = new File("src/test/resources/hourlyDone.data/done/imk_rvr_trckng_event_hourly.done.202101250500000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(doneDir + "/20210125/imk_rvr_trckng_event_hourly.done.202101250500000000"))
    val now = ZonedDateTime.of(2021, 1, 25, 18, 0, 0, 0, ZoneId.systemDefault())

    val df = job.readSource(now)
    assert(df.count() == 1)
    fs.delete(new Path(doneDir), true)
  }

  test("test update cache") {
    // prepare done file
    fs.mkdirs(new Path(doneDir + "/20210125"))
    val file1 = new File("src/test/resources/hourlyDone.data/done/imk_rvr_trckng_event_hourly.done.202101250500000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(doneDir + "/20210125/imk_rvr_trckng_event_hourly.done.202101250500000000"))
    val now = ZonedDateTime.of(2021, 1, 25, 22, 0, 0, 0, ZoneId.systemDefault())

    fs.delete(new Path(cacheDir), true)
    fs.mkdirs(new Path(cacheDir))
    val sourceDf = job.readSource(now)
    job.updateCache(sourceDf, cacheTable, cacheDir, now)
    val cacheDf = job.readCache()
    // cacheDf should be equal to sourceDf
    assert(sourceDf.except(cacheDf).count() == 0)
    assert(cacheDf.except(sourceDf).count() == 0)
    fs.delete(new Path(cacheDir), true)
  }

  test("test generate done files") {
    fs.delete(new Path(doneDir), true)
    // prepare done file, the last done is 2021-01-25 05
    fs.mkdirs(new Path(doneDir + "/20210125"))
    val file1 = new File("src/test/resources/hourlyDone.data/done/imk_rvr_trckng_event_hourly.done.202101250500000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(doneDir + "/20210125/imk_rvr_trckng_event_hourly.done.202101250500000000"))
    val now = ZonedDateTime.of(2021, 1, 25, 16, 10, 0, 0, ZoneId.systemDefault())

    // read source df
    val sourceDf = job.readSource(now)

    // generate new done files
    val lastDoneAndDelay = job.getLastDoneFileDateTimeAndDelay(now, doneDir)
    val lastDone = job.generateHourlyDoneFile(sourceDf, lastDoneAndDelay, now)

    assert(lastDone.isDefined)
    assert(lastDone.get.equals(ZonedDateTime.of(2021, 1, 25, 19, 0, 0, 0, ZoneId.systemDefault())))

    // verify done files, min ts is 2021-01-25 20:06:07.260, done file from 06 to 19 should be generated.
    val hourFormatter = new DecimalFormat("00")
    for (i <- 6 to 19) {
      assert(fs.exists(new Path(doneDir + "/20210125/imk_rvr_trckng_event_hourly.done.20210125" + hourFormatter.format(i) + "00000000")))
    }

    fs.delete(new Path(doneDir), true)
  }

  test("test update done files") {
    fs.delete(new Path(cacheDir), true)
    fs.mkdirs(new Path(cacheDir))

    fs.delete(new Path(doneDir), true)
    // prepare done file, the last done is 2021-01-25 05
    fs.mkdirs(new Path(doneDir + "/20210125"))
    val file1 = new File("src/test/resources/hourlyDone.data/done/imk_rvr_trckng_event_hourly.done.202101250500000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(doneDir + "/20210125/imk_rvr_trckng_event_hourly.done.202101250500000000"))
    val now = ZonedDateTime.of(2021, 1, 25, 22, 0, 0, 0, ZoneId.systemDefault())

    // init a empty cache
    val sourceDf = job.readSource(now)
    job.updateCache(sourceDf.filter(col("rvr_id").isNull), cacheTable, cacheDir, now)

    job.updateDoneFiles(now)

    // verify done files, min ts is 2021-01-25 20:06:07.260, done file from 06 to 19 should be generated.
    val hourFormatter = new DecimalFormat("00")
    for (i <- 6 to 19) {
      assert(fs.exists(new Path(doneDir + "/20210125/imk_rvr_trckng_event_hourly.done.20210125" + hourFormatter.format(i) + "00000000")))
    }

    fs.delete(new Path(doneDir), true)
    fs.delete(new Path(cacheDir), true)
  }

  test("test clear expired cache") {
    fs.delete(new Path(cacheDir), true)
    fs.mkdirs(new Path(cacheDir))

    val now = ZonedDateTime.of(2021, 1, 25, 22, 0, 0, 0, ZoneId.systemDefault())
    fs.create(new Path(cacheDir + "/" + "2021-01-20-01-01_123"))
    fs.create(new Path(cacheDir + "/" + "2021-01-23-01-01_123"))
    job.clearExpiredCache(Some(now), cacheDir)

    // 2021-01-20-01-01_123 should be deleted
    assert(!fs.exists(new Path(cacheDir + "/" + "2021-01-20-01-01_123")))
    assert(fs.exists(new Path(cacheDir + "/" + "2021-01-23-01-01_123")))

    fs.delete(new Path(cacheDir), true)
  }
}
