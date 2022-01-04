/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.hourlyDone

import java.io.File
import java.text.DecimalFormat
import java.time.{ZoneId, ZonedDateTime}

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

class TestUTPHourlyDoneJob extends BaseFunSuite {

  private val tmpPath = createTempDir()
  private val cacheDir = tmpPath + "/cache"
  private val doneDir = tmpPath + "/doneDir"
  private val jobDir = tmpPath + "/jobDir"

  var job: UTPHourlyDoneJob = _

  @transient private lazy val hadoopConf = {
    new Configuration()
  }

  private lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  protected val cacheTable = "utp_hourly_done_cache"

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
          `eventId`           string,
          `dt`                string,
          `hour`              string,
          `producerEventTs`   string,
          `channelType`       string,
          `actionType`        string
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

    job = new UTPHourlyDoneJob(Parameter(Array(
      "--mode", "local[8]",
      "--inputSource", "utp_event",
      "--cacheTable", cacheTable,
      "--cacheDir", cacheDir,
      "--doneFileDir", doneDir,
      "--jobDir", jobDir,
      "--doneFilePrefix", "utp_event_hourly.done.",
      "--partitions", "1"
    )))

    // prepare imk table
    val sourceFile = new File("src/test/resources/utpHourlyDone.data/utp/utp-7-96530.snappy.parquet")
    var inputDf = job.readFilesAsDF(sourceFile.getAbsolutePath).withColumn("dt", lit("2021-12-12"))
      .withColumn("hour", lit("00")).withColumn("channelType", lit("DISPLAY"))
    inputDf = inputDf.union(inputDf.withColumn("channelType", lit("MRKT_EMAIL"))).union(inputDf.withColumn("channelType",lit("MOBILE_NOTIF")))
    inputDf.show()
    inputDf.createOrReplaceTempView("utp_event")
  }

  test("test update done files") {
    fs.delete(new Path(cacheDir), true)
    fs.mkdirs(new Path(cacheDir))

    fs.delete(new Path(doneDir), true)
    // prepare done file, the last done is 2021-01-25 05
    fs.mkdirs(new Path(doneDir + "/20211212"))
    val file1 = new File("src/test/resources/utpHourlyDone.data/done/utp_event_hourly.done.202112120500000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(doneDir + "/20211212/utp_event_hourly.done.202112120500000000"))
    fs.createNewFile(new Path(doneDir + "/20211212/utp_event_hourly.ep.done.202112120500000000"))
    fs.createNewFile(new Path(doneDir + "/20211211/utp_event_hourly.xx.done.202112110500000000"))

    val now = ZonedDateTime.of(2021, 12, 12, 22, 0, 0, 0, ZoneId.systemDefault())

    // init a empty cache
    val sourceDf = job.readSource(now)
    job.updateCache(sourceDf.filter(col("eventId").isNull), cacheTable, cacheDir, now)

    job.updateDoneFiles(now)

    // verify done files, min ts is 2021-01-25 20:06:07.260, done file from 06 to 19 should be generated.
    val hourFormatter = new DecimalFormat("00")
    for (i <- 6 to 19) {
      assert(fs.exists(new Path(doneDir + "/20211212/utp_event_hourly.done.20211212" + hourFormatter.format(i) + "00000000")))
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
