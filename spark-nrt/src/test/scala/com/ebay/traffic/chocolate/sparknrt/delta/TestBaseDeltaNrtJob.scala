/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.delta

import java.io.File
import java.time.{ZoneId, ZonedDateTime}
import java.time.temporal.ChronoUnit

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.utils.TableSchema
import io.delta.tables.DeltaTable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class TestBaseDeltaNrtJob extends BaseFunSuite{

  private val tmpPath = createTempDir()
  private val deltaDir = tmpPath + "/delta/tracking-events"
  private val outPutDir = tmpPath + "/tracking-events"
  private val deltaDoneDir = tmpPath + "/deltaDoneDir"
  private val outputDoneDir = tmpPath + "/outputDoneDir"
  private val jobDir = tmpPath + "/jobDir"

  var job: BaseDeltaLakeNrtJob = _

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

    job = new BaseDeltaLakeNrtJob(Parameter(Array(
      "--mode", "local[8]",
      "--inputSource", "tracking_event_test",
      "--deltaDir", deltaDir,
      "--outPutDir", outPutDir,
      "--deltaDoneFileDir", deltaDoneDir,
      "--outputDoneFileDir", outputDoneDir,
      "--jobDir", jobDir,
      "--doneFilePrefix", "imk_rvr_trckng_event_hourly.done.",
      "--partitions", "1"
    )))

    // prepare master table
    val sourceFile = new File("src/test/resources/masterTable/master_table.csv")

    val trackingEventTable = TableSchema("df_tracking_event.json")
    val inputDf = job.readFilesAsDF(sourceFile.getAbsolutePath, trackingEventTable.dfSchema, "csv", "comma")
    inputDf.createOrReplaceTempView("tracking_event_test")
  }

  test("testGetLastDoneFileDatetimeFromFileList") {
    fs.delete(new Path(deltaDoneDir), true)

    val file1 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906191900000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(deltaDoneDir + "/imk_rvr_trckng_event_hourly.done.201906191900000000"))
    val file2 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906192000000000")
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(deltaDoneDir + "/imk_rvr_trckng_event_hourly.done.201906192000000000"))

    val actual = job.getLastDoneFileDatetimeFromDoneFiles(fs.listStatus(new Path(deltaDoneDir)))
    val expect = ZonedDateTime.of(2019, 6, 19, 20, 0, 0, 0, job.defaultZoneId).truncatedTo(ChronoUnit.HOURS)
    assert(actual.equals(expect))

    fs.delete(new Path(deltaDoneDir), true)
  }

  test("testGetLastDoneFileDatetime") {
    fs.delete(new Path(deltaDoneDir), true)

    // test delay in one day
    var now = ZonedDateTime.of(2019, 6, 19, 22, 0, 0, 0, ZoneId.systemDefault())

    fs.mkdirs(new Path(deltaDoneDir + "/20190619"))

    val file1 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906191900000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(deltaDoneDir + "/20190619/imk_rvr_trckng_event_hourly.done.201906191900000000"))
    val file2 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.201906192000000000")
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(deltaDoneDir + "/20190619/imk_rvr_trckng_event_hourly.done.201906192000000000"))

    var actual: ZonedDateTime = job.getLastDoneFileDateTimeAndDelay(now, deltaDoneDir )._1
    var expect: ZonedDateTime = ZonedDateTime.of(2019, 6, 19, 20, 0, 0, 0, job.defaultZoneId).truncatedTo(ChronoUnit.HOURS)
    assert(actual.equals(expect))
    println(actual)

    fs.delete(new Path(deltaDoneDir), true)

    // test delay cross day
    now = ZonedDateTime.of(2019, 6, 20, 22, 0, 0, 0, ZoneId.systemDefault())

    fs.mkdirs(new Path(deltaDoneDir + "/20190619"))

    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(deltaDoneDir + "/20190619/imk_rvr_trckng_event_hourly.done.201906191900000000"))
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(deltaDoneDir + "/20190619/imk_rvr_trckng_event_hourly.done.201906192000000000"))

    actual = job.getLastDoneFileDateTimeAndDelay(now, deltaDoneDir)._1
    expect = ZonedDateTime.of(2019, 6, 19, 20, 0, 0, 0, job.defaultZoneId).truncatedTo(ChronoUnit.HOURS)
    assert(actual.equals(expect))
    println(actual)
    fs.delete(new Path(deltaDoneDir), true)
  }

  test("test read source table") {
    // prepare done file
    fs.mkdirs(new Path(deltaDoneDir+"/20200817"))
    val file1 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.202008170500000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(deltaDoneDir + "/20200817/imk_rvr_trckng_event_hourly.done.202008170500000000"))
    val now = ZonedDateTime.of(2020, 8, 17, 22, 0, 0, 0, ZoneId.systemDefault())

    val df = job.readSource(now)
    df.show()
    fs.delete(new Path(deltaDoneDir), true)
  }

  test("test generate delta done files") {

    // prepare current date and last done file
    // the last done is 2020-08-16 05
    fs.mkdirs(new Path(deltaDoneDir+"/20200816"))
    val file1 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.202008160500000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(deltaDoneDir + "/20200816/imk_rvr_trckng_event_hourly.done.202008160500000000"))

    val now = ZonedDateTime.of(2020, 8, 17, 22, 0, 0, 0, ZoneId.systemDefault())

    // read source df
    val sourceDf = job.readSource(now)
    sourceDf.show()

    // generate new done files
    val lastDoneAndDelay = job.getLastDoneFileDateTimeAndDelay(now, deltaDoneDir)
    job.generateDeltaDoneFile(sourceDf, lastDoneAndDelay, now)

    // verify done files
    for( i <- 6 to 9 ) {
      assert(fs.exists(new Path(deltaDoneDir+"/20200816/imk_rvr_trckng_event_hourly.done.202008160" + i + "00000000")))
    }
    for( i <- 10 to 23 ) {
      assert(fs.exists(new Path(deltaDoneDir+"/20200816/imk_rvr_trckng_event_hourly.done.20200816" + i + "00000000")))
    }

    for( i <- 0 to 9 ) {
      assert(fs.exists(new Path(deltaDoneDir+"/20200817/imk_rvr_trckng_event_hourly.done.202008170" + i + "00000000")))
    }
    for( i <- 10 to 14 ) {
      assert(fs.exists(new Path(deltaDoneDir+"/20200817/imk_rvr_trckng_event_hourly.done.20200817" + i + "00000000")))
    }

    fs.delete(new Path(deltaDoneDir), true)
  }

  test("test update delta table") {

    // prepare delta table
    val deltaFileSource = new File("src/test/resources/masterTable/delta_table.csv")

    val trackingEventTable = TableSchema("df_delta_event.json")
    val inputDf = job.readFilesAsDF(deltaFileSource.getAbsolutePath, trackingEventTable.dfSchema, "csv", "comma")

    inputDf.write.format("delta").mode("overwrite").partitionBy("dt").save(deltaDir)

    val imkDeltaBeforeUpdate = DeltaTable.forPath(job.spark, deltaDir)
    assert(imkDeltaBeforeUpdate.toDF.count() == 5 )

    // prepare current date and last delta done file
    // the last done is 2020-08-17 05
    fs.mkdirs(new Path(deltaDoneDir+"/20200817"))
    val file1 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.202008170500000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(deltaDoneDir + "/20200817/imk_rvr_trckng_event_hourly.done.202008170500000000"))

    // set current time 2020-08-17 22
    val now = ZonedDateTime.of(2020, 8, 17, 22, 0, 0, 0, ZoneId.systemDefault())

    // update delta table
    job.updateDelta(now)

    // verification
    val imkDeltaAfterUpdate = DeltaTable.forPath(job.spark, deltaDir)
    assert(imkDeltaAfterUpdate.toDF.count() == 15)

    fs.delete(new Path(deltaDir), true)
  }

  test("test generate output done files") {
    // prepare current date and last done file
    // the last done of delta is 2020-08-17 05
    fs.mkdirs(new Path(deltaDoneDir+"/20200817"))
    val file1 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.202008170500000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(deltaDoneDir + "/20200817/imk_rvr_trckng_event_hourly.done.202008170500000000"))

    // the last done of output is 2020-08-16 05
    val now = ZonedDateTime.of(2020, 8, 17, 22, 0, 0, 0, ZoneId.systemDefault())
    fs.mkdirs(new Path(outputDoneDir+"/20200816"))
    val file2 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.202008160500000000")
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(outputDoneDir + "/20200816/imk_rvr_trckng_event_hourly.done.202008160500000000"))


    // generate new done files
    val lastDeltaDoneAndDelay = job.getLastDoneFileDateTimeAndDelay(now, deltaDoneDir)
    val lastOutputDoneAndDelay = job.getLastDoneFileDateTimeAndDelay(now, outputDoneDir)

    job.generateOutputDoneFile(lastDeltaDoneAndDelay, lastOutputDoneAndDelay)

    // verify done files
    for( i <- 6 to 9 ) {
      assert(fs.exists(new Path(outputDoneDir+"/20200816/imk_rvr_trckng_event_hourly.done.202008160" + i + "00000000")))
    }
    for( i <- 10 to 23 ) {
      assert(fs.exists(new Path(outputDoneDir+"/20200816/imk_rvr_trckng_event_hourly.done.20200816" + i + "00000000")))
    }

    for( i <- 0 to 5 ) {
      assert(fs.exists(new Path(outputDoneDir+"/20200817/imk_rvr_trckng_event_hourly.done.202008170" + i + "00000000")))
    }

    fs.delete(new Path(deltaDoneDir), true)
    fs.delete(new Path(outputDoneDir), true)
  }

  test("test update output")  {

    // prepare current date and last done file
    // the last done of delta is 2020-08-17 05
    fs.mkdirs(new Path(deltaDoneDir+"/20200817"))
    val file1 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.202008170500000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(deltaDoneDir + "/20200817/imk_rvr_trckng_event_hourly.done.202008170500000000"))

    // the last done of output is 2020-08-16 05
    fs.mkdirs(new Path(outputDoneDir+"/20200816"))
    val file2 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.202008160500000000")
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(outputDoneDir + "/20200816/imk_rvr_trckng_event_hourly.done.202008160500000000"))

    // prepare delta table
    // delta table contains 1 record in 2020-08-16,
    // 1 record in 2020-08-17 before the delta last done,
    // 4 records in 2020-07-17 after the delta last done
    val deltaFileSource = new File("src/test/resources/masterTable/delta_table.csv")

    val trackingEventTable = TableSchema("df_delta_event.json")
    val inputDf = job.readFilesAsDF(deltaFileSource.getAbsolutePath, trackingEventTable.dfSchema, "csv", "comma")

    inputDf.write.format("delta").mode("overwrite").partitionBy("dt").save(deltaDir)

    val imkDeltaBeforeUpdate = DeltaTable.forPath(job.spark, deltaDir)
    assert(imkDeltaBeforeUpdate.toDF.count() == 5 )

    // will update data between 2 done file hours
    // set current time 2020-08-17 22
    val now = ZonedDateTime.of(2020, 8, 17, 22, 0, 0, 0, ZoneId.systemDefault())
    job.updateOutput(now)

    // verification. There will be 1 record in output dt=2020-08-16 and 1 record in output dt=2020-08-17
    val df = job.readFilesAsDF(outPutDir)
    df.show()
    assert(df.count() == 2)

  }

  test("test imk etl job for parquet output") {

    //    job.run()
    //    job.stop()
  }
}
