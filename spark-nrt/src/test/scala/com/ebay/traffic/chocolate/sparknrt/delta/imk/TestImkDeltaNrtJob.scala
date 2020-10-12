/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.delta.imk


import java.io.File
import java.time.{ZoneId, ZonedDateTime}

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.delta.Parameter
import com.ebay.traffic.chocolate.sparknrt.utils.TableSchema
import io.delta.tables.DeltaTable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class TestImkDeltaNrtJob extends BaseFunSuite {

  private val tmpPath = createTempDir()
  private val deltaDir = tmpPath + "/delta/tracking-events"
  private val outPutDir = tmpPath + "/tracking-events"
  private val deltaDoneDir = tmpPath + "/deltaDoneDir"
  private val outputDoneDir = tmpPath + "/outputDoneDir"
  private val jobDir = tmpPath + "/jobDir"

  var job: ImkDeltaNrtJob = _

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

    job = new ImkDeltaNrtJob(Parameter(Array(
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
    val sourceFile = new File("src/test/resources/masterTable/imk/master_table.csv")

    val trackingEventTable = TableSchema("df_tracking_event.json")
    val inputDf = job.readFilesAsDF(sourceFile.getAbsolutePath, trackingEventTable.dfSchema, "csv", "comma")
    inputDf.createOrReplaceTempView("tracking_event_test")
  }

  test("test IMK update output") {

    // prepare current date and last done file
    // the last done of delta is 2020-08-17 05
    fs.mkdirs(new Path(deltaDoneDir + "/20200817"))
    val file1 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.202008170500000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(deltaDoneDir + "/20200817/imk_rvr_trckng_event_hourly.done.202008170500000000"))

    // the last done of output is 2020-08-16 05
    fs.mkdirs(new Path(outputDoneDir + "/20200816"))
    val file2 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.202008160500000000")
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(outputDoneDir + "/20200816/imk_rvr_trckng_event_hourly.done.202008160500000000"))

    // prepare delta table
    // delta table contains 1 record in 2020-08-16,
    // 1 record in 2020-08-17 before the delta last done,
    // 3 records in 2020-07-17 after the delta last done
    val deltaFileSource = new File("src/test/resources/masterTable/imk/delta_table.csv")

    val trackingEventTable = TableSchema("df_imk_delta.json")
    val inputDf = job.readFilesAsDF(deltaFileSource.getAbsolutePath, trackingEventTable.dfSchema, "csv", "comma")

    inputDf.write.format("delta").mode("overwrite").partitionBy("dt").save(deltaDir)

    val imkDeltaBeforeUpdate = DeltaTable.forPath(job.spark, deltaDir)
    assert(imkDeltaBeforeUpdate.toDF.count() == 6)

    // will update data between 2 done file hours
    // set current time 2020-08-17 22
    val now = ZonedDateTime.of(2020, 8, 17, 22, 0, 0, 0, ZoneId.systemDefault())
    job.updateOutput(now)

    // verification. There will be 1 record in output dt=2020-08-16 and 1 record in output dt=2020-08-17
    val df = job.readFilesAsDF(outPutDir)
    df.show()
    // 1 in 2020-08-16, 1 in 2020-08-17
    assert(df.count() == 2)
    fs.delete(new Path(tmpPath.toString), true)
  }

  test("test imk entire job") {
    val mockNow = ZonedDateTime.of(2020, 8, 17, 22, 0, 0, 0, ZoneId.systemDefault())
    val now = ZonedDateTime.of(2020, 8, 17, 22, 0, 0, 0, ZoneId.systemDefault()).format(job.dayFormatterInDoneFileName)
    val yesterday = mockNow.minusDays(1).format(job.dayFormatterInDoneFileName)
    fs.mkdirs(new Path(deltaDoneDir + "/" + now))
    fs.mkdirs(new Path(deltaDoneDir + "/" + yesterday))
    fs.create(new Path(deltaDoneDir + "/" + now + "/imk_rvr_trckng_event_hourly.done." + now + "0100000000"))

    fs.mkdirs(new Path(outputDoneDir + "/" + now))
    fs.mkdirs(new Path(outputDoneDir + "/" + yesterday))

    // prepare current date and last done file
    // the last done of delta is 2020-08-17 05
    fs.mkdirs(new Path(deltaDoneDir + "/20200817"))
    val file1 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.202008170500000000")
    fs.copyFromLocalFile(new Path(file1.getAbsolutePath), new Path(deltaDoneDir + "/20200817/imk_rvr_trckng_event_hourly.done.202008170500000000"))

    // the last done of output is 2020-08-16 05
    fs.mkdirs(new Path(outputDoneDir + "/20200816"))
    val file2 = new File("src/test/resources/touchImkHourlyDone.data/done/imk_rvr_trckng_event_hourly.done.202008160500000000")
    fs.copyFromLocalFile(new Path(file2.getAbsolutePath), new Path(outputDoneDir + "/20200816/imk_rvr_trckng_event_hourly.done.202008160500000000"))

    // prepare delta table
    // delta table contains 1 record in 2020-08-16,
    // 4 record in 2020-08-17 before the delta last done,
    // 1 records in 2020-07-17 after the delta last done, timestamp is 1597665600865
    val deltaFileSource = new File("src/test/resources/masterTable/imk/delta_table.csv")

    val trackingEventTable = TableSchema("df_imk_delta.json")
    val inputDf = job.readFilesAsDF(deltaFileSource.getAbsolutePath, trackingEventTable.dfSchema, "csv", "comma")

    inputDf.write.format("delta").mode("overwrite").partitionBy("dt").save(deltaDir)

    job.updateDelta(mockNow)
    job.updateOutput(mockNow)

    // verification
    val df = job.readFilesAsDF(outPutDir)
    df.show()
    // 1 in 2020-08-16, 4 in 2020-08-17
    assert(df.count() == 5)
    // done file catch up with delta table
    assert(fs.exists(new Path(outputDoneDir+"/20200817/imk_rvr_trckng_event_hourly.done.202008170500000000")))
    fs.delete(new Path(tmpPath.toString), true)
  }
}