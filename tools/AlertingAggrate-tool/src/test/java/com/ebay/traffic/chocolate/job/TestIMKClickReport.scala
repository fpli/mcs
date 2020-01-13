package com.ebay.traffic.chocolate.job

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions._

/**
  * Created by shuangxu on 10/22/19.
  */
class TestIMKClickReport extends BaseFunSuite{

  @transient lazy val hadoopConf = {
    new Configuration()
  }

  lazy val fs = {
    val f = FileSystem.get(hadoopConf)
    sys.addShutdownHook(f.close())
    f
  }

  val tmpPath = createTempPath()
  val testFile = "chocolate_date=2019-10-23_application_1561139602691_428469_00006_part_00000"

  test("test onceClickCount: non-click event (rvr_cmnd_type_cd != 1), non-imk channel (rvr_chnl_type_cd not in the list[2, 4, 16]"){
    val inputDir = getClass.getResource("/imkInput/").getPath
    val outputDir = tmpPath + "/imkOutput/"
    println("output dir: " + outputDir)
    val schema_epn_click_dir = "df_imk_apollo.json"
    val job = new IMKClickReport(inputDir, outputDir, "hourlyClickCount", schema_epn_click_dir, "test", "2019-10-23", "true")
    //206020857521214208: rvr_cmnd_type_cd is 2, 206021039988435328: rvr_chnl_type_cd is 18, 206020862765273857: event_ts is 2019-10-23 02:12:56.123
    val itemList = List("206020857521214208", "206021039988435328")

    val oriDf = job.readSequenceFilesAsDF(inputDir + testFile, job.schema_imk_click_table.dfSchema, job.delimiter, false)
    println("oriDf count: " + oriDf.count())
    oriDf.show()

    assert(oriDf.filter(col("rvr_id").isin(itemList:_*)).count() == 2)
    assert(oriDf.count() == 58)

    val df = job.onceClickCount(inputDir + testFile)
    println("filtered df count: " + df.count())
    df.show()

    assert(df.filter((col("event_dt") === "2019-10-23") and (col("click_hour") === "0") and (col("click_cnt") === "55") and (col("channel_id") === "16")).count() == 1)
    assert(df.filter((col("event_dt") === "2019-10-23") and (col("click_hour") === "2") and (col("click_cnt") === "1") and (col("channel_id") === "16")).count() == 1)
    assert(df.count() == 2)
  }

  test("normal case, test clicks all in correct date"){
    val inputDir = getClass.getResource("/imkInput/").getPath
    val outputDir = tmpPath + "/imkOutput/"
    println("output dir: " + outputDir)
    val schema_epn_click_dir = "df_imk_apollo.json"
    val job = new IMKClickReport(inputDir, outputDir, "hourlyClickCount", schema_epn_click_dir, "test", "2019-10-23", "true")

    job.run()

    val outputData = job.getData(outputDir)
    println(outputData.count())
    outputData.printSchema()
    outputData.show()

    assert(outputData.filter((col("event_dt") === "2019-10-22") and (col("click_hour") === "0") and (col("click_count") === "2") and (col("distinct_click_count") === "2") and (col("differences") === "0") and (col("channel_id") === "16")).count() == 1)
    assert(outputData.filter((col("event_dt") === "2019-10-21") and (col("click_hour") === "23") and (col("click_count") === "276") and (col("distinct_click_count") === "276") and (col("differences") === "0") and (col("channel_id") === "4")).count() == 1)
    assert(outputData.filter((col("event_dt") === "2019-10-23") and (col("click_hour") === "0") and (col("click_count") === "110") and (col("distinct_click_count") === "55") and (col("differences") === "55") and (col("channel_id") === "16")).count() == 1)
    assert(outputData.filter((col("event_dt") === "2019-10-23") and (col("click_hour") === "2") and (col("click_count") === "2") and (col("distinct_click_count") === "1") and (col("differences") === "1") and (col("channel_id") === "16")).count() == 1)

    assert(outputData.count()  == 4)

    job.stop()
  }
}
