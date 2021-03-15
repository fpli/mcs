package com.ebay.traffic.chocolate.job

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.functions._

class TestIMKClickReport extends BaseFunSuite{

  @transient lazy val hadoopConf = {
    new Configuration()
  }

  val tmpPath = createTempPath()

  test("normal case, test clicks all in correct date"){
    val inputDir = getClass.getResource("/imkInputParquet/").getPath
    val outputDir = tmpPath + "/imkOutput/"
    println("output dir: " + outputDir)
    val schema_epn_click_dir = "df_imk_apollo.json"
    val job = new IMKClickReport(inputDir, outputDir, "hourlyClickCount", schema_epn_click_dir, "test", "2019-12-24", "true")

    job.run()

    val outputData = job.getData(outputDir)
    println(outputData.count())
    outputData.printSchema()
    outputData.show()

    assert(outputData.filter((col("event_dt") === "2019-12-23") and (col("click_hour") === "15") and (col("click_count") === "15696") and (col("distinct_click_count") === "15696") and (col("differences") === "0") and (col("channel_id") === "2")).count() == 1)
    assert(outputData.filter((col("event_dt") === "2019-12-23") and (col("click_hour") === "15") and (col("click_count") === "8") and (col("distinct_click_count") === "8") and (col("differences") === "0") and (col("channel_id") === "16")).count() == 1)
    assert(outputData.filter((col("event_dt") === "2019-12-24") and (col("click_hour") === "15") and (col("click_count") === "212") and (col("distinct_click_count") === "212") and (col("differences") === "0") and (col("channel_id") === "4")).count() == 1)
    assert(outputData.filter((col("event_dt") === "2019-12-24") and (col("click_hour") === "15") and (col("click_count") === "7070") and (col("distinct_click_count") === "7068") and (col("differences") === "2") and (col("channel_id") === "0")).count() == 1)

    assert(outputData.count()  == 8)

    job.stop()
  }
}
