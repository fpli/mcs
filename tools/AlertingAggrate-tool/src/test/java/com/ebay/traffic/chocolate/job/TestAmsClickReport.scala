package com.ebay.traffic.chocolate.job

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

class TestAmsClickReport extends BaseFunSuite {

  @transient lazy val hadoopConf = {
    new Configuration()
  }

  lazy val fs = {
    val f = FileSystem.get(hadoopConf)
    sys.addShutdownHook(f.close())
    f
  }

  val tmpPath = createTempPath()


  test("test hourlyClickCount function in the AppflyerDataMain com.xl.traffic.chocolate.job") {
    var inputDir = getClass.getResource("/click/").getPath
    val outputDir = tmpPath + "/hourlyClickCount"
    val schema_epn_click_dir = "df_epn_click.json"
    val job = new AmsClickReport(inputDir, outputDir, "hourlyClickCount", schema_epn_click_dir, "test")

    job.run()

    val outputData = job.getData(outputDir)
    println(outputData.count())
    outputData.printSchema()
    outputData.show(4)

    assert(outputData.count()  == 3)

    job.stop()
  }

  test("test dailyClickTrend function in the AppflyerDataMain com.xl.traffic.chocolate.job") {
    var inputDir = getClass.getResource("/click/").getPath
    val outputDir = tmpPath + "/dailyClickTrend"
    val schema_epn_click_dir = "df_epn_click.json"
    val job = new AmsClickReport(inputDir, outputDir, "dailyClickTrend", schema_epn_click_dir, "test")

    job.run()

    val outputData = job.getData(outputDir)
    println(outputData.count())
    outputData.printSchema()
    outputData.show(4)

    assert(outputData.count()  == 3)

    job.stop()
  }

  test("test dailyDomainTrend function in the AppflyerDataMain com.xl.traffic.chocolate.job") {
    var inputDir = getClass.getResource("/click/").getPath
    val outputDir = tmpPath + "/dailyDomainTrend"
    val schema_epn_click_dir = "df_epn_click.json"
    val job = new AmsClickReport(inputDir, outputDir, "dailyDomainTrend", schema_epn_click_dir, "test")

    job.run()

    val outputData = job.getData(outputDir)
    println(outputData.count())
    outputData.printSchema()
    outputData.show(15)

    assert(outputData.count()  == 9)

    job.stop()
  }


  test("test extractDate") {
    val schema_epn_click_dir = "df_epn_click.json"
    val amsClickReport = new AmsClickReport("", "", "", schema_epn_click_dir, "test")
    val date = amsClickReport.extractDate("2019-07-21 00:00:18.145")
    assert(date === "2019-07-21")
  }

  test("test extractHour") {
    val schema_epn_click_dir = "df_epn_click.json"
    val amsClickReport = new AmsClickReport("", "", "", schema_epn_click_dir, "test")
    val hour = amsClickReport.extractHour("2019-07-21 00:00:18.145")
    assert(hour === "0")
  }

}
