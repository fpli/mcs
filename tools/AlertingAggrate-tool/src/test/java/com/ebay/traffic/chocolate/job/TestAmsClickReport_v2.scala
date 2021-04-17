package com.ebay.traffic.chocolate.job

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

class TestAmsClickReport_v2 extends BaseFunSuite {
  val schema_epn_click_dir = "df_epn_click.json"

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
    var inputDir = getClass.getResource("/click_v2/").getPath
    val outputDir = tmpPath + "/hourlyClickCount"
    val job = new AmsClickReport_v2(inputDir, outputDir, "hourlyClickCount","test")

    job.run()

    val outputData = job.getData(outputDir)
    println(outputData.count())
    outputData.printSchema()
    outputData.show(4)

    assert(outputData.count()  == 3)

    job.stop()
  }
  test("test dailyClickTrend function in the AppflyerDataMain com.xl.traffic.chocolate.job") {
    var inputDir = getClass.getResource("/click_v2/").getPath
    val outputDir = tmpPath + "/dailyClickTrend"
    val job = new AmsClickReport_v2(inputDir, outputDir, "dailyClickTrend", "test")

    job.run()

    val outputData = job.getData(outputDir)
    println(outputData.count())
    outputData.printSchema()
    outputData.show(4)

    assert(outputData.count()  == 3)

    job.stop()
  }

  test("test dailyDomainTrend function in the AppflyerDataMain com.xl.traffic.chocolate.job") {
    var inputDir = getClass.getResource("/click_v2/").getPath
    val outputDir = tmpPath + "/dailyDomainTrend"
    val job = new AmsClickReport_v2(inputDir, outputDir, "dailyDomainTrend", "test")

    job.run()

    val outputData = job.getData(outputDir)
    println(outputData.count())
    outputData.printSchema()
    outputData.show(15)

    assert(outputData.count()  == 9)

    job.stop()
  }


  test("test extractDate") {
    val amsClickReport = new AmsClickReport_v2("", "", "","test")
    val date = amsClickReport.extractDate("2019-07-26 23:58:01.434")
    assert(date === "2019-07-26")
  }

  test("test extractHour") {
    val amsClickReport = new AmsClickReport_v2("", "", "", "test")
    val hour = amsClickReport.extractHour("2019-07-21 00:00:18.145")
    assert(hour === "0")
  }

}
