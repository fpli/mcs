package com.ebay.traffic.chocolate.sparknrt.compare

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

class TestCompare extends BaseFunSuite{

  private val click_source = "/Users/huiclu/epn-nrt/compare-test/my-data"
  private val click_dest = "/Users/huiclu/epn-nrt/compare-test/your-data"
  private val click_outputPath = "/Users/huiclu/epn-nrt/tmp/test6/compare-click.txt"

  private val imp_source = "/Users/huiclu/epn-nrt/compare-test/my-impre-data"
  private val imp_dest = "/Users/huiclu/epn-nrt/compare-test/your-impre-data"
  private val imp_outputPath = "/Users/huiclu/epn-nrt/tmp/test6/compare-impression.txt"

  private val work_dir = "/Users/huiclu/epn-nrt/compare-test/"

  @transient private lazy val hadoopConf = {
    new Configuration()
  }

  private lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  override def beforeAll(): Unit = {

  }

  test("Test EPN Data compare job") {

    val args = Array(
      "--mode", "local[8]",
      "--click_source", click_source,
      "--click_dest", click_dest,
      "--impression_source", imp_source,
      "--impression_dest", imp_dest,
      "--click_outputPath", click_outputPath,
      "--impression_outputPath", imp_outputPath,
      "--click_run", "false",
      "--impression_run", "true",
      "--workDir", work_dir
    )
    val params = Parameter(args)
    val job = new TestCompareJob(params)

    job.run()
    job.stop()

  }

}
