package com.ebay.traffic.chocolate.sparknrt.crabDedupe

import java.io.File

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class TestCrabDedupeJob extends BaseFunSuite{
  private val tmpPath = createTempDir()
  private val workDir = tmpPath + "/workDir"
  private val inputDir = tmpPath + "/inputDir"
  private val outputDir = tmpPath + "/outputDir"

  @transient private lazy val hadoopConf = {
    new Configuration()
  }

  private lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  override def beforeAll(): Unit = {
    fs.copyFromLocalFile(new Path(new File("src/test/resources/crabDedupe.data/imk_rvr_trckng_testData.csv").getAbsolutePath), new Path(inputDir + "/imk_rvr_trckng_testData.csv"))
  }

  test("Test crabDedupeJob") {
    val args = Array(
      "--mode", "local[8]",
      "--workDir", workDir,
      "--inputDir", inputDir,
      "--outputDir", outputDir,
      "--maxDataFiles", "10",
      "--elasticsearchUrl", "http://10.148.181.34:9200",
      "--couchbaseDedupe", "false",
      "--snappyCompression", "false",
      "--couchbaseDatasource", "chocopartnercbdbhost",
      "--couchbaseTTL", "600",
      "--partitions", "1"
    )
    val params = Parameter(args)
    val job = new CrabDedupeJob(params)
    job.run()
  }

}
