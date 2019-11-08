package com.ebay.traffic.chocolate.sparknrt.crabDedupe

import java.io.File

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
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
    job.LAG_FILE = tmpPath + "/last_ts/PAID_SEARCH/crab_min_ts"
    job.run()
    val metadata = Metadata(workDir, "crabDedupe", MetadataEnum.dedupe)
    val dom = metadata.readDedupeOutputMeta()
    assert (dom.length == 1)
    assert (dom(0)._2.contains("date=2019-03-27"))
    assert (dom(0)._2.contains("date=2019-03-28"))

    val df1 = job.readFilesAsDFEx(dom(0)._2("date=2019-03-27"), job.schema_tfs.dfSchema, "csv", "bel")
    df1.show()
    assert (df1.count() == 7)

    val df2 = job.readFilesAsDFEx(dom(0)._2("date=2019-03-28"), job.schema_tfs.dfSchema, "csv", "bel")
    df2.show()
    assert (df2.count() == 1)

  }

}
