package com.ebay.traffic.chocolate.sparknrt.crabDedupe

import java.io.File
import java.time.{ZoneId, ZonedDateTime}

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class TestCrabDedupeJob extends BaseFunSuite{
  private val tmpPath = createTempDir()
  private val workDir = tmpPath + "/apps/tracking-events-workdir"
  private val inputDir = tmpPath + "/apps/tracking-events-workdir/crabScp/dest"
  private val outputDir = tmpPath + "/apps/tracking-events"

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
    job.LAG_FILE = tmpPath + "/last_ts/crabDedupe/crab_min_ts"
    job.run()
    val metadata = Metadata(workDir, "crabDedupe", MetadataEnum.dedupe)
    val dom = metadata.readDedupeOutputMeta()
    assert (dom.length == 1)
    assert (dom(0)._2.contains("date=2019-03-27"))
    assert (dom(0)._2.contains("date=2019-03-28"))

    val df1 = job.readFilesAsDFEx(dom(0)._2("date=2019-03-27"))
    df1.show()
    assert (df1.count() == 7)

    val df2 = job.readFilesAsDFEx(dom(0)._2("date=2019-03-28"))
    df2.show()
    assert (df2.count() == 1)

    assert(fs.exists(new Path(job.LAG_FILE)))
  }

  test("Test toDateTime") {
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
    assert(job.toDateTime("").isEmpty)
    assert(job.toDateTime("12233").isEmpty)
    assert(job.toDateTime("2019-10-18 19:10:20.123").isDefined)
    assert(job.toDateTime("2019-10-18 19:10:20.123").get.equals(ZonedDateTime.of(2019, 10, 18, 19, 10, 20, 123 * 1000 * 1000, ZoneId.systemDefault())))
  }

}
