package com.ebay.traffic.chocolate.sparknrt.crabTransform

import java.io.File

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class TestCrabTransformJob extends BaseFunSuite{

  private val tmpPath = createTempDir()
  private val workDir = tmpPath + "/workDir/"
  private val dataDir = tmpPath + "/dataDir"
  private val outPutDir = tmpPath + "/outPutDir/"
  private val kwDataDir = tmpPath + "/kwData/"

  @transient private lazy val hadoopConf = {
    new Configuration()
  }

  private lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  override def beforeAll(): Unit = {
    createTestData()
  }

  test("Test crabTransformJob") {
    val args = Array(
      "--mode", "local[8]",
      "--channel", "crabDedupe",
      "--transformedPrefix", "chocolate_",
      "--workDir", workDir,
      "--outputDir", outPutDir,
      "--kwDataDir", kwDataDir,
      "--compressOutPut", "false",
      "--maxMetaFiles", "2",
      "--elasticsearchUrl", "http://10.148.181.34:9200",
      "--metaFile", "dedupe",
      "--hdfsUri", "",
      "--xidParallelNum", "2"
    )
    val params = Parameter(args)
    val job = new CrabTransformJob(params)
    // prepare keyword lookup data
    val df = job.spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(tmpPath + "/kwData.csv")
    df.write.parquet(kwDataDir)
    job.run()
    val status1 = fs.listStatus(new Path(outPutDir + "/imkOutput"))
    assert(status1.nonEmpty)
    val status2 = fs.listStatus(new Path(outPutDir + "/dtlOutput"))
    assert(status2.nonEmpty)
    val status3 = fs.listStatus(new Path(outPutDir + "/mgOutput"))
    assert(status3.nonEmpty)

    println(job.getUserIdByCguid("", "1eb2d8b915c0a9e807109ca3f924b4c2", "1"))
  }

  def createTestData(): Unit = {
    val metadata = Metadata(workDir, "crabDedupe", MetadataEnum.dedupe)
    val dateFiles = DateFiles("date=2018-05-01", Array(dataDir + "/date=2018-05-01/imk_rvr_trckng_testData.csv"))
    val meta: MetaFiles = MetaFiles(Array(dateFiles))

    metadata.writeDedupeOutputMeta(meta)
    fs.copyFromLocalFile(new Path(new File("src/test/resources/crabTransform.data/imk_rvr_trckng_testData.csv").getAbsolutePath), new Path(dataDir + "/date=2018-05-01/imk_rvr_trckng_testData.csv"))
    fs.copyFromLocalFile(new Path(new File("src/test/resources/crabTransform.data/kwData.csv").getAbsolutePath), new Path(tmpPath + "/kwData.csv"))
  }

}
