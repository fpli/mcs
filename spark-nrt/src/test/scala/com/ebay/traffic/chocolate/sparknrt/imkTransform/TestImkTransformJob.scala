package com.ebay.traffic.chocolate.sparknrt.imkTransform

import java.io.File

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class TestImkTransformJob extends BaseFunSuite{

  val tmpPath: String = createTempPath()
  val workDir: String = tmpPath + "/workDir/"
  val dataDir: String = tmpPath + "/dataDir/"
  val outputDir: String = tmpPath + "/outputDir/"

  @transient private lazy val hadoopConf = {
    new Configuration()
  }

  private lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  override def beforeAll(): Unit = {
    createTestDataForPS()
  }

  test("Test imkTransformJob") {
    val args = Array(
      "--mode", "local[8]",
      "--channel", "PAID_SEARCH",
      "--transformedPrefix", "UT_PRE",
      "--workDir", workDir,
      "--outputDir", outputDir,
      "--compressOutPut", "false"
    )
    val params = Parameter(args)
    val job = new ImkTransformJob(params)
    job.run()
  }

  def createTestDataForPS(): Unit = {

    val metadata = Metadata(workDir, "PAID_SEARCH", MetadataEnum.imkDump)
    val dateFiles = DateFiles("date=2018-05-01", Array(dataDir + "/date=2018-05-01/testData.dat.gz"))
    val meta: MetaFiles = MetaFiles(Array(dateFiles))

    metadata.writeDedupeOutputMeta(meta, Array(".apollo"))
    fs.copyFromLocalFile(new Path(new File("src/test/resources/imkTransform.data/testData.dat.gz").getAbsolutePath), new Path(dataDir + "date=2018-05-01/testData.dat.gz"))
  }
}
