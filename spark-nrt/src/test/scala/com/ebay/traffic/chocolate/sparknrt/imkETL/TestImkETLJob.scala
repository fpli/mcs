package com.ebay.traffic.chocolate.sparknrt.imkETL

import java.io.{ByteArrayOutputStream, File}

import com.ebay.traffic.chocolate.spark.{BaseFunSuite, BaseSparkJob}
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory

import scala.collection.mutable.ArrayBuffer

class TestImkETLJob extends BaseFunSuite{

  private val tmpPath = createTempDir()
  private val workDir = tmpPath + "/apps/tracking-events-workdir"
  private val outPutDir = tmpPath + "/apps/tracking-events"

  private val localDir = getTestResourcePath("imkETL.data")

  private val kwDataDir = tmpPath + "/apps/kw_lkp/2020-01-05/"
  private val kwDataTempDir = tmpPath + "/apps/kw_lkp/temp/"

  @transient private lazy val hadoopConf = {
    new Configuration()
  }

  private lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  override def beforeAll(): Unit = {
    fs.mkdirs(new Path(workDir))
    fs.mkdirs(new Path(outPutDir))
    createTestData()
  }

  test("test imk etl job for parquet output") {
    val job = new ImkETLJob(Parameter(Array(
      "--mode", "local[8]",
      "--channel", "PAID_SEARCH,DISPLAY,ROI,SOCIAL_MEDIA",
      "--workDir", workDir,
      "--outPutDir", outPutDir,
      "--partitions", "1",
      "--elasticsearchUrl", "http://10.148.181.34:9200",
      "--transformedPrefix", "chocolate_",
      "--outputFormat", "parquet",
      "--compressOutPut", "false",
      "--kwDataDir", kwDataDir
    )))

    job.run()

    List("imkOutput", "dtlOutput", "mgOutput").foreach(dir => {
      List("date=2019-12-23", "date=2019-12-24").foreach(date => {
        // read target file. eg: /imkETL/imkOutput/date=2019-12-23/chocolate_*
        val targetFiles = fs.listStatus(new Path(outPutDir + "/imkTransform" + "/" + dir)).map(_.getPath.toUri.getPath)
        assert(targetFiles.count(file => file.contains(date)) == 1)
      })
    })

    job.stop()
  }

  test("test imk etl job for sequence output") {
    val job = new ImkETLJob(Parameter(Array(
      "--mode", "local[8]",
      "--channel", "PAID_SEARCH,DISPLAY,ROI,SOCIAL_MEDIA",
      "--workDir", workDir,
      "--outPutDir", outPutDir,
      "--partitions", "1",
      "--elasticsearchUrl", "http://10.148.181.34:9200",
      "--transformedPrefix", "chocolate_",
      "--outputFormat", "sequence",
      "--compressOutPut", "false",
      "--kwDataDir", kwDataDir
    )))

    job.run()

    List("imkOutput", "dtlOutput", "mgOutput").foreach(dir => {
      List("date=2019-12-23", "date=2019-12-24").foreach(date => {
        // read target file. eg: /imkETL/imkOutput/date=2019-12-23/chocolate_*
        val targetFiles = fs.listStatus(new Path(outPutDir + "/imkTransform" + "/" + dir)).map(_.getPath.toUri.getPath)
        assert(targetFiles.count(file => file.contains(date)) == 1)
      })
    })

    job.stop()
  }

  def createTestData(): Unit = {
    Map(
      "PAID_SEARCH" -> MetadataEnum.dedupe,
      "DISPLAY" -> MetadataEnum.dedupe,
      "ROI" -> MetadataEnum.dedupe,
      "SOCIAL_MEDIA" -> MetadataEnum.dedupe,
      "SEARCH_ENGINE_FREE_LISTINGS" -> MetadataEnum.dedupe).foreach(kv => {
      val channel = kv._1
      val usage = kv._2

      val metadataImkRTL = Metadata(workDir, channel, usage)
      metadataImkRTL.writeDedupeOutputMeta(MetaFiles(Array(DateFiles("date=2019-12-23", Array(outPutDir + "/" + channel + "/" + usage + "/date=2019-12-23/part-00000.snappy.parquet")))))
      metadataImkRTL.writeDedupeOutputMeta(MetaFiles(Array(DateFiles("date=2019-12-24", Array(outPutDir + "/" + channel + "/" + usage + "/date=2019-12-24/part-00000.snappy.parquet")))))

      fs.copyFromLocalFile(new Path(new File(localDir + "/" + channel + "/date=2019-12-23/part-00000.snappy.parquet").getAbsolutePath), new Path(outPutDir + "/" + channel + "/" + usage + "/date=2019-12-23/part-00000.snappy.parquet"))
      fs.copyFromLocalFile(new Path(new File(localDir + "/" + channel + "/date=2019-12-24/part-00000.snappy.parquet").getAbsolutePath), new Path(outPutDir + "/" + channel + "/" + usage + "/date=2019-12-24/part-00000.snappy.parquet"))
    })

    // prepare keyword file
    fs.copyFromLocalFile(new Path(new File(localDir + "/kwData.csv").getAbsolutePath), new Path(kwDataTempDir + "/kwData.csv"))
    val job = new BaseSparkJob(jobName = "", mode = "local[8]", enableHiveSupport = false) {
      override def run(): Unit = ???
    }
    job.spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(kwDataTempDir + "/kwData.csv").write.parquet(kwDataDir)
  }

  test("test parse mpre from rover url") {
    val job = new ImkETLJob(Parameter(Array(
      "--mode", "local[8]",
      "--channel", "PAID_SEARCH,DISPLAY,ROI,SOCIAL_MEDIA",
      "--workDir", workDir,
      "--outPutDir", outPutDir,
      "--partitions", "1",
      "--elasticsearchUrl", "http://10.148.181.34:9200",
      "--transformedPrefix", "chocolate_",
      "--outputFormat", "sequence",
      "--compressOutPut", "false",
      "--kwDataDir", kwDataDir
    )))
    val mpre = job.replaceMkgroupidMktypeAndParseMpreFromRover("test", "https://rover.ebay.com/rover/1/711-159181-164449-8/1?mpre=http%3A%2F%2Fwww.ebay.com")
    assert("http://www.ebay.com" == mpre)
  }
}
