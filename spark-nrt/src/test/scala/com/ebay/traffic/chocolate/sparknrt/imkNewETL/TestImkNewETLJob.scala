package com.ebay.traffic.chocolate.sparknrt.imkNewETL

import java.io.File

import com.ebay.traffic.chocolate.spark.{BaseFunSuite}
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import com.ebay.traffic.chocolate.sparknrt.utils.{MyIDV2, XIDResponseV2}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scalaj.http.Http
import spray.json._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class TestImkNewETLJob extends BaseFunSuite{

  private val tmpPath = createTempDir()
  private val workDir = tmpPath + "/apps/tracking-events-workdir"
  private val outPutDir = tmpPath + "/apps/tracking-events"

  private val localDir = getTestResourcePath("imkETL.data")

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
    val job = new ImkNewETLJob(Parameter(Array(
      "--mode", "local[8]",
      "--channel", "PAID_SEARCH,DISPLAY,ROI,SOCIAL_MEDIA",
      "--workDir", workDir,
      "--outPutDir", outPutDir,
      "--partitions", "1",
      "--elasticsearchUrl", "http://10.148.181.34:9200",
      "--transformedPrefix", "chocolate_",
      "--outputFormat", "parquet",
      "--compressOutPut", "false"
    )))

    job.run()

    List("date=2019-12-23", "date=2019-12-24").foreach(date => {
      val targetFiles = fs.listStatus(new Path(outPutDir + "/imkTransform" + "/" + "imkOutput")).map(_.getPath.toUri.getPath)
      assert(targetFiles.count(file => file.contains(date)) == 1)
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
      metadataImkRTL.writeDedupeOutputMeta(MetaFiles(Array(DateFiles("date=2019-12-23", Array(outPutDir + "/" + channel + "/" + usage + "/date=2019-12-23/part-00000.snappy.parquet")))),Array(".imketl"))
      metadataImkRTL.writeDedupeOutputMeta(MetaFiles(Array(DateFiles("date=2019-12-24", Array(outPutDir + "/" + channel + "/" + usage + "/date=2019-12-24/part-00000.snappy.parquet")))),Array(".imketl"))

      fs.copyFromLocalFile(new Path(new File(localDir + "/" + channel + "/date=2019-12-23/part-00000.snappy.parquet").getAbsolutePath), new Path(outPutDir + "/" + channel + "/" + usage + "/date=2019-12-23/part-00000.snappy.parquet"))
      fs.copyFromLocalFile(new Path(new File(localDir + "/" + channel + "/date=2019-12-24/part-00000.snappy.parquet").getAbsolutePath), new Path(outPutDir + "/" + channel + "/" + usage + "/date=2019-12-24/part-00000.snappy.parquet"))
    })
  }

  test("test parse mpre from rover url") {
    val job = new ImkNewETLJob(Parameter(Array(
      "--mode", "local[8]",
      "--channel", "PAID_SEARCH,DISPLAY,ROI,SOCIAL_MEDIA",
      "--workDir", workDir,
      "--outPutDir", outPutDir,
      "--partitions", "1",
      "--elasticsearchUrl", "http://10.148.181.34:9200",
      "--transformedPrefix", "chocolate_",
      "--outputFormat", "parquet",
      "--compressOutPut", "false"
    )))
    val mpre = job.replaceMkgroupidMktypeAndParseMpreFromRover("test", "https://rover.ebay.com/rover/1/711-159181-164449-8/1?mpre=http%3A%2F%2Fwww.ebay.com")
    assert("http://www.ebay.com" == mpre)
  }

  test("test xidRequest"){
    val myID = xidRequest();
    println(myID)
  }

  def xidRequest(): MyIDV2 = {
    Http(s"http://ersxid.vip.qa.ebay.com/anyid/v2/pguid/9eff149d16b3ef43115e30300135be9a")
      .header("X-EBAY-CONSUMER-ID", "urn:ebay-marketplace-consumerid:2e26698a-e3a3-499a-a36f-d34e45276d46")
      .header("X-EBAY-CLIENT-ID", "MarketingTracking")
      .timeout(1000, 1000)
      .asString
      .body
      .parseJson
      .convertTo[XIDResponseV2]
      .toMyIDV2
  }

  test("test judegNotEbaySitesUdf") {
    val job = new ImkNewETLJob(Parameter(Array(
      "--mode", "local[8]",
      "--channel", "PAID_SEARCH,DISPLAY,ROI,SOCIAL_MEDIA",
      "--workDir", workDir,
      "--outPutDir", outPutDir,
      "--partitions", "1",
      "--elasticsearchUrl", "http://10.148.181.34:9200",
      "--transformedPrefix", "chocolate_",
      "--outputFormat", "parquet",
      "--compressOutPut", "false"
    )))

    val data = Seq(
      Row(1, "ROI", "http://www.ebay.com"),
      Row(2, "DISPLAY", "http://www.ebay.com"),
      Row(3, "DISPLAY", "https://ebay.mtag.io/"),
      Row(4, "DISPLAY", "https://ebay.pissedconsumer.com/"),
      Row(5, "DISPLAY", null),
      Row(6, "PAID_SEARCH", null),
      Row(7, "PAID_SEARCH", "https://ebay.pissedconsumer.com/")
    )

    val schema = List(
      StructField("id", IntegerType, nullable = true),
      StructField("channel_type", StringType, nullable = true),
      StructField("referer", StringType, nullable = true)
    )

    val df: DataFrame = job.spark.createDataFrame(
      job.spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    val results = df.filter(job.judegNotEbaySitesUdf(col("channel_type"), col("referer")))
    results.show()
    val list = results.select(col("id")).collectAsList();
    assert(list.get(0).getInt(0) == 1)
    assert(list.get(1).getInt(0) == 3)
    assert(list.get(2).getInt(0) == 4)
    assert(list.get(3).getInt(0) == 5)
    assert(list.get(4).getInt(0) == 6)
  }
}
