package com.ebay.traffic.chocolate.sparknrt.imkDump

import java.io.File
import java.text.SimpleDateFormat

import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV2
import com.ebay.app.raptor.chocolate.avro.{ChannelAction, ChannelType, FilterMessage, HttpMethod}
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
  * Created by ganghuang on 12/3/18.
  */
class TestImkDumpJob extends BaseFunSuite{

  private val tmpPath = createTempDir()
  private val workDir = tmpPath + "/workDir/"
  private val outPutDir = tmpPath + "/outPutDir/"

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

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
    createTestDataForROI()
    fs.mkdirs(new Path(workDir))
    fs.mkdirs(new Path(outPutDir))
  }

  test("test imk dump job for paid search") {
    val args = Array(
      "--mode", "local[8]",
      "--channel", "PAID_SEARCH",
      "--workDir", workDir,
      "--outPutDir", outPutDir,
      "--partitions", "1",
      "--elasticsearchUrl", "http://10.148.181.34:9200"
    )
    val params = Parameter(args)
    val job = new ImkDumpJob(params)
    val metadata1 = Metadata(workDir, "PAID_SEARCH", MetadataEnum.capping)
    val dedupeMeta = metadata1.readDedupeOutputMeta(".epnnrt")
    val dedupeMetaPath = new Path(dedupeMeta(0)._1)

    assert (fs.exists(dedupeMetaPath))
    job.run()
    val outputFolder = new File(outPutDir + "/PAID_SEARCH/imkDump/date=2018-05-01")
    assert(outputFolder.listFiles().length > 0)
    outputFolder.listFiles().foreach(file => {
      assert(file.toPath.toString.contains("imk_rvr_trckng_"))
    })
    job.stop()

  }

  test("test imk dump job for roi events") {
    val args = Array(
      "--mode", "local[8]",
      "--channel", "ROI",
      "--workDir", workDir,
      "--outPutDir", outPutDir,
      "--partitions", "1",
      "--elasticsearchUrl", "http://10.148.181.34:9200"
    )
    val params = Parameter(args)
    val job = new ImkDumpRoiJob(params)
    val metadata1 = Metadata(workDir, "ROI", MetadataEnum.dedupe)
    val dedupeMeta = metadata1.readDedupeOutputMeta("")
    val dedupeMetaPath = new Path(dedupeMeta(0)._1)

    assert (fs.exists(dedupeMetaPath))
    job.run()
    val outputFolder = new File(outPutDir + "/ROI/imkDump/date=2018-05-01")
    assert(outputFolder.listFiles().length > 0)
    outputFolder.listFiles().foreach(file => {
      assert(file.toPath.toString.contains("imk_rvr_trckng_"))
    })
    job.stop()
  }

  def createTestDataForPS(): Unit = {
    val metadata = Metadata(workDir, "PAID_SEARCH", MetadataEnum.capping)
    val dateFiles = DateFiles("date=2018-05-01", Array("file://" + tmpPath + "/date=2018-05-01/part-00000.snappy.parquet"))
    val meta: MetaFiles = MetaFiles(Array(dateFiles))
    metadata.writeDedupeOutputMeta(meta, Array(".epnnrt"))

    // prepare data file
    val writer = AvroParquetWriter.
      builder[GenericRecord](new Path(tmpPath + "/date=2018-05-01/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV2.getClassSchema)
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()
    val timestamp = getTimestamp("2018-05-01")

    writeFilterMessage(6457493984045429247L, timestamp - 12,
      -1,
      "X-EBAY-CLIENT-IP:157.55.39.67|X-EBAY-C-TRACKING: guid=cc3af5c11660ac3d8844157cff04c381,cguid=cc3af5c71660ac3d8844157cff04c37c,tguid=cc3af5c11660ac3d8844157cff04c381,pageid=2067260,cobrandId=2|Referer:http://www.google.com|X-EBAY-C-ENDUSERCTX: userAgent=ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;no-carrier;414x736;3.0,deviceId=16178ec6e70.a88b147.489a0.fefc1716,deviceIdType=IDREF,contextualLocation=country%3DUS%2Cstate%3DCA%2Czip%3D95134|userid:123456|geoid:123456",
      "http://www.ebay.co.uk/",
      "Cache-Control:private,no-cache,no-store",
      "test",
      "cc3af5c71660ac3d8844157cff04c37c",
      "cc3af5c11660ac3d8844157cff04c381",
      123L,
      "157.55.39.67",
      "123",
      "ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;no-carrier;414x736;3.0",
      "1111",
      "https://www.google.com",
      "http://www.ebay.com",
      writer)
    writeFilterMessage(6457493984045429249L, timestamp - 12,
      -1,
      "X-EBAY-CLIENT-IP:157.55.39.67|X-EBAY-C-TRACKING: guid=cc3af5c11660ac3d8844157cff04c381,cguid=cc3af5c71660ac3d8844157cff04c37c,tguid=cc3af5c11660ac3d8844157cff04c381,pageid=2067260,cobrandId=2|Referer:http://www.google.com|X-EBAY-C-ENDUSERCTX: userAgent=ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;no-carrier;414x736;3.0,deviceId=16178ec6e70.a88b147.489a0.fefc1716,deviceIdType=IDREF,contextualLocation=country%3DUS%2Cstate%3DCA%2Czip%3D95134|userid:123456|geoid:123456",
      "http://www.ebay.co.uk/",
      "Cache-Control:private,no-cache,no-store",
      "test",
      "",
      "765135e71690a93f1ad44989ff43e508",
      123L,
      "157.55.39.67",
      "123",
      "ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;no-carrier;414x736;3.0",
      "1111",
      "https://www.google.com",
      "http://www.ebay.com",
      writer)
    writer.close()
  }


  def createTestDataForROI(): Unit = {
    val metadata = Metadata(workDir, "ROI", MetadataEnum.dedupe)
    val dateFiles = DateFiles("date=2018-05-01", Array("file://" + tmpPath + "/date=2018-05-01/part-00001.snappy.parquet"))
    val meta: MetaFiles = MetaFiles(Array(dateFiles))
    metadata.writeDedupeOutputMeta(meta, Array(""))

    // prepare data file
    val writer = AvroParquetWriter.
      builder[GenericRecord](new Path(tmpPath + "/date=2018-05-01/part-00001.snappy.parquet"))
      .withSchema(FilterMessageV2.getClassSchema)
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()
    val timestamp = getTimestamp("2018-05-01")

    writeFilterMessage(6457493984045429247L, timestamp - 12,
      -1,
      "X-EBAY-CLIENT-IP:157.55.39.67|X-EBAY-C-TRACKING: guid=cc3af5c11660ac3d8844157cff04c381,cguid=cc3af5c71660ac3d8844157cff04c37c,tguid=cc3af5c11660ac3d8844157cff04c381,pageid=2067260,cobrandId=2|Referer:http://www.google.com|X-EBAY-C-ENDUSERCTX: userAgent=ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;no-carrier;414x736;3.0,deviceId=16178ec6e70.a88b147.489a0.fefc1716,deviceIdType=IDREF,contextualLocation=country%3DUS%2Cstate%3DCA%2Czip%3D95134|userid:123456|geoid:123456",
      "https://rover.ebay.com/roverroi/1/707-515-1801-16?siteId=77&tranType=LocClass-FreeAd&LocClass-FreeAd=1&mpuid=53189805;1174241238;53189805;3274;195;203;;;;1564652607898&raptor=1",
      "Cache-Control:private,no-cache,no-store",
      "test",
      "cc3af5c71660ac3d8844157cff04c37c",
      "cc3af5c11660ac3d8844157cff04c381",
      123L,
      "157.55.39.67",
      "123",
      "ebayUserAgent/eBayIOS;5.19.0;iOS;11.2;Apple;x86_64;no-carrier;414x736;3.0",
      "1111",
      "https://www.ebay-kleinanzeigen.de/p-anzeige-aufgeben-bestaetigung.html?adId=1174241238&uuid=d7d8ce73-4a1e-466d-a769-d993eda747bb",
      "",
      writer)
    writer.close()
  }

  def writeFilterMessage(snapshot_id: Long,
                         timestamp: Long,
                         publisher_id: Long,
                         request_headers: String,
                         uri: String,
                         response_headers: String,
                         snid: String,
                         cguid: String,
                         guid: String,
                         geo_id: Long,
                         remote_ip: String,
                         lang_cd: String,
                         user_agent: String,
                         udid: String,
                         referer: String,
                         landing_page_url: String,
                         writer: ParquetWriter[GenericRecord]): FilterMessage = {
    val message = new FilterMessage()
    message.setSnapshotId(snapshot_id)
    message.setTimestamp(timestamp)
    message.setPublisherId(publisher_id)
    message.setRequestHeaders(request_headers)
    message.setResponseHeaders(response_headers)
    message.setChannelType(ChannelType.PAID_SEARCH)
    message.setChannelAction(ChannelAction.CLICK)
    message.setUri(uri)
    message.setHttpMethod(HttpMethod.GET)
    message.setSnid(snid)
    message.setCguid(cguid)
    message.setGuid(guid)
    message.setGeoId(geo_id)
    message.setRemoteIp(remote_ip)
    message.setLangCd(lang_cd)
    message.setUserAgent(user_agent)
    message.setUdid(udid)
    message.setReferer(referer)
    message.setLandingPageUrl(landing_page_url)
    writer.write(message)
    message
  }

  def getTimestamp(date: String): Long = {
    sdf.parse(date).getTime
  }
}
