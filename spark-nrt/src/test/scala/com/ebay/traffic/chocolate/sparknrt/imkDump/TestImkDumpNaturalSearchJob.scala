package com.ebay.traffic.chocolate.sparknrt.imkDump

import java.io.File
import java.text.SimpleDateFormat

import com.ebay.app.raptor.chocolate.avro.versions.{FilterMessageV2, FilterMessageV4}
import com.ebay.app.raptor.chocolate.avro.{ChannelAction, ChannelType, FilterMessage, HttpMethod}
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

class TestImkDumpNaturalSearchJob extends BaseFunSuite{
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
    createTestDataForNaturalSearch()
    fs.mkdirs(new Path(workDir))
    fs.mkdirs(new Path(outPutDir))
  }

  test("test imk dump job for natural search events") {
    val args = Array(
      "--mode", "local[8]",
      "--channel", "NATURAL_SEARCH",
      "--workDir", workDir,
      "--outPutDir", outPutDir,
      "--partitions", "1",
      "--elasticsearchUrl", "http://10.148.181.34:9200"
    )
    val params = Parameter(args)
    val job = new ImkDumpNSJob(params)
    val metadata1 = Metadata(workDir, "NATURAL_SEARCH", MetadataEnum.dedupe)
    val dedupeMeta = metadata1.readDedupeOutputMeta("")
    val dedupeMetaPath = new Path(dedupeMeta(0)._1)

    assert (fs.exists(dedupeMetaPath))
    job.run()
    val outputFolder = new File(outPutDir + "/NATURAL_SEARCH/imkDump/date=2018-05-01")
    assert(outputFolder.listFiles().length > 0)
    outputFolder.listFiles().foreach(file => {
      assert(file.toPath.toString.contains("imk_rvr_trckng_"))
    })
    job.stop()
  }

  def createTestDataForNaturalSearch(): Unit = {
    val metadata = Metadata(workDir, "NATURAL_SEARCH", MetadataEnum.dedupe)
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
      "X-EBAY-CLIENT-IP:157.55.39.67|X-EBAY-C-TRACKING: guid=1894593816e0aadaa2b49dcee8530255,cguid=1894593e16e0aadaa2b49dcee8530253,tguid=cc3af5c11660ac3d8844157cff04c381,pageid=3085,cobrandId=2|Referer:https://www.ebay.co.uk/|X-EBAY-C-ENDUSERCTX: userAgent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.18362,deviceId=16178ec6e70.a88b147.489a0.fefc1716,deviceIdType=IDREF,contextualLocation=country%3DUS%2Cstate%3DCA%2Czip%3D95134|userid:123456|geoid:123456",
      "https://rover.ebay.com/roverns/1/710-16388-7832-0?mpt=1572370403910&mpcl=https%3A%2F%2Fwww.ebay.co.uk%2F&mpvl=https%3A%2F%2Fwww.bing.com%2Fsearch%3Fq%3Debay%26form%3DEDNTHT%26mkt%3Den-gb%26httpsmsn%3D1%26msnews%3D1%26plvar%3D0%26refig%3Dde0359d2287f4fc6f0d43c815214d153%26sp%3D1%26qs%3DAS%26pq%3Deb%26sk%3DPRES1%26sc%3D8-2%26cvid%3Dde0359d2287f4fc6f0d43c815214d153%26cc%3DGB%26setlang%3Den-GB&itemId=111&geo_id=222",
      "Cache-Control:private,no-cache,no-store",
      "test",
      "1894593e16e0aadaa2b49dcee8530253",
      "1894593816e0aadaa2b49dcee8530255",
      0L,
      "157.55.39.67",
      "123",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.102 Safari/537.36 Edge/18.18362",
      "",
      "https://www.ebay.co.uk/",
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
    message.setChannelType(ChannelType.NATURAL_SEARCH)
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
