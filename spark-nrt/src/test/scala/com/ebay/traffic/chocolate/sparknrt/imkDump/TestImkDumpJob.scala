package com.ebay.traffic.chocolate.sparknrt.imkDump

import java.io.File
import java.text.SimpleDateFormat

import com.ebay.app.raptor.chocolate.avro.versions.{FilterMessageV1, FilterMessageV2}
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
  private val imkWorkDir = tmpPath + "/imkWorkDir/"

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
  }

  test("test imk dump job for paid search") {
    val args = Array(
      "--mode", "local[8]",
      "--channel", "PAID_SEARCH",
      "--workDir", workDir,
      "--outPutDir", outPutDir,
      "--imkWorkDir", imkWorkDir,
      "--elasticsearchUrl", "http://10.148.185.16:9200"
    )
    val params = Parameter(args)
    val job = new ImkDumpJob(params)
    val metadata1 = Metadata(workDir, "PAID_SEARCH", MetadataEnum.capping)
    val dedupeMeta = metadata1.readDedupeOutputMeta(".imk")
    val dedupeMetaPath = new Path(dedupeMeta(0)._1)

    assert (fs.exists(dedupeMetaPath))
    job.run()
    val outputFolder = new File(outPutDir + "/PAID_SEARCH/")
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
    metadata.writeDedupeOutputMeta(meta, Array(".imk"))

    // prepare data file
    val writer = AvroParquetWriter.
      builder[GenericRecord](new Path(tmpPath + "/date=2018-05-01/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema)
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()
    val timestamp = getTimestamp("2018-05-01")

    writeFilterMessage(6457493984045429247L, timestamp - 12, -1, "referer:http://www.google.com|User-Agent:shuang(Apple; CPU)|Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8|Accept-Encoding:gzip, deflate, sdch|Accept-Language:en-US,en;q=0.8|Cookie:ebay=%5Esbf%3D%23%5E; nonsession=CgADLAAFY825/NQDKACBiWWj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDjrjVIf; dp1=bbl/USen-US5cb5ce77^; s=CgAD4ACBY9Lj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDhRBcIc; npii=btguid/76cbd9ea15b0a93d12831833fff1c1085ad49dd7^trm/svid%3D1136038334911271815ad49dd7^cguid/76cbd9ea15b0a93d12831833fff1c1065ad49dd7^|Proxy-Connection:keep-alive|Upgrade-Insecure-Requests:1|X-EBAY-CLIENT-IP:157.55.39.67", "http://www.ebay.co.uk/", "Cache-Control:private,no-cache,no-store", writer)
    writer.close()
  }

  def writeFilterMessage(snapshot_id: Long,
                         timestamp: Long,
                         publisher_id: Long,
                         request_headers: String,
                         uri: String,
                         response_headers: String,
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
    message.setSnid("test")
    writer.write(message)
    message
  }

  def getTimestamp(date: String): Long = {
    sdf.parse(date).getTime
  }
}
