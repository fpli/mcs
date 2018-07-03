package com.ebay.traffic.chocolate.sparknrt.reporting

import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV1
import com.ebay.app.raptor.chocolate.avro.{ChannelAction, ChannelType, FilterMessage}
import com.ebay.traffic.chocolate.common.TestHelper
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.couchbase.{CouchbaseClient, CouchbaseClientMock}
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
  * Created by weibdai on 5/19/18.
  */
class TestReportingJob extends BaseFunSuite {

  val tmpPath = createTempPath()
  val inputDir = tmpPath + "/inputDir/"
  val workDir = tmpPath + "/workDir/"

  val channel = "EPN"

  val args = Array(
    "--mode", "local[8]",
    "--channel", channel,
    "--workDir", workDir
  )

  @transient lazy val hadoopConf = {
    new Configuration()
  }

  lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  val params = Parameter(args)
  val job = new ReportingJob(params)

  override def beforeAll(): Unit = {
    CouchbaseClientMock.startCouchbaseMock()
    CouchbaseClient.createClusterFunc = () => CouchbaseClientMock.connect()
    createTestDataForDedupe()
  }

  override def afterAll(): Unit = {
    // Call this before close mock...
    //CouchbaseClient.close(CouchbaseClient.reportBucket)
    //CouchbaseClientMock.closeCouchbaseMock()
  }

  test("Test Reporting") {

    val metadata1 = Metadata(workDir, channel, MetadataEnum.capping)
    val dedupeMeta = metadata1.readDedupeOutputMeta()

    assert (fs.exists(new Path(dedupeMeta(0)._1)))

    job.run()
    job.stop()

    assert (!fs.exists(new Path(dedupeMeta(0)._1)))

    // check against mock Couchbase...
    val bucket = CouchbaseClient.reportBucket

    val keyArray = Array(
      // publisher based report result...
      "PUBLISHER_11_2018-05-01_CLICK_MOBILE_FILTERED",
      "PUBLISHER_11_2018-05-01_CLICK_DESKTOP_FILTERED",
      "PUBLISHER_11_2018-05-01_CLICK_MOBILE_RAW",
      "PUBLISHER_11_2018-05-01_CLICK_DESKTOP_RAW",
      "PUBLISHER_22_2018-05-01_CLICK_MOBILE_FILTERED",
      "PUBLISHER_22_2018-05-01_CLICK_DESKTOP_RAW",
      "PUBLISHER_22_2018-05-01_CLICK_DESKTOP_FILTERED",
      "PUBLISHER_22_2018-05-01_CLICK_MOBILE_RAW",
      "PUBLISHER_11_2018-05-01_IMPRESSION_DESKTOP_RAW",
      "PUBLISHER_11_2018-05-01_IMPRESSION_MOBILE_FILTERED",
      "PUBLISHER_11_2018-05-01_IMPRESSION_MOBILE_RAW",
      "PUBLISHER_11_2018-05-01_IMPRESSION_DESKTOP_FILTERED",
      "PUBLISHER_22_2018-05-01_IMPRESSION_MOBILE_RAW",
      "PUBLISHER_22_2018-05-01_IMPRESSION_MOBILE_FILTERED",
      "PUBLISHER_22_2018-05-01_IMPRESSION_DESKTOP_FILTERED",
      "PUBLISHER_22_2018-05-01_IMPRESSION_DESKTOP_RAW",
      "PUBLISHER_11_2018-05-01_VIEWABLE_MOBILE_RAW",
      "PUBLISHER_11_2018-05-01_VIEWABLE_MOBILE_FILTERED",
      "PUBLISHER_11_2018-05-01_VIEWABLE_DESKTOP_FILTERED",
      "PUBLISHER_11_2018-05-01_VIEWABLE_DESKTOP_RAW",
      "PUBLISHER_22_2018-05-01_VIEWABLE_MOBILE_RAW",
      "PUBLISHER_22_2018-05-01_VIEWABLE_MOBILE_FILTERED",
      "PUBLISHER_22_2018-05-01_VIEWABLE_DESKTOP_RAW",
      "PUBLISHER_22_2018-05-01_VIEWABLE_DESKTOP_FILTERED",
      // campaign based report...
      "PUBLISHER_11_CAMPAIGN_111_2018-05-01_CLICK_MOBILE_FILTERED",
      "PUBLISHER_11_CAMPAIGN_111_2018-05-01_CLICK_DESKTOP_FILTERED",
      "PUBLISHER_11_CAMPAIGN_111_2018-05-01_CLICK_MOBILE_RAW",
      "PUBLISHER_11_CAMPAIGN_111_2018-05-01_CLICK_DESKTOP_RAW",
      "PUBLISHER_11_CAMPAIGN_111_2018-05-01_IMPRESSION_DESKTOP_RAW",
      "PUBLISHER_11_CAMPAIGN_111_2018-05-01_IMPRESSION_MOBILE_FILTERED",
      "PUBLISHER_11_CAMPAIGN_111_2018-05-01_IMPRESSION_MOBILE_RAW",
      "PUBLISHER_11_CAMPAIGN_111_2018-05-01_IMPRESSION_DESKTOP_FILTERED",
      "PUBLISHER_11_CAMPAIGN_111_2018-05-01_VIEWABLE_MOBILE_RAW",
      "PUBLISHER_11_CAMPAIGN_111_2018-05-01_VIEWABLE_MOBILE_FILTERED",
      "PUBLISHER_11_CAMPAIGN_111_2018-05-01_VIEWABLE_DESKTOP_FILTERED",
      "PUBLISHER_11_CAMPAIGN_111_2018-05-01_VIEWABLE_DESKTOP_RAW",

      "PUBLISHER_22_CAMPAIGN_222_2018-05-01_CLICK_MOBILE_FILTERED",
      "PUBLISHER_22_CAMPAIGN_222_2018-05-01_CLICK_DESKTOP_RAW",
      "PUBLISHER_22_CAMPAIGN_222_2018-05-01_CLICK_DESKTOP_FILTERED",
      "PUBLISHER_22_CAMPAIGN_222_2018-05-01_CLICK_MOBILE_RAW",
      "PUBLISHER_22_CAMPAIGN_222_2018-05-01_IMPRESSION_MOBILE_RAW",
      "PUBLISHER_22_CAMPAIGN_222_2018-05-01_IMPRESSION_MOBILE_FILTERED",
      "PUBLISHER_22_CAMPAIGN_222_2018-05-01_IMPRESSION_DESKTOP_FILTERED",
      "PUBLISHER_22_CAMPAIGN_222_2018-05-01_IMPRESSION_DESKTOP_RAW",
      "PUBLISHER_22_CAMPAIGN_222_2018-05-01_VIEWABLE_MOBILE_RAW",
      "PUBLISHER_22_CAMPAIGN_222_2018-05-01_VIEWABLE_MOBILE_FILTERED",
      "PUBLISHER_22_CAMPAIGN_222_2018-05-01_VIEWABLE_DESKTOP_RAW",
      "PUBLISHER_22_CAMPAIGN_222_2018-05-01_VIEWABLE_DESKTOP_FILTERED"
    )

    for (i <- keyArray.indices) {
      assert(bucket.exists(keyArray(i)))
      println("key: " + keyArray(i) + " value: " + bucket.get(keyArray(i)).content().toString)
    }
  }

  def getTimestamp(date: String): Long = {
    job.sdf.parse(date).getTime
  }

  def writeFilterMessage(channelType: ChannelType,
                         channelAction: ChannelAction,
                         snapshotId: Long,
                         publisherId: Long,
                         campaignId: Long,
                         timestamp: Long,
                         rtRuleFlags: Long,
                         nrtRuleFlags: Long,
                         isMobi: Boolean,
                         writer: ParquetWriter[GenericRecord]): FilterMessage = {
    val message = TestHelper.newFilterMessage(channelType,
      channelAction,
      snapshotId,
      publisherId,
      campaignId,
      timestamp,
      rtRuleFlags,
      nrtRuleFlags,
      isMobi)
    writer.write(message)
    message
  }

  def createTestDataForDedupe(): Unit = {
    // prepare metadata file
    val metadata = Metadata(workDir, channel, MetadataEnum.capping)

    val dateFiles = DateFiles("date=2018-05-01", Array("file://" + inputDir + "/date=2018-05-01/part-00000.snappy.parquet"))
    var meta: MetaFiles = MetaFiles(Array(dateFiles))

    metadata.writeDedupeOutputMeta(meta)

    // prepare data file
    val writer = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-05-01/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema)
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val timestamp = getTimestamp("2018-05-01")

    // Desktop
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 1L, 11L, 111L, timestamp - 12, 1, 0, false, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 2L, 11L, 111L, timestamp - 11, 0, 0, false, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 3L, 22L, 222L, timestamp - 10, 1, 0, false, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 4L, 22L, 222L, timestamp - 9, 0, 0, false, writer)

    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 5L, 11L, 111L, timestamp - 8, 1, 0, false, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 6L, 11L, 111L, timestamp - 7, 0, 0, false, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 7L, 22L, 222L, timestamp - 6, 1, 0, false, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 8L, 22L, 222L, timestamp - 5, 0, 0, false, writer)

    writeFilterMessage(ChannelType.EPN, ChannelAction.VIEWABLE, 9L, 11L, 111L, timestamp - 4, 1, 0, false, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.VIEWABLE, 10L, 11L, 111L, timestamp - 3, 0, 0, false, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.VIEWABLE, 11L, 22L, 222L, timestamp - 2, 1, 0, false, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.VIEWABLE, 12L, 22L, 222L, timestamp - 1, 0, 0, false, writer)

    // Mobile
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 13L, 11L, 111L, timestamp - 12, 1, 0, true, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 14L, 11L, 111L, timestamp - 11, 0, 0, true, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 15L, 22L, 222L, timestamp - 10, 1, 0, true, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 16L, 22L, 222L, timestamp - 9, 0, 0, true, writer)

    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 17L, 11L, 111L, timestamp - 8, 1, 0, true, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 18L, 11L, 111L, timestamp - 7, 0, 0, true, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 19L, 22L, 222L, timestamp - 6, 1, 0, true, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 20L, 22L, 222L, timestamp - 5, 0, 0, true, writer)

    writeFilterMessage(ChannelType.EPN, ChannelAction.VIEWABLE, 21L, 11L, 111L, timestamp - 4, 1, 0, true, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.VIEWABLE, 22L, 11L, 111L, timestamp - 3, 0, 0, true, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.VIEWABLE, 23L, 22L, 222L, timestamp - 2, 1, 0, true, writer)
    writeFilterMessage(ChannelType.EPN, ChannelAction.VIEWABLE, 24L, 22L, 222L, timestamp - 1, 0, 0, true, writer)

    writer.close()
  }
}
