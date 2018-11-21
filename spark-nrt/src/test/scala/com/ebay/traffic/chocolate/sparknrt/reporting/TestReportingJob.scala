package com.ebay.traffic.chocolate.sparknrt.reporting

import java.text.SimpleDateFormat

import com.couchbase.client.java.document.JsonArrayDocument
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV1
import com.ebay.app.raptor.chocolate.avro.{ChannelAction, ChannelType, FilterMessage}
import com.ebay.traffic.chocolate.common.TestHelper
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.couchbase.{CorpCouchbaseClient, CouchbaseClientMock}
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

  private val tmpPath = createTempPath()
  private val inputDir = tmpPath + "/inputDir/"
  private val workDir = tmpPath + "/workDir/"
  private val archiveDir = tmpPath + "/archiveDir/"

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
    CouchbaseClientMock.startCouchbaseMock()
    CorpCouchbaseClient.getBucketFunc = () => {
      (None, CouchbaseClientMock.connect().openBucket("default"))
    }

    createTestDataForEPN()
    createTestDataForDisplay()
  }

  override def afterAll(): Unit = {
    // Call this before close mock...
    //CouchbaseClient.close(CouchbaseClient.reportBucket)
    //CouchbaseClientMock.closeCouchbaseMock()
  }

  test("Test EPN reporting job") {

    val args = Array(
      "--mode", "local[8]",
      "--channel", "EPN",
      "--workDir", workDir,
      "--archiveDir", archiveDir,
      "--elasticsearchUrl", "http://10.148.181.34:9200"
    )

    val params = Parameter(args)
    val job = new ReportingJob(params)

    val metadata1 = Metadata(workDir, "EPN", MetadataEnum.capping)
    val dedupeMeta = metadata1.readDedupeOutputMeta()
    val dedupeMetaPath = new Path(dedupeMeta(0)._1)

    assert (fs.exists(dedupeMetaPath))

    job.run()
    job.stop()

    assert (!fs.exists(dedupeMetaPath)) // moved
    assert (fs.exists(new Path(job.archiveDir, dedupeMetaPath.getName))) // archived

    // check against mock Couchbase...
    val bucket = CorpCouchbaseClient.getBucketFunc.apply()._2

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
      println("key: " + keyArray(i) + " value: " + bucket.get(keyArray(i), classOf[JsonArrayDocument]).content().toString)
    }
  }

  test("Test Display reporting job") {

    val args = Array(
      "--mode", "local[8]",
      "--channel", "DISPLAY",
      "--workDir", workDir,
      "--archiveDir", archiveDir,
      "--elasticsearchUrl", "http://10.148.181.34:9200"
    )

    val params = Parameter(args)
    val job = new ReportingJob(params)

    val metadata1 = Metadata(workDir, "DISPLAY", MetadataEnum.capping)
    val dedupeMeta = metadata1.readDedupeOutputMeta()
    val dedupeMetaPath = new Path(dedupeMeta(0)._1)

    assert (fs.exists(dedupeMetaPath))

    job.run()
    job.stop()

    assert (!fs.exists(dedupeMetaPath)) // moved
    assert (fs.exists(new Path(job.archiveDir, dedupeMetaPath.getName))) // archived

    // check against mock Couchbase...
    val bucket = CorpCouchbaseClient.getBucketFunc.apply()._2

    val keyArray = Array(
      "ROTATION_707-53477-19255-1_2018-05-02_CLICK_MOBILE_FILTERED",
      "ROTATION_707-53477-19255-0_2018-05-02_CLICK_DESKTOP_FILTERED",
      "ROTATION_707-53477-19255-1_2018-05-02_CLICK_MOBILE_RAW",
      "ROTATION_707-53477-19255-0_2018-05-02_CLICK_DESKTOP_RAW",
      "ROTATION_707-53477-19255-1_2018-05-02_IMPRESSION_MOBILE_FILTERED",
      "ROTATION_707-53477-19255-0_2018-05-02_IMPRESSION_DESKTOP_FILTERED",
      "ROTATION_707-53477-19255-1_2018-05-02_IMPRESSION_MOBILE_RAW",
      "ROTATION_707-53477-19255-0_2018-05-02_IMPRESSION_DESKTOP_RAW",
      "ROTATION_707-53477-19255-1_2018-05-02_VIEWABLE_MOBILE_FILTERED",
      "ROTATION_707-53477-19255-0_2018-05-02_VIEWABLE_DESKTOP_FILTERED",
      "ROTATION_707-53477-19255-1_2018-05-02_VIEWABLE_MOBILE_RAW",
      "ROTATION_707-53477-19255-0_2018-05-02_VIEWABLE_DESKTOP_RAW"
    )

    for (i <- keyArray.indices) {
      assert(bucket.exists(keyArray(i)))
      println("key: " + keyArray(i) + " value: " + bucket.get(keyArray(i), classOf[JsonArrayDocument]).content().toString)
    }
  }

  test("Test rotation id parser") {

    val args = Array(
      "--mode", "local[8]",
      "--channel", "DISPLAY",
      "--workDir", workDir,
      "--archiveDir", archiveDir
    )

    val params = Parameter(args)
    val job = new ReportingJob(params)

    val r1 = job.getRotationId("http://rover.ebay.com/rover/1/707-53477-19255-0/4?test=display")
    assertResult("707-53477-19255-0")(r1)
    val r2 = job.getRotationId("https://rover.ebay.com/rover/1/707-53477-19255-1/4")
    assertResult("707-53477-19255-1")(r2)
    val r3 = job.getRotationId("https://rover.ebay.com/rover/1/abc707-53477-19255-1/4")
    assertResult("-1")(r3)
    val r4 = job.getRotationId("https://rover.ebay.com/rover/1/707707-53477534775-19255192551-11/4")
    assertResult("-1")(r4)
  }

  def getTimestamp(date: String): Long = {
    sdf.parse(date).getTime
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

  def writeFilterMessage(channelType: ChannelType,
                         channelAction: ChannelAction,
                         snapshotId: Long,
                         uri: String,
                         timestamp: Long,
                         rtRuleFlags: Long,
                         nrtRuleFlags: Long,
                         isMobi: Boolean,
                         writer: ParquetWriter[GenericRecord]): FilterMessage = {
    val message = TestHelper.newFilterMessage(channelType,
      channelAction,
      snapshotId,
      -1L,
      -1L,
      timestamp,
      rtRuleFlags,
      nrtRuleFlags,
      isMobi)
    message.setUri(uri)
    writer.write(message)
    message
  }

  def createTestDataForEPN(): Unit = {
    // prepare metadata file
    val metadata = Metadata(workDir, "EPN", MetadataEnum.capping)

    val dateFiles = DateFiles("date=2018-05-01", Array("file://" + inputDir + "/date=2018-05-01/part-00000.snappy.parquet"))
    val meta: MetaFiles = MetaFiles(Array(dateFiles))

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

  def createTestDataForDisplay(): Unit = {
    // prepare metadata file
    val metadata = Metadata(workDir, "DISPLAY", MetadataEnum.capping)

    val dateFiles = DateFiles("date=2018-05-02", Array("file://" + inputDir + "/date=2018-05-02/part-00000.snappy.parquet"))
    val meta: MetaFiles = MetaFiles(Array(dateFiles))

    metadata.writeDedupeOutputMeta(meta)

    // prepare data file
    val writer = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-05-02/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema)
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val timestamp = getTimestamp("2018-05-02")

    val uri1 = "http://rover.ebay.com/rover/1/707-53477-19255-0/4?test=display"
    val uri2 = "https://rover.ebay.com/rover/1/707-53477-19255-1/4"

    // Desktop
    writeFilterMessage(ChannelType.DISPLAY, ChannelAction.CLICK, 1L, uri1, timestamp - 12, 1, 0, false, writer)
    writeFilterMessage(ChannelType.DISPLAY, ChannelAction.CLICK, 2L, uri1, timestamp - 11, 0, 0, false, writer)

    writeFilterMessage(ChannelType.DISPLAY, ChannelAction.IMPRESSION, 3L, uri1, timestamp - 8, 1, 0, false, writer)
    writeFilterMessage(ChannelType.DISPLAY, ChannelAction.IMPRESSION, 4L, uri1, timestamp - 7, 0, 0, false, writer)

    writeFilterMessage(ChannelType.DISPLAY, ChannelAction.VIEWABLE, 5L, uri1, timestamp - 4, 1, 0, false, writer)
    writeFilterMessage(ChannelType.DISPLAY, ChannelAction.VIEWABLE, 6L, uri1, timestamp - 3, 0, 0, false, writer)

    // Mobile
    writeFilterMessage(ChannelType.DISPLAY, ChannelAction.CLICK, 7L, uri2, timestamp - 12, 1, 0, true, writer)
    writeFilterMessage(ChannelType.DISPLAY, ChannelAction.CLICK, 8L, uri2, timestamp - 11, 0, 0, true, writer)

    writeFilterMessage(ChannelType.DISPLAY, ChannelAction.IMPRESSION, 9L, uri2, timestamp - 8, 1, 0, true, writer)
    writeFilterMessage(ChannelType.DISPLAY, ChannelAction.IMPRESSION, 10L, uri2, timestamp - 7, 0, 0, true, writer)

    writeFilterMessage(ChannelType.DISPLAY, ChannelAction.VIEWABLE, 11L, uri2, timestamp - 4, 1, 0, true, writer)
    writeFilterMessage(ChannelType.DISPLAY, ChannelAction.VIEWABLE, 12L, uri2, timestamp - 3, 0, 0, true, writer)

    writer.close()
  }
}