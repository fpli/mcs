package com.ebay.traffic.chocolate.sparknrt.sword

import java.text.SimpleDateFormat
import java.util

import com.ebay.app.raptor.chocolate.avro.{ChannelAction, ChannelType, FilterMessage}
import com.ebay.traffic.chocolate.common.{KafkaTestHelper, MiniKafkaCluster, TestHelper}
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

class TestSwordJob extends BaseFunSuite {
  var kafkaCluster: MiniKafkaCluster = null
  val tmpPath = createTempPath()
  val workDir = tmpPath + "/workDir/"
  val dataDir = tmpPath + "/dataDir/"
  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  val topic = "test-kafka-topic"
  val channel = "EPN"

  @transient private lazy val hadoopConf = {
    new Configuration()
  }

  private lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  override def beforeAll(): Unit = {
    kafkaCluster = KafkaTestHelper.newKafkaCluster()
    createTestDataForEPN()
    createTestDataForDisplay()
  }

  test("Test Sword") {
    val bootstrapServers = kafkaCluster.getProducerProperties(classOf[org.apache.kafka.common.serialization.LongSerializer],
      classOf[org.apache.kafka.common.serialization.ByteArraySerializer]).getProperty("bootstrap.servers")

    val args = Array(
      "--mode", "local[8]",
      "--channel", channel,
      "--kafkaTopic", topic,
      "--workDir", workDir,
      "--bootstrapServers", bootstrapServers,
      "--dataDir", dataDir
    )
    val params = Parameter(args)
    val job = new SwordJob(params)
    val metadata1 = Metadata(workDir, "EPN", MetadataEnum.capping)
    val dedupeMeta = metadata1.readDedupeOutputMeta(".detection")
    val dedupeMetaPath = new Path(dedupeMeta(0)._1)

    assert (fs.exists(dedupeMetaPath))
    job.run()
    val consumer = kafkaCluster.createConsumer(classOf[org.apache.kafka.common.serialization.LongDeserializer],
      classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer])
    consumer.subscribe(util.Arrays.asList(topic))

    val records = consumer.poll(10000)
    assert (records.count() === 4)
    assert (!fs.exists(dedupeMetaPath))
    consumer.close()
    job.stop()

  }

    override def afterAll(): Unit = {
    KafkaTestHelper.shutdown()
  }

  def createTestDataForEPN(): Unit = {
    // prepare metadata file
    val metadata = Metadata(workDir, "EPN", MetadataEnum.capping)

    val dateFiles = DateFiles("date=2018-05-01", Array("file://" + dataDir + "/date=2018-05-01/part-00000.snappy.parquet"))
    val meta: MetaFiles = MetaFiles(Array(dateFiles))

    metadata.writeDedupeOutputMeta(meta, Array(".detection"))

    // prepare data file
    val writer = AvroParquetWriter.
      builder[GenericRecord](new Path(dataDir + "/date=2018-05-01/part-00000.snappy.parquet"))
      .withSchema(FilterMessage.getClassSchema)
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
    writer.close()
  }

  def createTestDataForDisplay(): Unit = {
    // prepare metadata file
    val metadata = Metadata(workDir, "DISPLAY", MetadataEnum.capping)

    val dateFiles = DateFiles("date=2018-05-02", Array("file://" + dataDir + "/date=2018-05-02/part-00000.snappy.parquet"))
    val meta: MetaFiles = MetaFiles(Array(dateFiles))
    metadata.writeDedupeOutputMeta(meta, Array(".detection"))
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

}
