package com.ebay.traffic.chocolate.sparknrt.capping

import com.ebay.app.raptor.chocolate.avro.{ChannelAction, ChannelType, FilterMessage}
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV1
import com.ebay.traffic.chocolate.common.TestHelper
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

class TestCappingRuleJob extends BaseFunSuite {
  val tmpPath = createTempPath()
  val inputDir = tmpPath + "/inputDir/"
  val workDir = tmpPath + "/workDir/"
  val outputDir = tmpPath + "/outputDir/"
  val ipThreshold = "5"
  val channel = "EPN"

  val args = Array(
    "--mode", "local[8]",
    "--channel", channel,
    "--inputDir", inputDir,
    "--workDir", workDir,
    "--outputDir", outputDir,
    "--ipThreshold", ipThreshold
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
  val job = new CappingRuleJob(params)

  def getTimestamp(date: String): Long = {
    job.sdf.parse(date).getTime
  }

  def writeFilterMessage(channelType: ChannelType, channelAction: ChannelAction, snapshotId: Long, publisherId: Long, campaignId: Long, timestamp: Long, ip: String, writer: ParquetWriter[GenericRecord]): FilterMessage = {
    val message = TestHelper.newFilterMessage(channelType, channelAction, snapshotId, publisherId, campaignId, timestamp, ip)
    writer.write(message)
    message
  }

  import job.spark.implicits._

  test("test capping rules") {
    val metadata = Metadata(inputDir, channel)

    val dateFiles0 = new DateFiles("2018-01-01", Array("file://" + inputDir + "/date=2018-01-01/part-00000.snappy.parquet"))
    var meta: MetaFiles = new MetaFiles(Array(dateFiles0))

    fs.mkdirs(new Path("file://" + inputDir + "/date=2017-12-31/"))
    // test dedupe output meta
    metadata.writeDedupeOutputMeta(meta)

    val timestamp1 = getTimestamp("2018-01-01")
    val timestamp2 = getTimestamp("2018-01-02")
    val timestampBefore24h = timestamp1 - 1

    val writer1_0 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-01/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 1L, 11L, 111L, timestampBefore24h, "1.1.1.3", writer1_0)
    writer1_0.close()
    job.run()
    val df0 = job.readFilesAsDFEx(Array(outputDir + "/date=2018-01-01/"))
    df0.show()
    assert (df0.count() == 1)
    assert(df0.filter($"filter_failed"==="IPCapping").count() == 0)


    val dateFiles1 = new DateFiles("2018-01-01", Array("file://" + inputDir + "/date=2018-01-01/part-00001.snappy.parquet", "file://" + inputDir + "/date=2018-01-01/part-00002.snappy.parquet"))
    val dateFiles2 = new DateFiles("2018-01-02", Array("file://" + inputDir + "/date=2018-01-02/part-00001.snappy.parquet", "file://" + inputDir + "/date=2018-01-02/part-00002.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles1, dateFiles2))
    fs.delete(new Path(inputDir+"/meta/"), true)
    metadata.writeDedupeOutputMeta(meta)

    val writer1_1 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-01/part-00001.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val writer1_2 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-01/part-00002.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val writer2_1 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-02/part-00001.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val writer2_2 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-02/part-00002.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 2L, 11L, 111L, timestamp1, "1.1.1.1", writer1_1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 3L, 11L, 111L, timestamp1, "1.1.1.2", writer1_1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 4L, 11L, 111L, timestamp1, "1.1.1.2", writer1_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 5L, 11L, 111L, timestamp2, "1.1.1.2", writer1_2)

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 6L, 11L, 111L, timestamp2, "1.1.1.1", writer2_1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 7L, 11L, 111L, timestamp2, "1.1.1.2", writer2_1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 8L, 11L, 111L, timestamp2, "1.1.1.2", writer2_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 9L, 11L, 111L, timestamp2, "1.1.1.2", writer2_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 10L, 11L, 111L, timestamp2, "1.1.1.3", writer2_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 11L, 11L, 111L, timestamp2, "1.1.1.3", writer2_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 12L, 11L, 111L, timestamp2, "1.1.1.3", writer2_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 13L, 11L, 111L, timestamp2, "1.1.1.3", writer2_2)

    writer1_1.close()
    writer1_2.close()
    writer2_1.close()
    writer2_2.close()

    job.run()

    val df1 = job.readFilesAsDFEx(Array(outputDir + "/date=2018-01-01/"))
    df1.show()
    assert (df1.count() == 5)
    assert(df1.filter($"filter_failed"==="IPCapping").count() == 0)

    val df2 = job.readFilesAsDFEx(Array(outputDir + "/date=2018-01-02/"))
    df2.show()
    assert (df2.count() == 8)
    assert(df2.filter($"filter_failed"==="IPCapping").count() == 3)
  }
}
