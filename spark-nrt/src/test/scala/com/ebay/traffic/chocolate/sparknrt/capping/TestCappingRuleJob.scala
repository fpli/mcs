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

  val channel = "EPN"

  val args = Array(
    "--mode", "local[8]",
    "--channel", channel,
    "--inputDir", inputDir,
    "--workDir", workDir,
    "--outputDir", outputDir
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

  def writeFilterMessage(channelType: ChannelType, channelAction: ChannelAction, snapshotId: Long, publisherId: Long, campaignId: Long, date: String, ip: String, writer: ParquetWriter[GenericRecord]): FilterMessage = {
    val message = TestHelper.newFilterMessage(channelType, channelAction, snapshotId, publisherId, campaignId, getTimestamp(date), ip)
    writer.write(message)
    message
  }

  test("test capping rules") {
    val metadata = Metadata(inputDir, channel)

    val dateFiles1 = new DateFiles("2018-01-01", Array("file://" + inputDir + "/date=2018-01-01/part-00000.snappy.parquet", "file://" + inputDir + "/date=2018-01-01/part-00001.snappy.parquet"))
    val dateFiles2 = new DateFiles("2018-01-02", Array("file://" + inputDir + "/date=2018-01-02/part-00000.snappy.parquet", "file://" + inputDir + "/date=2018-01-02/part-00001.snappy.parquet"))
    val meta: MetaFiles = new MetaFiles(Array(dateFiles1, dateFiles2))

    fs.mkdirs(new Path("file://" + inputDir + "/date=2017-12-31/"))
    // test dedupe output meta
    metadata.writeDedupeOutputMeta(meta)

    val writer1_1 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-01/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val writer1_2 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-01/part-00001.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val writer2_1 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-02/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val writer2_2 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-02/part-00001.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 1L, 11L, 111L, "2018-01-01", "1.1.1.1", writer1_1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 2L, 11L, 111L, "2018-01-01", "1.1.1.2", writer1_1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 3L, 11L, 111L, "2018-01-01", "1.1.1.2", writer1_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 4L, 11L, 111L, "2018-01-02", "1.1.1.2", writer1_2)

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 5L, 11L, 111L, "2018-01-02", "1.1.1.1", writer2_1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 6L, 11L, 111L, "2018-01-02", "1.1.1.2", writer2_1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 7L, 11L, 111L, "2018-01-02", "1.1.1.2", writer2_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 8L, 11L, 111L, "2018-01-02", "1.1.1.2", writer2_2)

    writer1_1.close()
    writer1_2.close()
    writer2_1.close()
    writer2_2.close()

    job.run()
  }
}
