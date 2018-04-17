package com.ebay.traffic.chocolate.sparknrt.capping

import com.ebay.app.raptor.chocolate.avro.{ChannelAction, ChannelType, FilterMessage}
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV1
import com.ebay.traffic.chocolate.common.TestHelper
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
  * Created by xiangli4 on 4/8/18.
  */
class TestCappingRuleJob extends BaseFunSuite {
  val tmpPath = createTempPath()
  val inputDir = tmpPath + "/inputDir/"
  val workDir = tmpPath + "/workDir/"
  val outputDir = tmpPath + "/capping/"
  val ipThreshold = "6"
  val channel = "EPN"

  val args = Array(
    "--mode", "local[8]",
    "--channel", channel,
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
    val metadata = Metadata(workDir, channel, MetadataEnum.dedupe)

    val dateFiles0 = new DateFiles("date=2018-01-01", Array("file://" + inputDir + "/date=2018-01-01/part-00000.snappy.parquet"))
    var meta: MetaFiles = new MetaFiles(Array(dateFiles0))

    fs.mkdirs(new Path("file://" + inputDir + "/date=2017-12-31/"))
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
    // handle 1st meta containing 1 meta 1 date 1 file
    job.run()
    val df0 = job.readFilesAsDFEx(Array(outputDir + "/" + channel + "/date=2018-01-01/"))
    df0.show()
    assert (df0.count() == 1)
    assert(df0.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule)).=!=(0)).count() == 0)

    val dateFiles1 = new DateFiles("date=2018-01-01", Array("file://" + inputDir + "/date=2018-01-01/part-00001.snappy.parquet", "file://" + inputDir + "/date=2018-01-01/part-00002.snappy.parquet"))
    val dateFiles2 = new DateFiles("date=2018-01-02", Array("file://" + inputDir + "/date=2018-01-02/part-00001.snappy.parquet", "file://" + inputDir + "/date=2018-01-02/part-00002.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles1, dateFiles2))
    metadata.writeDedupeOutputMeta(meta)

    val dateFiles3 = new DateFiles("date=2018-01-02", Array("file://" + inputDir + "/date=2018-01-02/part-00003.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles3))
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

    val writer3 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-02/part-00003.snappy.parquet"))
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
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 9L, 11L, 111L, timestamp2, "1.1.1.3", writer2_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 10L, 11L, 111L, timestamp2, "1.1.1.3", writer2_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 11L, 11L, 111L, timestamp2, "1.1.1.3", writer2_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 12L, 11L, 111L, timestamp2, "1.1.1.3", writer2_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 13L, 11L, 111L, timestamp2, "1.1.1.3", writer2_2)

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 14L, 11L, 111L, timestamp2, "1.1.1.2", writer3)

    writer1_1.close()
    writer1_2.close()
    writer2_1.close()
    writer2_2.close()
    writer3.close()

    // handle 2nd meta containing 1 meta 2 date 4 file
    job.run()

    // handle 3rd meta containing 1 meta 1 date 1 file
    job.run()

    val df1 = job.readFilesAsDFEx(Array(outputDir + "/" + channel + "/date=2018-01-01/"))
    df1.show()
    assert (df1.count() == 5)
    assert(df1.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule)).=!=(0)).count() == 0)

    val df2 = job.readFilesAsDFEx(Array(outputDir + "/" + channel + "/date=2018-01-02/"))
    df2.show()
    assert (df2.count() == 9)
    //only the last batch has 1 ip rule faild
    assert(df2.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule)).=!=(0)).count() == 1)
  }
}
