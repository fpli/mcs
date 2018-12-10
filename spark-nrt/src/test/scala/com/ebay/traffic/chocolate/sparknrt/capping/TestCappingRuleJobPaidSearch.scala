package com.ebay.traffic.chocolate.sparknrt.capping

import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV1
import com.ebay.app.raptor.chocolate.avro.{ChannelAction, ChannelType, FilterMessage}
import com.ebay.traffic.chocolate.common.TestHelper
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.junit.Ignore

/**
  * Created by xiangli4 on 4/8/18.
  */
@Ignore
class TestCappingRuleJobPaidSearch extends BaseFunSuite {
  val tmpPath = createTempPath()
  val inputDir = tmpPath + "/inputDir/"
  val workDir = tmpPath + "/workDir/"
  val outputDir = tmpPath + "/outputDir/"
  val archiveDir = tmpPath + "/archiveDir/"
  val ipThreshold = "6"
  val channel = "PAID_SEARCH"

  val args = Array(
    "--mode", "local[8]",
    "--channel", channel,
    "--workDir", workDir,
    "--outputDir", outputDir,
    "--archiveDir", archiveDir,
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

  def writeFilterMessage(channelType: ChannelType, channelAction: ChannelAction, snapshotId: Long, publisherId: Long, campaignId: Long, timestamp: Long, ip: String, cguid: String, writer: ParquetWriter[GenericRecord]): FilterMessage = {
    val message = TestHelper.newFilterMessage(channelType, channelAction, snapshotId, publisherId, campaignId, timestamp, ip, cguid)
    writer.write(message)
    message
  }

  import job.spark.implicits._

  test("test paid search capping rules") {
    val metadata = Metadata(workDir, channel, MetadataEnum.dedupe)

    val dateFiles0 = new DateFiles("date=2018-01-01", Array("file://" + inputDir + "/date=2018-01-01/part-00000.snappy.parquet"))
    var meta: MetaFiles = new MetaFiles(Array(dateFiles0))

    fs.mkdirs(new Path("file://" + inputDir + "/date=2017-12-31/"))
    metadata.writeDedupeOutputMeta(meta)

    val timestamp1 = getTimestamp("2018-01-01")
    val timestamp2 = getTimestamp("2018-01-02")
    val timestamp3 = getTimestamp("2018-01-03")
    val timestampBefore24h = timestamp1 - 1

    val cguid1 = "3dc2b6951630aa4763d4a844f4b212f8"
    val cguid2 = "d30ebafe1580a93d128516d5ffef202f"
    val cguid3 = "8782800f1630a6882fc1341630aa1381"

    val writer1_0 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-01/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.CLICK, 1L, 11L, 111L, timestampBefore24h, "1.1.1.3", cguid3, writer1_0)
    writer1_0.close()
    // handle 1st meta containing 1 meta 1 date 1 file
    job.run()
    val df0 = job.readFilesAsDFEx(Array(outputDir + "/" + channel + "/capping" + "/date=2018-01-01/"))
    df0.show()
    assert(df0.count() == 1)
    assert(df0.filter($"nrt_rule_flags".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule)).=!=(0)).count() == 0)

    val dateFiles1 = new DateFiles("date=2018-01-01", Array("file://" + inputDir + "/date=2018-01-01/part-00001.snappy.parquet", "file://" + inputDir + "/date=2018-01-01/part-00002.snappy.parquet"))
    val dateFiles2 = new DateFiles("date=2018-01-02", Array("file://" + inputDir + "/date=2018-01-02/part-00001.snappy.parquet", "file://" + inputDir + "/date=2018-01-02/part-00002.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles1, dateFiles2))
    metadata.writeDedupeOutputMeta(meta)

    val dateFiles3 = new DateFiles("date=2018-01-02", Array("file://" + inputDir + "/date=2018-01-02/part-00003.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles3))
    metadata.writeDedupeOutputMeta(meta)

    // no click in this meta
    val dateFiles4 = new DateFiles("date=2018-01-03", Array("file://" + inputDir + "/date=2018-01-03/part-00001.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles4))
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

    val writer4 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-03/part-00001.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.CLICK, 2L, 11L, 111L, timestamp1, "1.1.1.1", cguid1, writer1_1)
    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.CLICK, 3L, 11L, 111L, timestamp1, "1.1.1.2", cguid2, writer1_1)
    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.CLICK, 4L, 11L, 111L, timestamp1, "1.1.1.2", cguid2, writer1_2)
    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.CLICK, 5L, 11L, 111L, timestamp2, "1.1.1.2", cguid2, writer1_2)

    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.CLICK, 6L, 11L, 111L, timestamp2, "1.1.1.1", cguid1, writer2_1)
    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.CLICK, 7L, 11L, 111L, timestamp2, "1.1.1.2", cguid2,  writer2_1)
    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.CLICK, 8L, 11L, 111L, timestamp2, "1.1.1.2", cguid2, writer2_2)
    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.CLICK, 9L, 22L, 111L, timestamp2, "1.1.1.3", cguid3, writer2_2)
    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.CLICK, 10L, 11L, 111L, timestamp2, "1.1.1.3", cguid3, writer2_2)
    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.CLICK, 11L, 11L, 111L, timestamp2, "1.1.1.3", cguid3, writer2_2)
    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.CLICK, 12L, 11L, 111L, timestamp2, "1.1.1.3", cguid3, writer2_2)
    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.CLICK, 13L, 11L, 111L, timestamp2, "1.1.1.3", cguid3, writer2_2)

    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.CLICK, 14L, 11L, 111L, timestamp2, "1.1.1.2", cguid2, writer3)
    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.IMPRESSION, 15L, 11L, 111L, timestamp2, "1.1.1.2", cguid2, writer3)

    writeFilterMessage(ChannelType.PAID_SEARCH, ChannelAction.IMPRESSION, 16L, 11L, 111L, timestamp3, "1.1.1.2", cguid2, writer4)

    writer1_1.close()
    writer1_2.close()
    writer2_1.close()
    writer2_2.close()
    writer3.close()
    writer4.close()

    // handle 2nd meta containing 1 meta 2 date 4 file
    job.run()

    // handle 3rd meta containing 1 meta 1 date 1 file
    job.run()

    // handle 4th meta containing 1 meta 1 date 1 file, no events
    job.run()

    val df1 = job.readFilesAsDFEx(Array(outputDir + "/" + channel + "/capping" + "/date=2018-01-01/"))
    df1.show()
    assert(df1.count() == 5)
    assert(df1.filter($"nrt_rule_flags".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule)).=!=(0)).count() == 0)


    val df2 = job.readFilesAsDFEx(Array(outputDir + "/" + channel + "/capping" + "/date=2018-01-02/"))
    df2.show()
    assert(df2.count() == 10)
    assert(df2.filter($"nrt_rule_flags".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule)).=!=(0)).count() == 1)


    val df3 = job.readFilesAsDFEx(Array(outputDir + "/" + channel + "/capping" + "/date=2018-01-03/"))
    df3.show()
    assert(df3.count() == 1)
    assert(df3.filter($"nrt_rule_flags".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule)).=!=(0)).count() == 0)
  }


}
