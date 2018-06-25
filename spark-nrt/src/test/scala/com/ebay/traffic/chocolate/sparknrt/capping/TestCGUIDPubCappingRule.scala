package com.ebay.traffic.chocolate.sparknrt.capping

import java.text.SimpleDateFormat

import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV1
import com.ebay.app.raptor.chocolate.avro.{ChannelAction, ChannelType, FilterMessage}
import com.ebay.traffic.chocolate.common.TestHelper
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.capping.rules.CGUIDPubCappingRule
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.junit.Ignore

/**
  * Created by jialili1 on 5/29/18.
  */
@Ignore
class TestCGUIDPubCappingRule extends BaseFunSuite {
  lazy val windowLong = "long"
  lazy val windowShort = "short"

  val tmpPath = createTempPath()
  val inputDir = tmpPath + "/workDir/dedupe"
  val workDir = tmpPath + "/workDir/capping"
  val outputDir = tmpPath + "/outputDir/"
  val channel = "EPN"

  val args = Array(
    "--mode", "local[8]",
    "--channel", channel,
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
  val sparkJob = new CappingRuleJob(params)
  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def getTimestamp(date: String): Long = {
    sdf.parse(date).getTime
  }

  def writeFilterMessage(channelType: ChannelType, channelAction: ChannelAction, snapshotId: Long, publisherId: Long, campaignId: Long, cguid: String, timestamp: Long, writer: ParquetWriter[GenericRecord]): FilterMessage = {
    val message = TestHelper.newFilterMessage(channelType, channelAction, snapshotId, publisherId, campaignId, cguid, timestamp)
    writer.write(message)
    message
  }

  import sparkJob.spark.implicits._

  ignore("test cguid-pub capping rule") {
    val metadata = Metadata(workDir, channel, MetadataEnum.dedupe)

    val dateFiles0 = new DateFiles("date=2018-01-01", Array("file://" + inputDir + "/date=2018-01-01/part-00000.snappy.parquet"))
    var meta: MetaFiles = new MetaFiles(Array(dateFiles0))

    fs.mkdirs(new Path("file://" + inputDir + "/date=2017-12-31/"))
    metadata.writeDedupeOutputMeta(meta)

    val timestamp1 = getTimestamp("2018-01-01")
    val timestamp2 = timestamp1 + 10800000
    val timestamp3 = getTimestamp("2018-01-02")
    val timestamp4 = getTimestamp("2018-01-03")
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

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 1L, 11L, 111L, cguid3, timestampBefore24h, writer1_0)
    writer1_0.close()
    val dateFiles_0 = new DateFiles("date=2018-01-01", Array(inputDir + "/date=2018-01-01/part-00000.snappy.parquet"))

    val job_01 = new CGUIDPubCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_S), dateFiles_0, sparkJob, windowShort)
    val job_02 = new CGUIDPubCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_L), dateFiles_0, sparkJob, windowLong)
    // handle 1st meta containing 1 meta 1 date 1 file
    val df_01 = job_01.test()
    val df_02 = job_02.test()
    df_01.show()
    df_02.show()
    assert(df_01.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_S)).=!=(0)).count() == 0)
    assert(df_02.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_L)).=!=(0)).count() == 0)
    job_01.postTest()
    job_02.postTest()

    val dateFiles1 = new DateFiles("date=2018-01-01", Array("file://" + inputDir + "/date=2018-01-01/part-00001.snappy.parquet", "file://" + inputDir + "/date=2018-01-01/part-00002.snappy.parquet"))
    val dateFiles2 = new DateFiles("date=2018-01-02", Array("file://" + inputDir + "/date=2018-01-02/part-00001.snappy.parquet", "file://" + inputDir + "/date=2018-01-02/part-00002.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles1, dateFiles2))

    val dateFiles3 = new DateFiles("date=2018-01-02", Array("file://" + inputDir + "/date=2018-01-02/part-00003.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles3))
    metadata.writeDedupeOutputMeta(meta)

    // no click in this meta
    val dateFiles4 = new DateFiles("date=2018-01-03", Array("file://" + inputDir + "/date=2018-01-03/part-00001.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles4))

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

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 2L, 11L, 111L, cguid1, timestamp1, writer1_1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 3L, 11L, 111L, cguid2, timestamp1, writer1_1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 4L, 11L, 111L, cguid2, timestamp1, writer1_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 5L, 11L, 111L, cguid2, timestamp2, writer1_2)

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 6L, 11L, 111L, cguid1, timestamp2, writer2_1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 7L, 11L, 111L, cguid2, timestamp2, writer2_1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 8L, 11L, 111L, cguid2, timestamp2, writer2_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 9L, 11L, 111L, cguid3, timestamp2, writer2_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 10L, 11L, 111L, cguid3, timestamp2, writer2_2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 11L, 11L, 111L, cguid3, timestamp2, writer2_2)

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 12L, 11L, 111L, cguid3, timestamp3, writer3)
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 13L, 11L, 111L, cguid3, timestamp3, writer3)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 14L, 22L, 111L, cguid3, timestamp3, writer3)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 15L, -1L, 111L, cguid3, timestamp3, writer3)

    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 16L, 11L, 111L, cguid2, timestamp4, writer4)

    writer1_1.close()
    writer1_2.close()
    writer2_1.close()
    writer2_2.close()
    writer3.close()
    writer4.close()

    // handle 2nd meta containing 1 meta 2 date 4 file
    val dateFiles_1 = new DateFiles("date=2018-01-01", Array(inputDir + "/date=2018-01-01/part-00001.snappy.parquet",
      inputDir + "/date=2018-01-01/part-00002.snappy.parquet"))
    val job_11 = new CGUIDPubCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_S), dateFiles_1, sparkJob, windowShort)
    val job_12 = new CGUIDPubCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_L), dateFiles_1, sparkJob, windowLong)
    val df_11 = job_11.test()
    val df_12 = job_12.test()
    df_11.show()
    df_12.show()
    assert(df_11.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_S)).=!=(0)).count() == 0)
    assert(df_12.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_L)).=!=(0)).count() == 0)
    job_11.postTest()
    job_12.postTest()

    // handle 3rd meta containing 1 meta 2 date 1 file
    val dateFiles_2 = new DateFiles("date=2018-01-02", Array(inputDir + "/date=2018-01-02/part-00001.snappy.parquet",
      inputDir + "/date=2018-01-02/part-00002.snappy.parquet"))
    val job_21 = new CGUIDPubCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_S), dateFiles_2, sparkJob, windowShort)
    val job_22 = new CGUIDPubCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_L), dateFiles_2, sparkJob, windowLong)
    val df_21 = job_21.test()
    val df_22 = job_22.test()
    df_21.show()
    df_22.show()
    assert(df_21.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_S)).=!=(0)).count() == 2)
    assert(df_22.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_L)).=!=(0)).count() == 2)
    job_21.postTest()
    job_22.postTest()

    // handle 3rd meta containing 1 meta 1 date 1 file
    val dateFiles_3 = new DateFiles("date=2018-01-02", Array(inputDir + "/date=2018-01-02/part-00003.snappy.parquet"))
    val job_31 = new CGUIDPubCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_S), dateFiles_3, sparkJob, windowShort)
    val job_32 = new CGUIDPubCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_L), dateFiles_3, sparkJob, windowLong)
    val df_31 = job_31.test()
    val df_32 = job_32.test()
    df_31.show()
    df_32.show()
    assert(df_31.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_S)).=!=(0)).count() == 1)
    assert(df_32.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_L)).=!=(0)).count() == 0)
    job_31.postTest()
    job_32.postTest()

    // handle 4th meta containing 1 meta 1 date 1 file, no events
    val dateFiles_4 = new DateFiles("date=2018-01-03", Array(inputDir + "/date=2018-01-03/part-00001.snappy.parquet"))
    val job_41 = new CGUIDPubCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_S), dateFiles_4, sparkJob, windowShort)
    val job_42 = new CGUIDPubCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_L), dateFiles_4, sparkJob, windowLong)
    val df_41 = job_41.test()
    val df_42 = job_42.test()
    df_41.show()
    df_42.show()
    assert(df_41.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_S)).=!=(0)).count() == 0)
    assert(df_42.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.CGUIDPubCappingRule_L)).=!=(0)).count() == 0)
    job_41.postTest()
    job_42.postTest()
  }
}
