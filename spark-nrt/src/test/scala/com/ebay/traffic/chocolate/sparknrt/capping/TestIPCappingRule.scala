package com.ebay.traffic.chocolate.sparknrt.capping

import java.text.SimpleDateFormat

import com.ebay.app.raptor.chocolate.avro.{ChannelAction, ChannelType, FilterMessage}
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV1
import com.ebay.traffic.chocolate.common.TestHelper
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.capping.rules.IPCappingRule
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
  * Created by xiangli4 on 3/30/18.
  */
class TestIPCappingRule extends BaseFunSuite {
  val tmpPath = createTempPath()
  val inputDir = tmpPath + "/workDir/dedupe"
  val workDir = tmpPath + "/workDir/capping"
  val outputDir = tmpPath + "/outputDir/"
  val ipThreshold = "5"
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
  val sparkJob = new CappingRuleJob(params)
  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  def getTimestamp(date: String): Long = {
    sdf.parse(date).getTime
  }

  def writeFilterMessage(channelType: ChannelType, channelAction: ChannelAction, snapshotId: Long, publisherId: Long, campaignId: Long, timestamp: Long, ip: String, writer: ParquetWriter[GenericRecord]): FilterMessage = {
    val message = TestHelper.newFilterMessage(channelType, channelAction, snapshotId, publisherId, campaignId, timestamp, ip)
    writer.write(message)
    message
  }

  import sparkJob.spark.implicits._

  test("test ip capping rule") {
    val metadata = Metadata(workDir, channel, MetadataEnum.dedupe)

    val dateFiles0 = new DateFiles("date=2018-01-01", Array("file://" + inputDir + "/date=2018-01-01/part-00000.snappy.parquet"))
    var meta: MetaFiles = new MetaFiles(Array(dateFiles0))

    fs.mkdirs(new Path("file://" + inputDir + "/date=2017-12-31/"))
    metadata.writeDedupeOutputMeta(meta)

    val timestamp1 = getTimestamp("2018-01-01")
    val timestamp2 = getTimestamp("2018-01-02")
    val timestamp3 = getTimestamp("2018-01-03")
    val timestampBefore24h = timestamp1 - 1

    val writer1_0 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-01/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 1L, 11L, 111L, timestampBefore24h, "1.1.1.3", writer1_0)
    writer1_0.close()
    val dateFiles_0 = new DateFiles("date=2018-01-01", Array(inputDir + "/date=2018-01-01/part-00000.snappy.parquet"))

    val job_0 = new IPCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule), dateFiles_0, sparkJob)
    // handle 1st meta containing 1 meta 1 date 1 file
    val df_0 = job_0.test()
    df_0.show()
    assert(df_0.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule)).=!=(0)).count() == 0)
    job_0.postTest()

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

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 14L, 11L, 111L, timestamp2, "1.1.1.2", writer3)
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 15L, 11L, 111L, timestamp2, "1.1.1.2", writer3)

    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 16L, 11L, 111L, timestamp3, "1.1.1.2", writer4)

    writer1_1.close()
    writer1_2.close()
    writer2_1.close()
    writer2_2.close()
    writer3.close()
    writer4.close()

    val dateFiles_1 = new DateFiles("date=2018-01-01", Array(inputDir + "/date=2018-01-01/part-00001.snappy.parquet",
      inputDir + "/date=2018-01-01/part-00002.snappy.parquet"))
    val job_1 = new IPCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule), dateFiles_1, sparkJob)
    // handle 2nd meta containing 1 meta 2 date 4 file
    val df_1 = job_1.test()
    df_1.show()
    assert(df_1.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule)).=!=(0)).count() == 0)
    job_1.postTest()

    val dateFiles_2 = new DateFiles("date=2018-01-02", Array(inputDir + "/date=2018-01-02/part-00001.snappy.parquet",
      inputDir + "/date=2018-01-02/part-00002.snappy.parquet"))
    val job_2 = new IPCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule), dateFiles_2, sparkJob)
    // handle 3rd meta containing 1 meta 1 date 1 file
    val df_2 = job_2.test()
    df_2.show()
    assert(df_2.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule)).=!=(0)).count() == 3)
    job_2.postTest()

    // handle 4th meta containing 1 meta 1 date 1 file, no click
    val dateFiles_3 = new DateFiles("date=2018-01-03", Array(inputDir + "/date=2018-01-03/part-00001.snappy.parquet"))
    val job_3 = new IPCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule), dateFiles_3, sparkJob)
    val df_3 = job_3.test()
    df_3.show()
    assert(df_3.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.IPCappingRule)).=!=(0)).count() == 0)
    job_3.postTest()

  }
}
