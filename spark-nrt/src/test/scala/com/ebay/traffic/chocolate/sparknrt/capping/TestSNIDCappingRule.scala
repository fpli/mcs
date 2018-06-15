package com.ebay.traffic.chocolate.sparknrt.capping

import java.text.SimpleDateFormat

import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV1
import com.ebay.app.raptor.chocolate.avro.{ChannelAction, ChannelType, FilterMessage}
import com.ebay.traffic.chocolate.common.TestHelper
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.capping.rules.SNIDCappingRule
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
  * Created by xiangli4 on 5/30/18.
  */
class TestSNIDCappingRule extends BaseFunSuite {

  lazy val windowLong = "long"
  lazy val windowShort = "short"

  val tmpPath = createTempPath()
  val inputDir = tmpPath + "/workDir/dedupe"
  val workDir = tmpPath + "/workDir/"
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
  val job = new CappingRuleJob(params)
  val sparkJob = new CappingRuleJob(params)
  val sdf = new SimpleDateFormat("yyyy-MM-dd")

  val metadata = Metadata(workDir, channel, MetadataEnum.dedupe)

  def getTimestamp(date: String): Long = {
    sdf.parse(date).getTime
  }

  def writeFilterMessage(channelType: ChannelType, channelAction: ChannelAction, snapshotId: Long, publisherId: Long, timestamp: Long, snid: String, writer: ParquetWriter[GenericRecord]): FilterMessage = {
    val message = TestHelper.newFilterMessage(channelType, channelAction, snapshotId, publisherId, timestamp, snid)
    writer.write(message)
    message
  }

  import sparkJob.spark.implicits._

  test("test snid capping rule long") {

    // test only impression
    val writer0 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-01/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 1L, 1L, getTimestamp("2018-01-01") + 1, "snid0", writer0)
    writer0.close()

    val dateFiles_0 = new DateFiles("date=2018-01-01", Array(inputDir + "/date=2018-01-01/part-00000.snappy.parquet"))
    var meta: MetaFiles = new MetaFiles(Array(dateFiles_0))
    fs.mkdirs(new Path("file://" + inputDir + "/date=2017-12-31/"))
    metadata.writeDedupeOutputMeta(meta)
    job.run()
    val df_0 = job.readFilesAsDFEx(Array(outputDir + "/" + channel + "/capping" + "/date=2018-01-01/"))
    df_0.show()
    assert(df_0.filter($"nrt_rule_flags".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L)).=!=(0)).count() == 0)

    // test only click
    val writer1 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-02/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 2L, 1L, getTimestamp("2018-01-02"), "snid1", writer1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 3L, 1L, getTimestamp("2018-01-02"), "snid0", writer1)
    writer1.close()
    val dateFiles_1 = new DateFiles("date=2018-01-02", Array(inputDir + "/date=2018-01-02/part-00000.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles_1))
    metadata.writeDedupeOutputMeta(meta)
    job.run()
    val df_1 = job.readFilesAsDFEx(Array(outputDir + "/" + channel + "/capping" + "/date=2018-01-02/"))
    df_1.show()
    assert(df_1.filter($"nrt_rule_flags".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L)).=!=(0)).count() == 1)

    // test impression and click in today
    val writer2 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-03/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 4L, 1L, getTimestamp("2018-01-03"), "snid2", writer2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 5L, 1L, getTimestamp("2018-01-03"), "snid2", writer2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 6L, 1L, getTimestamp("2018-01-03"), "snid3", writer2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 7L, 1L, getTimestamp("2018-01-03") + 1, "snid4", writer2)
    writer2.close()
    val dateFiles_2 = new DateFiles("date=2018-01-03", Array(inputDir + "/date=2018-01-03/part-00000.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles_2))
    metadata.writeDedupeOutputMeta(meta)
    job.run()
    val df_2 = job.readFilesAsDFEx(Array(outputDir + "/" + channel + "/capping" + "/date=2018-01-03/"))
    df_2.show()
    assert(df_2.filter($"nrt_rule_flags".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L)).=!=(0)).count() == 1)

    // test impression and click in cross day
    val writer3 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-04/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 8L, 1L, getTimestamp("2018-01-04"), "snid2", writer3)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 9L, 1L, getTimestamp("2018-01-04"), "snid4", writer3)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 10L, 1L, getTimestamp("2018-01-04"), "snid4", writer3)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 11L, 1L, getTimestamp("2018-01-04"), "snid5", writer3)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 12L, 1L, getTimestamp("2018-01-04"), "snid6", writer3)
    writer3.close()
    val dateFiles_3 = new DateFiles("date=2018-01-04", Array(inputDir + "/date=2018-01-04/part-00000.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles_3))
    metadata.writeDedupeOutputMeta(meta)
    job.run()
    val df_3 = job.readFilesAsDFEx(Array(outputDir + "/" + channel + "/capping" + "/date=2018-01-04/"))
    df_3.show()
    assert(df_3.filter($"nrt_rule_flags".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L)).=!=(0)).count() == 2)
  }

  test("test snid capping rule short") {

    // test only impression
    val writer4 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-05/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 13L, 1L, getTimestamp("2018-01-05") + 86400000 - 2000, "snid7", writer4)
    writer4.close()
    val dateFiles_4 = new DateFiles("date=2018-01-05", Array(inputDir + "/date=2018-01-05/part-00000.snappy.parquet"))
    var meta: MetaFiles = new MetaFiles(Array(dateFiles_4))
    metadata.writeDedupeOutputMeta(meta)
    job.run()
    val df_4 = job.readFilesAsDFEx(Array(outputDir + "/" + channel + "/capping" + "/date=2018-01-05/"))
    df_4.show()
    assert(df_4.filter($"nrt_rule_flags".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S)).=!=(0)).count() == 0)

    // test only click
    val writer5 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-06/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()
    // click inside 3 seconds
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 14L, 1L, getTimestamp("2018-01-06"), "snid7", writer5)
    // click after 3 seconds
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 15L, 1L, getTimestamp("2018-01-06") + 2000, "snid7", writer5)
    writer5.close()
    val dateFiles_5 = new DateFiles("date=2018-01-06", Array(inputDir + "/date=2018-01-06/part-00000.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles_5))
    metadata.writeDedupeOutputMeta(meta)
    job.run()
    val df_5 = job.readFilesAsDFEx(Array(outputDir + "/" + channel + "/capping" + "/date=2018-01-06/"))
    df_5.show()
    assert(df_5.filter($"nrt_rule_flags".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S)).=!=(0)).count() == 1)

    // test impression and click in today
    val writer6 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-07/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 16L, 1L, getTimestamp("2018-01-07"), "snid8", writer6)
    // click in 3 seconds
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 17L, 1L, getTimestamp("2018-01-07") + 2999, "snid8", writer6)
    // click after 3 seconds
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 18L, 1L, getTimestamp("2018-01-07") + 3000, "snid8", writer6)
    // different snid
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 19L, 1L, getTimestamp("2018-01-07") + 86400000 - 2000, "snid9", writer6)
    writer6.close()
    val dateFiles_6 = new DateFiles("date=2018-01-07", Array(inputDir + "/date=2018-01-07/part-00000.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles_6))
    metadata.writeDedupeOutputMeta(meta)
    job.run()
    val df_6 = job.readFilesAsDFEx(Array(outputDir + "/" + channel + "/capping" + "/date=2018-01-07/"))
    df_6.show()
    assert(df_6.filter($"nrt_rule_flags".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S)).=!=(0)).count() == 1)

    // test impression and click in cross day
    val writer7 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-01-08/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema())
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    // click in 3 seconds cross day
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 20L, 1L, getTimestamp("2018-01-08"), "snid9", writer7)
    // click in 3 seconds cross day
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 21L, 1L, getTimestamp("2018-01-08"), "snid9", writer7)
    // click after 3 seconds cross day
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 22L, 1L, getTimestamp("2018-01-08") + 1000, "snid9", writer7)
    // today impression
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 23L, 1L, getTimestamp("2018-01-08"), "snid10", writer7)
    // click in 3 seconds
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 24L, 1L, getTimestamp("2018-01-08"), "snid10", writer7)
    writer7.close()
    val dateFiles_7 = new DateFiles("date=2018-01-08", Array(inputDir + "/date=2018-01-08/part-00000.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles_7))
    metadata.writeDedupeOutputMeta(meta)
    job.run()
    val df_7 = job.readFilesAsDFEx(Array(outputDir + "/" + channel + "/capping" + "/date=2018-01-08/"))
    df_7.show()
    assert(df_7.filter($"nrt_rule_flags".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S)).=!=(0)).count() == 3)
  }
}
