package com.ebay.traffic.chocolate.sparknrt.capping

import java.text.SimpleDateFormat

import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV1
import com.ebay.app.raptor.chocolate.avro.{ChannelAction, ChannelType, FilterMessage}
import com.ebay.traffic.chocolate.common.TestHelper
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.capping.rules.SNIDCappingRule
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
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
  val sparkJob = new CappingRuleJob(params)
  val sdf = new SimpleDateFormat("yyyy-MM-dd")

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
    val job_0 = new SNIDCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L),
      CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S), dateFiles_0, sparkJob, "long")
    val df_0 = job_0.test()
    df_0.show()
    assert(df_0.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L)).=!=(0)).count() == 0)
    job_0.postTest()

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
    val job_1 = new SNIDCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L),
      CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S), dateFiles_1, sparkJob, "long")
    val df_1 = job_1.test()
    df_1.show()
    assert(df_1.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L)).=!=(0)).count() == 1)
    job_1.postTest()

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
    val job_2 = new SNIDCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L),
      CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S), dateFiles_2, sparkJob, "long")
    val df_2 = job_2.test()
    df_2.show()
    assert(df_2.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L)).=!=(0)).count() == 1)
    job_2.postTest()

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
    val job_3 = new SNIDCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L),
      CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S), dateFiles_3, sparkJob, "long")
    val df_3 = job_3.test()
    df_3.show()
    assert(df_3.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L)).=!=(0)).count() == 2)
    job_3.postTest()
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
    val job_4 = new SNIDCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L),
      CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S), dateFiles_4, sparkJob, "long")
    val df_4 = job_4.test()
    df_4.show()
    assert(df_4.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S)).=!=(0)).count() == 0)
    job_4.postTest()

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
    val job_5 = new SNIDCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L),
      CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S), dateFiles_5, sparkJob, "long")
    val df_5 = job_5.test()
    df_5.show()
    assert(df_5.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S)).=!=(0)).count() == 1)
    job_5.postTest()

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
    val job_6 = new SNIDCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L),
      CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S), dateFiles_6, sparkJob, "long")
    val df_6 = job_6.test()
    df_6.show()
    assert(df_6.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S)).=!=(0)).count() == 1)
    job_6.postTest()

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
    val job_7 = new SNIDCappingRule(params, CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_L),
      CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S), dateFiles_7, sparkJob, "long")
    val df_7 = job_7.test()
    df_7.show()
    assert(df_7.filter($"capping".bitwiseAND(CappingRuleEnum.getBitValue(CappingRuleEnum.SnidCappingRule_S)).=!=(0)).count() == 3)
    job_7.postTest()

  }
}
