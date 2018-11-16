package com.ebay.traffic.chocolate.sparknrt.monitoring

import java.text.SimpleDateFormat

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

/**
  * Created by jialili1 on 11/15/18
 */
class TestMonitoringJob extends BaseFunSuite {
  private val tmpPath = createTempPath()
  private val inputDir = tmpPath + "/inputDir/"
  private val workDir = tmpPath + "/workDir/"
  private val channel = "EPN"
  private val elasticsearchUrl = "http://10.148.181.34:9200"

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  val args = Array(
    "--mode", "local[8]",
    "--channel", channel,
    "--workDir", workDir,
    "--elasticsearchUrl", elasticsearchUrl
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
  val job = new MonitoringJob(params)
  val timestamp1 = getTimestamp("2018-11-01")
  val timestamp2 = timestamp1 + 3600000

  def getTimestamp(date: String): Long = {
    sdf.parse(date).getTime
  }

  def writeFilterMessage(channelType: ChannelType, channelAction: ChannelAction, snapshotId: Long, timestamp: Long, rtRuleFlags: Long, nrtRuleFlags: Long, writer: ParquetWriter[GenericRecord]): FilterMessage = {
    val message = TestHelper.newFilterMessage(snapshotId, timestamp, channelType, channelAction, rtRuleFlags, nrtRuleFlags)
    writer.write(message)
    message
  }

  test("test monitoring") {
    val metadata = Metadata(workDir, "EPN", MetadataEnum.capping)
    val dateFiles0 = DateFiles("date=2018-11-01", Array("file://" + inputDir + "/date=2018-11-01/part-00000.snappy.parquet"))
    var meta = new MetaFiles(Array(dateFiles0))
    metadata.writeDedupeOutputMeta(meta, Array(".monitoring"))

    val dateFiles1 = DateFiles("date=2018-11-01", Array("file://" + inputDir + "/date=2018-11-01/part-00001.snappy.parquet", "file://" + inputDir + "/date=2018-11-01/part-00002.snappy.parquet"))
    meta = new MetaFiles(Array(dateFiles1))
    metadata.writeDedupeOutputMeta(meta, Array(".monitoring"))

    // prepare data file
    // writer0 has no data
    val writer0 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-11-01/part-00000.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema)
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    writer0.close()

    val writer1 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-11-01/part-00001.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema)
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val writer2 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-11-01/part-00002.snappy.parquet"))
      .withSchema(FilterMessageV1.getClassSchema)
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 1L, timestamp1, 1, 2, writer1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 2L, timestamp1, 0, 4, writer1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 3L, timestamp1, 1, 8, writer1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 4L, timestamp2, 0, 16, writer1)

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 5L, timestamp2, 1, 32, writer2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 6L, timestamp2, 0, 64, writer2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 7L, timestamp2, 1, 128, writer2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 8L, timestamp2, 0, 256, writer2)

    writer1.close()
    writer2.close()

    job.run()
    job.stop()
  }
}
