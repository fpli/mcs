package com.ebay.traffic.chocolate.sparknrt

import java.text.SimpleDateFormat

import com.ebay.app.raptor.chocolate.avro.versions.{FilterMessageV1, FilterMessageV2}
import com.ebay.app.raptor.chocolate.avro.{ChannelAction, ChannelType, FilterMessage}
import com.ebay.traffic.chocolate.common.TestHelper
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

/**
 * Created by jialili1 on 1/7/19.
 */
class TestSchemaSparkCompatible extends BaseFunSuite {

  val tmpPath = createTempPath()

  @transient lazy val hadoopConf = {
    new Configuration()
  }

  lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  def getTimestamp(date: String): Long = {
    sdf.parse(date).getTime
  }

  def writeFilterMessage(channelType: ChannelType, channelAction: ChannelAction, snapshotId: Long, publisherId: Long, campaignId: Long, cguid: String, timestamp: Long, writer: ParquetWriter[GenericRecord]): FilterMessage = {
    val message = TestHelper.newFilterMessage(channelType, channelAction, snapshotId, publisherId, campaignId, cguid, timestamp)
    writer.write(message)
    message
  }

  def writeFilterMessageV1(channelType: ChannelType, channelAction: ChannelAction, snapshotId: Long, publisherId: Long, campaignId: Long, cguid: String, timestamp: Long, writer: ParquetWriter[GenericRecord]): FilterMessageV1 = {
    val message = TestHelper.newFilterMessageV1(channelType, channelAction, snapshotId, publisherId, campaignId, cguid, timestamp)
    writer.write(message)
    message
  }

  val job = new BaseSparkNrtJob("TestSparkJob", "local[8]") {

    override def run() = {

      val timestamp1 = getTimestamp("2018-01-01")
      val timestamp2 = timestamp1 + 10800000

      val cguid0 = "5552b695werlkl4763d4a844f4bfff00"
      val cguid1 = "3dc2b6951630aa4763d4a844f4b212f8"

      val writer1_0 = AvroParquetWriter.
        builder[GenericRecord](new Path(tmpPath + "/part-00000.snappy.parquet"))
        .withSchema(FilterMessageV2.getClassSchema())
        .withConf(hadoopConf)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()

      writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 1L, 11L, 111L, cguid0, timestamp1, writer1_0)
      writer1_0.close()

      val writer1_0_v1 = AvroParquetWriter.
        builder[GenericRecord](new Path(tmpPath + "/part-00000_v1.snappy.parquet"))
        .withSchema(FilterMessageV1.getClassSchema())
        .withConf(hadoopConf)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build()

      writeFilterMessageV1(ChannelType.EPN, ChannelAction.CLICK, 100L, 11L, 111L, cguid1, timestamp2, writer1_0_v1)
      writer1_0_v1.close()

      val df = readFilesAsDFEx(Array(tmpPath + "/part-00000.snappy.parquet", tmpPath + "/part-00000_v1.snappy.parquet"))
      df.show()

      assert (df.count() == 2)

    }
  }

  test("test schema spark compatible") {

    job.run()

    job.stop()

  }
}
