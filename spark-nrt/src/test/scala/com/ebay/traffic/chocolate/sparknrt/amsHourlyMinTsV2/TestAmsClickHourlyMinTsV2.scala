package com.ebay.traffic.chocolate.sparknrt.amsHourlyMinTsV2

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}

class TestAmsClickHourlyMinTsV2 extends BaseFunSuite {
  private val tmpPath = createTempPath()
  private val inputDir = getTestResourcePath("amsClickSample_v2.snappy.parquet")
  private val workDir = tmpPath + "/workDir/"
  private val outputDir = tmpPath + "/outputDir/"
  private val channel = "EPN"
  private val usage = "epnnrt_scp_click"
  private val metaSuffix = ".epnnrt_hercules"

  val args = Array(
    "--mode", "local[8]",
    "--workDir", workDir,
    "--channel", channel,
    "--usage", usage,
    "--metaSuffix", metaSuffix,
    "--outputDir", outputDir
  )

  val params = ParameterV2(args)
  val job = new AmsHourlyMinTsJobV2(params)

  test("test AmsHourlyMinTs Job for click") {
    val metadata = Metadata(workDir, channel, MetadataEnum.epnnrt_scp_click)
    val dateFiles = DateFiles("date=2019-06-23", Array(inputDir))
    val meta = MetaFiles(Array(dateFiles))
    val outputMetaDir = workDir + "meta/EPN/output/epnnrt_scp_click/"
    metadata.writeOutputMeta(meta, outputMetaDir, usage, Array(metaSuffix))

    job.run()
    job.stop()
  }

}
