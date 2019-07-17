package com.ebay.traffic.chocolate.sparknrt.amsHourlyMinTs

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}

class TestAmsImpHourlyMinTs extends BaseFunSuite {
  private val tmpPath = createTempPath()
  private val inputDir = getTestResourcePath("amsImpSample.dat.gz")
  private val workDir = tmpPath + "/workDir/"
  private val outputDir = tmpPath + "/outputDir/"
  private val channel = "EPN"
  private val usage = "epnnrt_scp_imp"
  private val metaSuffix = ".epnnrt_hercules"

  val args = Array(
    "--mode", "local[8]",
    "--workDir", workDir,
    "--channel", channel,
    "--usage", usage,
    "--metaSuffix", metaSuffix,
    "--outputDir", outputDir
  )

  val params = Parameter(args)
  val job = new AmsHourlyMinTsJob(params)

  test("test AmsHourlyMinTs job for impression") {
    val metadata = Metadata(workDir, channel, MetadataEnum.epnnrt_scp_imp)
    val dateFiles = DateFiles("date=2019-07-16", Array(inputDir))
    val meta = new MetaFiles(Array(dateFiles))
    val outputMetaDir = workDir + "meta/EPN/output/epnnrt_scp_imp/"
    metadata.writeOutputMeta(meta, outputMetaDir, usage, Array(metaSuffix))

    job.run()
    job.stop()
  }


}
