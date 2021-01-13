package com.ebay.traffic.chocolate.sparknrt.calImkV2Watermark

import java.io.File

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.calImkV2Watermark.{CalImkV2Watermark, Parameter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class TestCalImkV2Watermark extends BaseFunSuite {
  private val tmpPath = createTempDir()
  private val crabTransformDataDir = tmpPath + "/crabTransform/imkOutput"
  private val imkCrabTransformDataDir = tmpPath + "/imkTransformMerged/imkOutput"
  private val dedupAndSinkKafkaLagDir = tmpPath + "/last_ts"
  private val outPutDir = tmpPath + "/outPutDir"
  private val channels = "PAID_SEARCH,ROI,crabDedupe"

  private val localDir = getTestResourcePath("crabTransformWatermark.data")

  private val imkDir = "imk"
  private val lastTsDir = "last_ts"

  private val imkFile1 = "chocolate_date=2019-12-23_local-1607328473870_00000.parquet"
  private val imkFile2 = "chocolate_date=2019-12-24_local-1607328473870_00000.parquet"

  @transient private lazy val hadoopConf = {
    new Configuration()
  }

  private lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  override def beforeAll(): Unit = {
    createTestData()
  }

  test("Test calImkV2mWatermark") {
    val args = Array(
      "--mode", "local[8]",
      "--imkCrabTransformDataDir", imkCrabTransformDataDir,
      "--dedupAndSinkKafkaLagDir", dedupAndSinkKafkaLagDir,
      "--channels", channels,
      "--outputDir", outPutDir,
      "--elasticsearchUrl", "http://10.148.181.34:9200"
    )
    val params = Parameter(args)
    val job = new CalImkV2Watermark(params)
    job.run()

    assert(job.readFileContent(new Path(outPutDir + "/imkCrabTransformWatermark"), fs).equals("1577084400010"))
    assert(job.readFileContent(new Path(outPutDir + "/dedupAndSinkWatermark_PAID_SEARCH"), fs).equals("1569720941824"))
    assert(job.readFileContent(new Path(outPutDir + "/dedupAndSinkWatermark_ROI"), fs).equals("1569720941824"))
    assert(job.readFileContent(new Path(outPutDir + "/dedupAndSinkWatermark_crabDedupe"), fs).equals("1569720943443"))
  }

  def createTestData(): Unit = {
    fs.mkdirs(new Path(crabTransformDataDir))

    fs.copyFromLocalFile(new Path(new File(localDir + "/" + imkDir + "/" + imkFile1).getAbsolutePath), new Path(imkCrabTransformDataDir + "/" + imkFile1))
    fs.copyFromLocalFile(new Path(new File(localDir + "/" + imkDir + "/" + imkFile2).getAbsolutePath), new Path(imkCrabTransformDataDir + "/" + imkFile2))

    fs.copyFromLocalFile(new Path(new File(localDir + "/" + lastTsDir + "/PAID_SEARCH/0").getAbsolutePath), new Path(dedupAndSinkKafkaLagDir + "/PAID_SEARCH/0"))
    fs.copyFromLocalFile(new Path(new File(localDir + "/" + lastTsDir + "/PAID_SEARCH/1").getAbsolutePath), new Path(dedupAndSinkKafkaLagDir + "/PAID_SEARCH/1"))

    fs.copyFromLocalFile(new Path(new File(localDir + "/" + lastTsDir + "/PAID_SEARCH/0").getAbsolutePath), new Path(dedupAndSinkKafkaLagDir + "/ROI/0"))

    fs.copyFromLocalFile(new Path(new File(localDir + "/" + lastTsDir + "/PAID_SEARCH/1").getAbsolutePath), new Path(dedupAndSinkKafkaLagDir + "/crabDedupe/crab_min_ts"))
  }

}
