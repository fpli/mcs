package com.ebay.traffic.chocolate.sparknrt.crabTransformWatermark

import java.io.File
import java.time.{ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * @author Zhiyuan Wang
 * @since 2019/9/29
 */
class TestCalCrabTransformWatermark extends BaseFunSuite {
  private val tmpPath = createTempDir()
  private val workDir = tmpPath + "/workDir"
  private val crabTransformDataDir = tmpPath + "/crabTransform/imkOutput"
  private val imkCrabTransformDataDir = tmpPath + "/imkTransformMerged/imkOutput"
  private val dedupAndSinkKafkaLagDir = tmpPath + "/last_ts"
  private val outPutDir = tmpPath + "/outPutDir"
  private val channels = "PAID_SEARCH,ROI,crabDedupe"

  private val localDir = getTestResourcePath("crabTransformWatermark.data")

  private val crabDir = "crab"
  private val imkDir = "imk"
  private val lastTsDir = "last_ts"

  private val crabFile = "chocolate_date=2019-08-31_application_1561139602691_263337_00000"
  private val imkFile1 = "chocolate_date=2019-09-01_application_1561139602691_263356_00000"
  private val imkFile2 = "chocolate_date=2019-09-01_application_1561139602691_263356_00001"

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

  test("Test calCrabTransformWatermark") {
    val args = Array(
      "--mode", "local[8]",
      "--crabTransformDataDir", crabTransformDataDir,
      "--imkCrabTransformDataDir", imkCrabTransformDataDir,
      "--dedupAndSinkKafkaLagDir", dedupAndSinkKafkaLagDir,
      "--channels", channels,
      "--outputDir", outPutDir,
      "--elasticsearchUrl", "http://10.148.181.34:9200"
    )
    val params = Parameter(args)
    val job = new CalCrabTransformWatermark(params)
    job.run()

    assert(!fs.exists(new Path(outPutDir + "/crabTransformWatermark")))

//    assert(job.readFileContent(new Path(outPutDir + "/crabTransformWatermark"), fs).equals("1567350689871"))
    assert(job.readFileContent(new Path(outPutDir + "/imkCrabTransformWatermark"), fs)
      .equals(ZonedDateTime.parse("2019-09-01 23:16:42.591",
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
          .withZone(ZoneId.systemDefault())).toInstant.toEpochMilli.toString))
    assert(job.readFileContent(new Path(outPutDir + "/dedupAndSinkWatermark_PAID_SEARCH"), fs).equals("1569720941824"))
    assert(job.readFileContent(new Path(outPutDir + "/dedupAndSinkWatermark_ROI"), fs).equals("1569720941824"))
    assert(job.readFileContent(new Path(outPutDir + "/dedupAndSinkWatermark_crabDedupe"), fs).equals("1569720943443"))
  }

  def createTestData(): Unit = {
//    fs.copyFromLocalFile(new Path(new File(localDir + "/" + crabDir + "/" + crabFile).getAbsolutePath), new Path(crabTransformDataDir + "/" + crabFile))
    fs.mkdirs(new Path(crabTransformDataDir))

    fs.copyFromLocalFile(new Path(new File(localDir + "/" + imkDir + "/" + imkFile1).getAbsolutePath), new Path(imkCrabTransformDataDir + "/" + imkFile1))
    fs.copyFromLocalFile(new Path(new File(localDir + "/" + imkDir + "/" + imkFile2).getAbsolutePath), new Path(imkCrabTransformDataDir + "/" + imkFile2))

    fs.copyFromLocalFile(new Path(new File(localDir + "/" + lastTsDir + "/PAID_SEARCH/0").getAbsolutePath), new Path(dedupAndSinkKafkaLagDir + "/PAID_SEARCH/0"))
    fs.copyFromLocalFile(new Path(new File(localDir + "/" + lastTsDir + "/PAID_SEARCH/1").getAbsolutePath), new Path(dedupAndSinkKafkaLagDir + "/PAID_SEARCH/1"))

    fs.copyFromLocalFile(new Path(new File(localDir + "/" + lastTsDir + "/PAID_SEARCH/0").getAbsolutePath), new Path(dedupAndSinkKafkaLagDir + "/ROI/0"))

    fs.copyFromLocalFile(new Path(new File(localDir + "/" + lastTsDir + "/PAID_SEARCH/1").getAbsolutePath), new Path(dedupAndSinkKafkaLagDir + "/crabDedupe/crab_min_ts"))
  }

}
