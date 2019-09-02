package com.ebay.traffic.chocolate.sparknrt.imkCrabTransformRotation

import java.io.File

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class TestImkCrabTransformRotationJob extends BaseFunSuite{

  private val tmpPath = createTempDir()
  private val inputDir = tmpPath + "/inputDir"
  private val outPutDir = tmpPath + "/outPutDir"

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

  test("Test imkCrabTransformRotationJob") {
    val args = Array(
      "--mode", "local[8]",
      "--transformedPrefix", "chocolate_",
      "--inputDir", inputDir,
      "--outputDir", outPutDir,
      "--elasticsearchUrl", "http://10.148.181.34:9200"
    )
    val params = Parameter(args)
    val job = new ImkCrabTransformRotationJob(params)

    job.run()

    fs.listStatus(new Path(outPutDir + "/imkOutput")).map(s => s.getPath.toString).foreach(path => {
      println(path)
    })

    fs.listStatus(new Path(outPutDir + "/dtlOutput")).map(s => s.getPath.toString).foreach(path => {
      println(path)
    })

    fs.listStatus(new Path(outPutDir + "/mgOutput")).map(s => s.getPath.toString).foreach(path => {
      println(path)
    })

  }

  def createTestData(): Unit = {
    fs.copyFromLocalFile(new Path(new File("src/test/resources/imkCrabTransformRotation.data/imkOutput/chocolate_date=2019-08-31_application_1561139602691_263337_00000").getAbsolutePath), new Path(inputDir + "/imkOutput/chocolate_date=2019-08-31_application_1561139602691_263337_00000"))
    fs.copyFromLocalFile(new Path(new File("src/test/resources/imkCrabTransformRotation.data/imkOutput/chocolate_date=2019-09-01_application_1561139602691_263356_00000").getAbsolutePath), new Path(inputDir + "/imkOutput/chocolate_date=2019-09-01_application_1561139602691_263356_00000"))
    fs.copyFromLocalFile(new Path(new File("src/test/resources/imkCrabTransformRotation.data/imkOutput/chocolate_date=2019-09-01_application_1561139602691_263356_00001").getAbsolutePath), new Path(inputDir + "/imkOutput/chocolate_date=2019-09-01_application_1561139602691_263356_00001"))
    fs.copyFromLocalFile(new Path(new File("src/test/resources/imkCrabTransformRotation.data/dtlOutput/chocolate_date=2019-08-31_application_1561139602691_263410_00000").getAbsolutePath), new Path(inputDir + "/dtlOutput/chocolate_date=2019-08-31_application_1561139602691_263410_00000"))
    fs.copyFromLocalFile(new Path(new File("src/test/resources/imkCrabTransformRotation.data/dtlOutput/chocolate_date=2019-09-01_application_1561139602691_263429_00000").getAbsolutePath), new Path(inputDir + "/dtlOutput/chocolate_date=2019-09-01_application_1561139602691_263429_00000"))
    fs.copyFromLocalFile(new Path(new File("src/test/resources/imkCrabTransformRotation.data/dtlOutput/chocolate_date=2019-09-01_application_1561139602691_263429_00001").getAbsolutePath), new Path(inputDir + "/dtlOutput/chocolate_date=2019-09-01_application_1561139602691_263429_00001"))
    fs.copyFromLocalFile(new Path(new File("src/test/resources/imkCrabTransformRotation.data/mgOutput/chocolate_date=2019-08-31_application_1561139602691_263432_00000").getAbsolutePath), new Path(inputDir + "/mgOutput/chocolate_date=2019-08-31_application_1561139602691_263432_00000"))
    fs.copyFromLocalFile(new Path(new File("src/test/resources/imkCrabTransformRotation.data/mgOutput/chocolate_date=2019-09-01_application_1561139602691_263405_00001").getAbsolutePath), new Path(inputDir + "/mgOutput/chocolate_date=2019-09-01_application_1561139602691_263405_00001"))
    fs.copyFromLocalFile(new Path(new File("src/test/resources/imkCrabTransformRotation.data/mgOutput/chocolate_date=2019-09-01_application_1561139602691_263405_00002").getAbsolutePath), new Path(inputDir + "/mgOutput/chocolate_date=2019-09-01_application_1561139602691_263405_00002"))
  }

}
