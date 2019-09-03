package com.ebay.traffic.chocolate.sparknrt.imkCrabTransformOutputMerge

import java.io.File

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.utils.TableSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame

class TestImkCrabTransformOutputMergeJob extends BaseFunSuite {

  private val tmpPath = createTempDir()

  private val inputDir = tmpPath + "/inputDir"
  private val outputDir = tmpPath + "/outputDir"
  private val backupDir = tmpPath + "/backupDir"

  private val imkDir = "imkOutput"
  private val dtlDir = "dtlOutput"
  private val mgDir = "mgOutput"

  private val localDir = "src/test/resources/imkCrabTransformOutputMerge.data"

  private val imkRawFile1 = "chocolate_date=2019-08-31_application_1561139602691_263337_00000"
  private val imkRawFile2 = "chocolate_date=2019-09-01_application_1561139602691_263356_00000"
  private val imkRawFile3 = "chocolate_date=2019-09-01_application_1561139602691_263356_00001"

  private val dtlRawFile1 = "chocolate_date=2019-08-31_application_1561139602691_263410_00000"
  private val dtlRawFile2 = "chocolate_date=2019-09-01_application_1561139602691_263429_00000"
  private val dtlRawFile3 = "chocolate_date=2019-09-01_application_1561139602691_263429_00001"

  private val mgRawFile1 = "chocolate_date=2019-08-31_application_1561139602691_266751_00001"
  private val mgRawFile2 = "chocolate_date=2019-09-01_application_1561139602691_266775_00000"
  private val mgRawFile3 = "chocolate_date=2019-09-01_application_1561139602691_266789_00000"

  private val schema_apollo = TableSchema("df_imk_apollo.json")
  private val schema_apollo_dtl = TableSchema("df_imk_apollo_dtl.json")
  private val schema_apollo_mg = TableSchema("df_imk_apollo_mg.json")

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

  test("Test imkCrabTransformOutputMergeJob") {
    val args = Array(
      "--mode", "local[8]",
      "--transformedPrefix", "chocolate_",
      "--inputDir", inputDir,
      "--outputDir", outputDir,
      "--backupDir", backupDir,
      "--compressOutPut", "false",
      "--elasticsearchUrl", "http://10.148.181.34:9200"
    )
    val params = Parameter(args)
    val job = new ImkCrabTransformOutputMergeJob(params)

    val rawImkDf = job.readFilesAsDFEx(Array(
      inputDir + "/" + imkDir + "/" + imkRawFile1,
      inputDir + "/" + imkDir + "/" + imkRawFile2,
      inputDir + "/" + imkDir + "/" + imkRawFile3),
      schema_apollo.dfSchema, "sequence", "delete").cache()
    rawImkDf.count()

    val rawDtlDf = job.readFilesAsDFEx(Array(
      inputDir + "/" + dtlDir + "/" + dtlRawFile1,
      inputDir + "/" + dtlDir + "/" + dtlRawFile2,
      inputDir + "/" + dtlDir + "/" + dtlRawFile3),
      schema_apollo_dtl.dfSchema, "sequence", "delete").cache()
    rawDtlDf.count()

    val rawMgDf = job.readFilesAsDFEx(Array(
      inputDir + "/" + mgDir + "/" + mgRawFile1,
      inputDir + "/" + mgDir + "/" + mgRawFile2,
      inputDir + "/" + mgDir + "/" + mgRawFile3),
      schema_apollo_mg.dfSchema, "sequence", "delete").cache()
    rawMgDf.count()

    job.run()

    assert(fs.exists(new Path(backupDir + "/" + imkDir + "/" + imkRawFile1)))
    assert(fs.exists(new Path(backupDir + "/" + imkDir + "/" + imkRawFile2)))
    assert(fs.exists(new Path(backupDir + "/" + imkDir + "/" + imkRawFile3)))

    assert(fs.exists(new Path(backupDir + "/" + dtlDir + "/" + dtlRawFile1)))
    assert(fs.exists(new Path(backupDir + "/" + dtlDir + "/" + dtlRawFile2)))
    assert(fs.exists(new Path(backupDir + "/" + dtlDir + "/" + dtlRawFile3)))

    assert(fs.exists(new Path(backupDir + "/" + mgDir + "/" + mgRawFile1)))
    assert(fs.exists(new Path(backupDir + "/" + mgDir + "/" + mgRawFile2)))
    assert(fs.exists(new Path(backupDir + "/" + mgDir + "/" + mgRawFile3)))

    val backupImkDf = job.readFilesAsDFEx(Array(
      backupDir + "/" + imkDir + "/" + imkRawFile1,
      backupDir + "/" + imkDir + "/" + imkRawFile2,
      backupDir + "/" + imkDir + "/" + imkRawFile3),
      schema_apollo.dfSchema, "sequence", "delete")

    val backupDtlDf: DataFrame = job.readFilesAsDFEx(Array(
      backupDir + "/" + dtlDir + "/" + dtlRawFile1,
      backupDir + "/" + dtlDir + "/" + dtlRawFile2,
      backupDir + "/" + dtlDir + "/" + dtlRawFile3),
      schema_apollo_dtl.dfSchema, "sequence", "delete")

    val backupMgDf = job.readFilesAsDFEx(Array(
      backupDir + "/" + mgDir + "/" + mgRawFile1,
      backupDir + "/" + mgDir + "/" + mgRawFile2,
      backupDir + "/" + mgDir + "/" + mgRawFile3),
      schema_apollo_mg.dfSchema, "sequence", "delete")

    val mergedImkDf = job.readFilesAsDFEx(fs.listStatus(new Path(outputDir + "/" + imkDir)).map(s => s.getPath.toString),
      schema_apollo.dfSchema, "sequence", "delete")

    val mergedDtlDf = job.readFilesAsDFEx(fs.listStatus(new Path(outputDir + "/" + dtlDir)).map(s => s.getPath.toString),
      schema_apollo_dtl.dfSchema, "sequence", "delete")

    val mergedMgDf = job.readFilesAsDFEx(fs.listStatus(new Path(outputDir + "/" + mgDir)).map(s => s.getPath.toString),
      schema_apollo_mg.dfSchema, "sequence", "delete")

    // compare raw files and merged files
    assert(rawImkDf.except(mergedImkDf).count == 0)
    assert(mergedImkDf.except(rawImkDf).count == 0)

    assert(rawDtlDf.except(mergedDtlDf).count == 0)
    assert(mergedDtlDf.except(rawDtlDf).count == 0)

    assert(rawMgDf.except(mergedMgDf).count == 0)
    assert(mergedMgDf.except(rawMgDf).count == 0)

    // compare merged files and backup files
    assert(mergedImkDf.except(backupImkDf).count == 0)
    assert(backupImkDf.except(mergedImkDf).count == 0)

    assert(mergedDtlDf.except(backupDtlDf).count == 0)
    assert(backupDtlDf.except(mergedDtlDf).count == 0)

    assert(mergedMgDf.except(backupMgDf).count == 0)
    assert(backupMgDf.except(mergedMgDf).count == 0)

    // compare raw files and backup files
    assert(rawImkDf.except(backupImkDf).count == 0)
    assert(backupImkDf.except(rawImkDf).count == 0)

    assert(rawDtlDf.except(backupDtlDf).count == 0)
    assert(backupDtlDf.except(rawDtlDf).count == 0)

    assert(rawMgDf.except(backupMgDf).count == 0)
    assert(backupMgDf.except(rawMgDf).count == 0)
  }

  def createTestData(): Unit = {
    fs.copyFromLocalFile(new Path(new File(localDir + "/" + imkDir + "/" + imkRawFile1).getAbsolutePath), new Path(inputDir + "/" + imkDir + "/" + imkRawFile1))
    fs.copyFromLocalFile(new Path(new File(localDir + "/" + imkDir + "/" + imkRawFile2).getAbsolutePath), new Path(inputDir + "/" + imkDir + "/" + imkRawFile2))
    fs.copyFromLocalFile(new Path(new File(localDir + "/" + imkDir + "/" + imkRawFile3).getAbsolutePath), new Path(inputDir + "/" + imkDir + "/" + imkRawFile3))

    fs.copyFromLocalFile(new Path(new File(localDir + "/" + dtlDir + "/" + dtlRawFile1).getAbsolutePath), new Path(inputDir + "/" + dtlDir + "/" + dtlRawFile1))
    fs.copyFromLocalFile(new Path(new File(localDir + "/" + dtlDir + "/" + dtlRawFile2).getAbsolutePath), new Path(inputDir + "/" + dtlDir + "/" + dtlRawFile2))
    fs.copyFromLocalFile(new Path(new File(localDir + "/" + dtlDir + "/" + dtlRawFile3).getAbsolutePath), new Path(inputDir + "/" + dtlDir + "/" + dtlRawFile3))

    fs.copyFromLocalFile(new Path(new File(localDir + "/" + mgDir + "/" + mgRawFile1).getAbsolutePath), new Path(inputDir + "/" + mgDir + "/" + mgRawFile1))
    fs.copyFromLocalFile(new Path(new File(localDir + "/" + mgDir + "/" + mgRawFile2).getAbsolutePath), new Path(inputDir + "/" + mgDir + "/" + mgRawFile2))
    fs.copyFromLocalFile(new Path(new File(localDir + "/" + mgDir + "/" + mgRawFile3).getAbsolutePath), new Path(inputDir + "/" + mgDir + "/" + mgRawFile3))
  }

}
