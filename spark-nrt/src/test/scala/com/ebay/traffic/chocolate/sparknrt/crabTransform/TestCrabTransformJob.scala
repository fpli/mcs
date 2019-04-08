package com.ebay.traffic.chocolate.sparknrt.crabTransform

import java.io.File

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class TestCrabTransformJob extends BaseFunSuite{

  private val tmpPath = createTempDir()
  private val workDir = tmpPath + "/workDir/"
  private val dataDir = tmpPath + "/dataDir"
  private val outPutDir = tmpPath + "/outPutDir/"
  private val kwDataDir = tmpPath + "/kwData"
  private val hiveData = tmpPath + "/hiveData/default.db"

  @transient private lazy val hadoopConf = {
    new Configuration()
  }

//  @transient lazy val spark = {
//    val builder = SparkSession.builder().appName("UnitTest")
//    builder.master("local[8]")
//      .appName("SparkUnitTesting")
//      .config("spark.sql.shuffle.partitions", "1")
//      .config("spark.driver.bindAddress", "127.0.0.1")
//      .config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir"))
//      .enableHiveSupport()
//    builder.getOrCreate()
//  }

  private lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  override def beforeAll(): Unit = {
    createTestData()
    createTestHiveTable()
  }

  test("Test crabTransformJob") {
    val args = Array(
      "--mode", "local[8]",
      "--channel", "crabDedupe",
      "--transformedPrefix", "chocolate_",
      "--workDir", workDir,
      "--outputDir", outPutDir,
      "--kwDataDir", kwDataDir,
      "--compressOutPut", "false",
      "--maxMetaFiles", "2",
      "--elasticsearchUrl", "http://10.148.181.34:9200",
      "--metaFile", "dedupe",
      "--hdfsUri", "",
      "--xidParallelNum", "2"
    )
    val params = Parameter(args)
    val job = new CrabTransformJob(params)
//    job.run()

  }


  def createTestData(): Unit = {
    val metadata = Metadata(workDir, "crabDedupe", MetadataEnum.dedupe)
    val dateFiles = DateFiles("date=2018-05-01", Array(dataDir + "/date=2018-05-01/imk_rvr_trckng_testData.csv"))
    val meta: MetaFiles = MetaFiles(Array(dateFiles))

    metadata.writeDedupeOutputMeta(meta)
    fs.copyFromLocalFile(new Path(new File("src/test/resources/crabTransform.data/imk_rvr_trckng_testData.csv").getAbsolutePath), new Path(dataDir + "/date=2018-05-01/imk_rvr_trckng_testData.csv"))
    fs.copyFromLocalFile(new Path(new File("src/test/resources/crabTransform.data/kwData.csv").getAbsolutePath), new Path(kwDataDir + "/kwData.csv"))
  }

  def createTestHiveTable(): Unit = {
//    spark.sql("create database if not exists default location '" + hiveData +"'")
//    spark.sql("create table if not exists dw_kwdm_kw_lkp(kw_id bigint, kw string)")
  }


}
