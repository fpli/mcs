package com.ebay.traffic.chocolate.sparknrt.crabTransform

import java.io.File

import com.ebay.traffic.chocolate.spark.{BaseFunSuite, BaseSparkJob}
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class TestCrabTransformJob extends BaseFunSuite{

  private val tmpPath = createTempDir()
  private val workDir = tmpPath + "/workDir/"
  private val dataDir = tmpPath + "/dataDir"
  private val outPutDir = tmpPath + "/outPutDir/"
  private val kwDataDir = tmpPath + "/kwData/"

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

  test("Test crabTransformJob") {
    val args = Array(
      "--mode", "local[8]",
      "--channel", "crabDedupe",
      "--transformedPrefix", "chocolate_",
      "--workDir", workDir,
      "--outputDir", outPutDir,
      "--joinKeyword", "true",
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
    // prepare keyword lookup data
    val df = job.spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(tmpPath + "/kwData.csv")
    df.write.parquet(kwDataDir)
    job.run()
    val status1 = fs.listStatus(new Path(outPutDir + "/imkOutput"))
    assert(status1.nonEmpty)
    val status2 = fs.listStatus(new Path(outPutDir + "/dtlOutput"))
    assert(status2.nonEmpty)
    val status3 = fs.listStatus(new Path(outPutDir + "/mgOutput"))
    assert(status3.nonEmpty)

    println(job.getUserIdByCguid("", "1eb2d8b915c0a9e807109ca3f924b4c2", "1"))
  }

  def createTestData(): Unit = {
    val metadata = Metadata(workDir, "crabDedupe", MetadataEnum.dedupe)
    val dateFiles = DateFiles("date=2018-05-01", Array(dataDir + "/date=2018-05-01/imk_rvr_trckng_testData.csv"))
    val meta: MetaFiles = MetaFiles(Array(dateFiles))

    metadata.writeDedupeOutputMeta(meta)
    fs.copyFromLocalFile(new Path(new File("src/test/resources/crabTransform.data/imk_rvr_trckng_testData.csv").getAbsolutePath), new Path(dataDir + "/date=2018-05-01/imk_rvr_trckng_testData.csv"))
    fs.copyFromLocalFile(new Path(new File("src/test/resources/crabTransform.data/kwData.csv").getAbsolutePath), new Path(tmpPath + "/kwData.csv"))
  }

  test("Test filterIsNotDup") {
    val job = new BaseSparkJob("test", "local[8]") {
      override def run(): Unit = {
      }
    }

    val data = Seq(
      Row(1L, "", true, 1, 1, true),
      Row(2L, "", true, 1, 1, false),
      Row(3L, "", true, 1, 1, true)
    )

    val schema = List(
      StructField("KW_ID", LongType, nullable = true),
      StructField("KW", StringType, nullable = true),
      StructField("SPL_TERM_IND", BooleanType, nullable = true),
      StructField("WORD_COUNT", IntegerType, nullable = true),
      StructField("INIT_SRC_ID", IntegerType, nullable = true),
      StructField("IS_DUP", BooleanType, nullable = true)
    )

    val df = job.spark.createDataFrame(
      job.spark.sparkContext.parallelize(data),
      StructType(schema)
    )

    import job.spark.implicits._

    val notDupDF = df.filter($"is_dup" === false)

    assert(notDupDF.count() == 1)
    assert(df.except(notDupDF).count == 2)
  }

  test("Test not join keyword") {
    val args = Array(
      "--mode", "local[8]",
      "--channel", "crabDedupe",
      "--transformedPrefix", "chocolate_",
      "--workDir", workDir,
      "--outputDir", outPutDir,
      "--joinKeyword", "false",
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
    // prepare keyword lookup data
    val df = job.spark.read.format("csv").option("header", "true").option("delimiter", "\t").load(tmpPath + "/kwData.csv")
    df.write.parquet(kwDataDir)
    job.run()
    val status1 = fs.listStatus(new Path(outPutDir + "/imkOutput"))
    assert(status1.nonEmpty)
    val status2 = fs.listStatus(new Path(outPutDir + "/dtlOutput"))
    assert(status2.nonEmpty)
    val status3 = fs.listStatus(new Path(outPutDir + "/mgOutput"))
    assert(status3.nonEmpty)

    println(job.getUserIdByCguid("", "1eb2d8b915c0a9e807109ca3f924b4c2", "1"))
  }

}
