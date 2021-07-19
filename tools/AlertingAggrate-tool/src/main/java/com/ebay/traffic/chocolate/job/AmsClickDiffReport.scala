package com.ebay.traffic.chocolate.job

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame,SparkSession}
import org.slf4j.LoggerFactory

object AmsClickDiffReport extends App {

  override def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      println("Wrong arguments")
    }
    val newClickInputPath:String=args(0)
    val oldClickInputPath:String=args(1)
    val outputPath:String = args(2)
    val mode: String = args(3)

    val job = new AmsClickDiffReport(newClickInputPath, oldClickInputPath,outputPath, mode)
    job.run()
    job.stop()
  }

}
class AmsClickDiffReport(val newClickInputPath: String, val oldClickInputPath: String,val outputPath: String, val mode: String = "local[4]") extends Serializable {
  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Whether the spark com.xl.traffic.chocolate.job is in local mode
    */
  @transient lazy val isTest: Boolean = {
    mode.indexOf("test") == 0
  }

  /**
    * The spark session, which is the entrance point of DataFrame, DataSet and Spark SQL.
    */
  @transient lazy val spark = {
    val builder: SparkSession.Builder = SparkSession.builder().appName("AmsClickDiffReportMain")

    if (isTest) {
      logger.info("Test mode")
      builder.master("local")
        .appName("SparkUnitTesting")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir"))
      // for test, hive support is not enabled. Use in-memory catalog implementation
    } else {
      logger.info("Prod mode")
    }
    builder.getOrCreate()
  }

  /**
    * The spark context
    */
  @transient lazy val sc = {
    spark.sparkContext
  }

  /**
    * The java spark context
    */
  @transient lazy val jsc = {
    JavaSparkContext.fromSparkContext(sc);
  }

  /**
    * The sql context
    */
  @transient lazy val sqlsc = {
    spark.sqlContext;
  }

  /**
    * The hadoop conf
    */
  @transient lazy val hadoopConf = {
    new Configuration()
  }

  /**
    * The file system
    */
  @transient lazy val fs = {
    val fs: FileSystem = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }


  import spark.implicits._

  def run(): Unit = {
    generateClickDiffReport
  }

  def generateClickDiffReport = {
    val newClickCount:Long=spark.read
      .parquet(newClickInputPath)
      .count()
    logger.info("new click count is {}",newClickCount)
    val oldClickCount:Long=spark.read
      .parquet(oldClickInputPath)
      .count()
    logger.info("old click count is {}",oldClickCount)

  }

  /**
    * stop the spark com.xl.traffic.chocolate.job.
    */
  def stop() = {
    spark.stop()
  }
}
