package com.ebay.traffic.chocolate.job

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * Created by lxiong1
  */
object MergeSmallFilesMain extends App {

  override def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Wrong arguments")
    }
    val mode = args(0)
    println("Mode: " + mode)
    val inputdir = args(1)
    println("Mode: " + inputdir)
    val outdir = args(2)
    println("Input Path: " + outdir)

    val job = new MergeSmallFilesMain(mode, inputdir, outdir)
    job.run()
    job.stop()
  }

}


class MergeSmallFilesMain(val mode: String = "local[4]",
                          val input: String,
                          val output: String
                         ) extends Serializable {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Whether the spark job is in local mode
    */
  lazy val isTest: Boolean = {
    mode.indexOf("test") == 0
  }

  /**
    * The spark session, which is the entrance point of DataFrame, DataSet and Spark SQL.
    */
  @transient lazy val spark = {
    val builder = SparkSession.builder().appName("EpnClickCountMain")

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
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  def run(): Unit = {
    val epnClick = spark.read
      .option("delimiter", "\007")
      .csv(input)

    epnClick.repartition(1)
      .write
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .option("escape", null)
      .option("quote", "")
      .option("quoteMode", "NONE")
      .option("nullValue", "")
      .csv(output)
  }

  /**
    * stop the spark job.
    */
  def stop() = {
    spark.stop()
  }

}