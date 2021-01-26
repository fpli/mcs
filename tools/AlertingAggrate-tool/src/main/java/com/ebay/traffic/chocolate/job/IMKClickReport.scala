package com.ebay.traffic.chocolate.job

import java.text.SimpleDateFormat
import java.util.Properties

import com.ebay.traffic.chocolate.job.util.DateUtil._
import com.ebay.traffic.chocolate.job.util.TableSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.SaveMode

/**
  * Created by shuangxu on 10/22/19.
  */
object IMKClickReport extends App{

  override def main(args: Array[String]): Unit = {
    val inputdir = args(0)
    println("Input path: " + inputdir)
    val outputdir = args(1)
    println("Output Path: " + outputdir)
    val jobtask = args(2)
    println("com.xl.traffic.chocolate.job task: " + jobtask)
    val schema_imk_click_dir = args(3)
    println("schema imk click dir: " + schema_imk_click_dir)
    val mode = args(4)
    println("mode: " + mode)
    val current_date = args(5)
    println("current date: " + current_date)
    val header = args(6)

    val job = new IMKClickReport(inputdir, outputdir, jobtask, schema_imk_click_dir, mode, current_date, header)
    job.run()
    job.stop()
  }
}

class IMKClickReport(inputdir: String,
                     outputdir: String,
                     jobtask: String,
                     schema_imk_click_dir: String,
                     mode: String = "local[4]",
                     current_date: String,
                     header: String) extends Serializable{

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  lazy val empty = ""
  lazy val click_cd = "1"
  lazy val roi_cd = "2"
  lazy val roi_chl_cd = "0"

  /**channel_id for each channel:
    * 0 for ROI
    * 3 for Natural search
    * 2 for Paid Search
    * 4 for Display
    * 16 for Social Media
    * 28 for Search Engine Free Listings
    */
  var imk_channel_list = List[String]()

  var channelMap =Map[String, String]()

  /**
    * Counter for the corrupt rows.
    */
  @transient lazy val corruptRows = new ThreadLocal[Int]() {
    override def initialValue: Int = 0
  }
  lazy val MAX_CORRUPT_ROWS = 100

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
    val builder = SparkSession.builder().appName("EpnClickCountMain")

    if (isTest) {
      logger.info("Test mode")
      builder.master("local")
        .appName("SparkUnitTesting")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir"))
    } else {
      logger.info("Prod mode")
    }
    builder.getOrCreate()
  }

  @transient lazy val properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("IMKClickReport.properties"))
    properties
  }

  lazy val extractHourUdf: UserDefinedFunction = udf(extractHour(_: String))
  lazy val extractDateUdf: UserDefinedFunction = udf(extracDate(_: String))
  lazy val getChannelNameUdf: UserDefinedFunction = udf(getChannelNamebyId(_: String))
  lazy val calculateDiffUdf: UserDefinedFunction = udf(calculateDiff(_:Int, _:Int))

  import spark.implicits._

  def run(): Unit = {
    if (jobtask.equalsIgnoreCase("hourlyClickCount")) {
      logger.info("hourlyClickCount start: " + jobtask)
      hourlyClickCount(header)
    } else {
      logger.info("no match function start")
    }
  }

  def hourlyClickCount(header: String) = {
    logger.info("hourlyClickCount function")
    imk_channel_list = properties.getProperty("imk_channel_list").split(",").toList
    properties.getProperty("channelMap").split(",").map(channel => {
      val tmp = channel.split(":");
      channelMap += (tmp(0) -> tmp(1))
    });
    if(imk_channel_list.length <= 0 || channelMap.size <= 0){
      throw new Exception("imk_channel_list or channelMap value is null,imk_channel_list:"+imk_channel_list+",channelMap:"+channelMap)
    }
    val imkClickToday = onceClickCount(inputdir + "chocolate_date=" + current_date + "*")
    val imkClickYesterday = onceClickCount(inputdir + "chocolate_date=" + getDateBeforeNDay(current_date, -1) + "*")
    val imkClickBeforeYesterday = onceClickCount(inputdir + "chocolate_date=" + getDateBeforeNDay(current_date, -2) + "*")

    imkClickToday.union(imkClickYesterday).union(imkClickBeforeYesterday)
      .select("event_dt", "click_hour", "click_cnt", "channel_id", "distinct_click_cnt", "diff")
      .groupBy(col("channel_id"), col("event_dt"), col("click_hour"))
      .agg(sum(col("click_cnt")).as("click_count"), sum(col("distinct_click_cnt")).as("distinct_click_count"), sum(col("diff")).as("differences"))
      .withColumn("channel_name", getChannelNameUdf(col("channel_id")))
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("channel_name")
      .option("header", header)
      .csv(outputdir)
  }

  def onceClickCount(dir: String): DataFrame ={
    logger.info("onceClickCount function:" + dir)
    val imkClick = readParquetFilesAsDF(dir,false)
      .filter(((col("rvr_cmnd_type_cd") === click_cd) and(col("rvr_chnl_type_cd") isin(imk_channel_list:_*)))
        or((col("rvr_cmnd_type_cd") === roi_cd) and(col("rvr_chnl_type_cd") === roi_chl_cd)))
      .withColumn("click_hour", extractHourUdf(col("event_ts"))).withColumn("event_dt",extractDateUdf(col("event_ts")))
    val imkClickCnt = imkClick.groupBy(col("rvr_chnl_type_cd").as("channel_id"), col("click_hour"),col("event_dt"))
      .agg(count(col("rvr_id")).as("click_cnt"), countDistinct(col("rvr_id")).as("distinct_click_cnt"))
      .withColumn("diff", calculateDiffUdf(col("click_cnt"), col("distinct_click_cnt")))
    imkClickCnt
  }

  def readParquetFilesAsDF(inputPaths: String,
                      broadcastHint: Boolean = false): DataFrame = {
    val df = spark.read.parquet(inputPaths)
    if (broadcastHint) broadcast(df) else df
  }

  def extractHour(ts: String): String = {
    try {
      if (ts == null) {
        return "999"
      }
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      return simpleDateFormat.parse(ts).getHours.toString
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.info("error ts: " + ts)
        logger.info("exception===>:" + ex.getMessage)
        return "999"
      }
    }
  }

  def extracDate(ts: String): String = {
    try {
      if (ts == null) {
        return "999"
      }
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      return new SimpleDateFormat("yyyy-MM-dd").format(simpleDateFormat.parse(ts))
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.info("error ts: " + ts)
        logger.info("exception===>:" + ex.getMessage)
        return "999"
      }
    }
  }

  def calculateDiff(count: Int, distinctCount: Int) = {
    if(count == null || distinctCount == null || count <= 0 || distinctCount <= 0)
      -1
    else{
      val res = count - distinctCount;
      res
    }
  }

  def getChannelNamebyId(channel_id: String) = {
    if(channel_id == null || channel_id.length == 0 || !channelMap.contains(channel_id))
      empty
    else
      channelMap(channel_id)
  }

  /**
    * stop the spark com.xl.traffic.chocolate.job.
    */
  def stop() = {
    spark.stop()
  }

  /** *
    * For test
    *
    * @param dir
    * @return
    */
  def getData(dir: String): DataFrame = {
    spark.read.option("header", "true").csv(dir)
  }
}