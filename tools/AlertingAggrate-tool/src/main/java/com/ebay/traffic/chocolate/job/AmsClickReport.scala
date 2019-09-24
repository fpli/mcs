package com.ebay.traffic.chocolate.job

import java.text.SimpleDateFormat
import java.util.Date

import com.ebay.traffic.chocolate.job.util.DateUtil._
import com.ebay.traffic.chocolate.job.util.TableSchema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.slf4j.LoggerFactory

object AmsClickReport extends App {

  override def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Wrong arguments")
    }
    val inputdir = args(0)
    println("Input path: " + inputdir)
    val outputdir = args(1)
    println("Output Path: " + outputdir)
    val jobtask = args(2)
    println("com.xl.traffic.chocolate.job task: " + jobtask)
    val schema_epn_click_dir = args(3)
    println("schema epn click dir: " + schema_epn_click_dir)
    val mode = args(4)
    println("mode: " + mode)

    val job = new AmsClickReport(inputdir, outputdir, jobtask, schema_epn_click_dir, mode)
    job.run()
    job.stop()
  }

}

class AmsClickReport(val inputdir: String, val outputdir: String, val jobtask: String, val schema_epn_click_dir: String, val mode: String = "local[4]") extends Serializable {
  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  @transient val schema_epn_click_table = TableSchema(schema_epn_click_dir)

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

  lazy val extractHourUdf: UserDefinedFunction = udf(extractHour(_: String))

  lazy val extractDateUdf: UserDefinedFunction = udf(extractDate(_: String))

  import spark.implicits._

  def run(): Unit = {
    if (jobtask.equalsIgnoreCase("hourlyClickCount")) {
      logger.info("hourlyClickCount start: " + jobtask)
      hourlyClickCount
    } else if (jobtask.equalsIgnoreCase("dailyClickTrend")) {
      logger.info("dailyClickTrend start " + jobtask)
      dailyClickTrend
    } else if (jobtask.equalsIgnoreCase("dailyDomainTrend")) {
      logger.info("dailyDomainTrend start " + jobtask)
      dailyDomainTrend
    } else {
      logger.info("no match function start")
    }
  }

  def hourlyClickCount = {
    logger.info("hourlyClickCount function")
    val epnClickToday = onceClickCount(inputdir + "date=" + getToady(isTest) + "/dw_ams.ams_clicks_cs_*.dat.gz").withColumn("count_dt", lit(getToady(isTest)))
    val epnClickYesterday = onceClickCount(inputdir + "date=" + getYesterday(isTest) + "/dw_ams.ams_clicks_cs_*.dat.gz").withColumn("count_dt", lit(getYesterday(isTest)))
    val epnClickBeforeYesterday = onceClickCount(inputdir + "date=" + getBeforeYesterday(isTest) + "/dw_ams.ams_clicks_cs_*.dat.gz").withColumn("count_dt", lit(getBeforeYesterday(isTest)))

    val total = epnClickToday.union(epnClickYesterday).union(epnClickBeforeYesterday)
    total.select("count_dt", "click_hour", "click_count", "rsn_cd", "roi_fltr_yn_ind")
      .repartition(1)
      .write
      .option("header", "true")
      .csv(outputdir)
  }

  def onceClickCount(dir: String): DataFrame = {
    logger.info("onceClickCount function:" + dir)
    val epnClick = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .schema(schema_epn_click_table.dfSchema)
      .load(dir)
      .withColumn("click_hour", extractHourUdf($"click_ts"))
    val epnClick1 = epnClick
      .groupBy("click_hour")
      .agg(count($"click_id").as("click_count"))
    val epnClick2 = epnClick
      .withColumn("click_hour2", col("click_hour"))
      .drop(col("click_hour"))
      .filter($"ams_trans_rsn_cd" === "0")
      .groupBy("click_hour2")
      .agg(count($"ams_trans_rsn_cd").as("rsn_cd"))
    val epnClick3 = epnClick
      .withColumn("click_hour3", col("click_hour"))
      .drop(col("click_hour"))
      .filter($"roi_fltr_yn_ind" === "0")
      .groupBy("click_hour3")
      .agg(count($"roi_fltr_yn_ind").as("roi_fltr_yn_ind"))

    val epnClick4 = epnClick1.join(epnClick2, col("click_hour") === col("click_hour2"), "full")
      .drop(col("click_hour2"))
    val epnClick5 = epnClick4.join(epnClick3, col("click_hour") === col("click_hour3"), "full")
      .drop(col("click_hour3"))

    return epnClick5
  }

  def dailyClickTrend = {
    logger.info("dailyClickTrend function" + inputdir)
    val detailDir = getHalfMonthYesterday(inputdir, isTest)
    val dir = detailDir.split(",")
    val epnClick = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .schema(schema_epn_click_table.dfSchema)
      .load(dir(0), dir(1), dir(2), dir(3), dir(4), dir(5), dir(6), dir(7), dir(8), dir(9), dir(10), dir(11), dir(12), dir(13), dir(14))
      .withColumn("click_dt", extractDateUdf(col("click_ts")))
    val epnClick1 = epnClick
      .groupBy("click_dt")
      .agg(count($"click_id").as("click_cnt"))
    val epnClick2 = epnClick
      .withColumn("click_dt2", col("click_dt"))
      .drop(col("click_dt"))
      .filter($"ams_trans_rsn_cd" === "0")
      .groupBy("click_dt2")
      .agg(count($"ams_trans_rsn_cd").as("rsn_cd"))
    val epnClick3 = epnClick
      .withColumn("click_dt3", col("click_dt"))
      .drop(col("click_dt"))
      .filter($"roi_fltr_yn_ind" === "0")
      .groupBy("click_dt3")
      .agg(count($"roi_fltr_yn_ind").as("roi_fltr_yn_ind"))
    val epnClick4 = epnClick1.join(epnClick2, col("click_dt") === col("click_dt2"), "full")
      .drop(col("click_dt2"))
    val epnClick5 = epnClick4.join(epnClick3, col("click_dt") === col("click_dt3"), "full")
      .drop(col("click_dt3"))

    epnClick5.select("click_dt", "click_cnt", "rsn_cd", "roi_fltr_yn_ind")
      .repartition(1)
      .write
      .option("header", "true")
      .csv(outputdir)
  }

  def oneDayDomainTrend(dir: String, date: String): DataFrame = {
    logger.info("dailyClickTrend function" + dir)
    val domainTrend = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .schema(schema_epn_click_table.dfSchema)
      .load(dir)
      .withColumn("click_dt", extractDateUdf(col("click_ts"))).filter($"rfrng_dmn_name" isNotNull)
    val domainTrend1 = domainTrend.groupBy("rfrng_dmn_name").agg(count($"click_id").as("click_cnt")).sort($"click_cnt".desc).limit(3).withColumn("ranking", monotonically_increasing_id + 1)
    return domainTrend1
  }

  def dailyDomainTrend = {

    val clickTrendToday = oneDayDomainTrend(inputdir + "date=" + getToady(isTest), getToady(isTest)).withColumn("click_dt", lit(getToady(isTest)))
    val clickTrendYesterday = oneDayDomainTrend(inputdir + "date=" + getYesterday(isTest), getYesterday(isTest)).withColumn("click_dt", lit(getYesterday(isTest)))
    val clickTrendBeforeYesterday = oneDayDomainTrend(inputdir + "date=" + getBeforeYesterday(isTest), getYesterday(isTest)).withColumn("click_dt", lit(getBeforeYesterday(isTest)))

    val total = clickTrendToday.union(clickTrendYesterday).union(clickTrendBeforeYesterday)
    total.select("click_dt", "rfrng_dmn_name", "click_cnt", "ranking")
      .repartition(1)
      .write
      .option("header", "true")
      .csv(outputdir)
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

  def extractDate(ts: String): String = {
    try {
      if (ts == null)
        return "0000"
      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      val simpleDateFormat1 = new SimpleDateFormat("yyyy-MM-dd")
      val t = new Date(simpleDateFormat1.parse(ts).getTime())

      return simpleDateFormat1.format(t)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.info("error ts: " + ts)
        logger.info("exception===>:" + ex.getMessage)
        return "0000"
      }
    }
  }

  /** *
    * For test
    *
    * @param dir
    * @return
    */
  def getData(dir: String): Dataset[Row] = {
    spark.read.option("header", "true").csv(dir)
  }

  /**
    * stop the spark com.xl.traffic.chocolate.job.
    */
  def stop() = {
    spark.stop()
  }
}
