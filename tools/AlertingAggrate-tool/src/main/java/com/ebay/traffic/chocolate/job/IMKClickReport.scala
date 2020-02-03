package com.ebay.traffic.chocolate.job

import java.text.SimpleDateFormat

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

  @transient val schema_imk_click_table = TableSchema(schema_imk_click_dir)

  lazy val delimiter = "\u007F"
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
    */
  lazy val imk_channel_list = List("3", "2", "4", "16")

  lazy val channelMap =
    Map("0" -> "ROI",
      "2" -> "PaidSearch",
      "3" -> "NaturalSearch",
      "4" -> "Display",
      "16" -> "SocialMedia")

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
    val imkClickToday = onceClickCount(inputdir + "chocolate_date=" + current_date + "*")
    val imkClickYesterday = onceClickCount(inputdir + "chocolate_date=" + getDateBeforeNDay(current_date, -1) + "*")
    val imkClickBeforeYesterday = onceClickCount(inputdir + "chocolate_date=" + getDateBeforeNDay(current_date, -2) + "*")

    val total = imkClickToday.union(imkClickYesterday).union(imkClickBeforeYesterday)
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
    val imkClick = readSequenceFilesAsDF(dir, schema_imk_click_table.dfSchema, delimiter, false)
      .filter(((col("rvr_cmnd_type_cd") === click_cd) and(col("rvr_chnl_type_cd") isin(imk_channel_list:_*)))
        or((col("rvr_cmnd_type_cd") === roi_cd) and(col("rvr_chnl_type_cd") === roi_chl_cd)))
      .withColumn("click_hour", extractHourUdf(col("event_ts")))

    val imkClickCnt = imkClick.groupBy(col("rvr_chnl_type_cd").as("channel_id"), col("click_hour"),col("event_dt"))
      .agg(count(col("rvr_id")).as("click_cnt"), countDistinct(col("rvr_id")).as("distinct_click_cnt"))
      .withColumn("diff", calculateDiffUdf(col("click_cnt"), col("distinct_click_cnt")))

    imkClickCnt
  }

  /**
    * Read table files as Dataframe.
    *
    * @param inputPaths    the input paths of table files
    * @param schema        the dataframe schema of table
    * @param delimiter     the delimiter for fields in the file,
    *                      the value can be one of 'bel', 'tab', 'space', 'comma', 'del'.
    * @param broadcastHint whether to broadcast the dataframe
    * @return the dataframe
    */
  def readSequenceFilesAsDF(inputPaths: String, schema: StructType = null,
                            delimiter: String, broadcastHint: Boolean = false): DataFrame = {

    val df = spark.createDataFrame(sc.sequenceFile[String, String](inputPaths)
          .values.map(asRow(_, delimiter))
          .map(toDfRow(_, schema)).filter(_ != null), schema)
    if (broadcastHint) broadcast(df) else df
  }

  /**
    * Split the row string to fields of string array using delimiter.
    *
    * @param line   the row string
    * @param colSep the delimiter
    * @return the fields
    */
  final def asRow(line: String, colSep: String): Array[String] = {
    line.split(colSep, -1)
  }

  /**
    * Convert string array of row fields to DataFrame row
    * according to the table schema.
    *
    * @param values string array of row fields
    * @param schema dataframe schema
    * @return dataframe row
    */
  def toDfRow(values: Array[String], schema: StructType): Row = {
//    require(values.length == schema.fields.length
//      || values.length == schema.fields.length + 1)
    if(values == null || ((values.length != schema.fields.length) && (values.length != schema.fields.length + 1))){
      logger.warn(values.mkString("|"))
      null
    } else{
      try {
        Row(values zip schema map (e => {
          if (e._1.length == 0) {
            null
          } else {
            e._2.dataType match {
              case _: StringType => e._1.trim
              case _: LongType => e._1.trim.toLong
              case _: IntegerType => e._1.trim.toInt
              case _: ShortType => e._1.trim.toShort
              case _: FloatType => e._1.trim.toFloat
              case _: DoubleType => e._1.trim.toDouble
              case _: ByteType => e._1.trim.toByte
              case _: BooleanType => e._1.trim.toBoolean
            }
          }
        }): _*)
      } catch {
        case ex: Exception => {
          corruptRows.set(corruptRows.get + 1)
          if (corruptRows.get() <= MAX_CORRUPT_ROWS) {
            logger.warn("Failed to parse row: " + values.mkString("|"), ex)
            null
          } else {
            logger.error("Two many corrupt rows.")
            throw ex
          }
        }
      }
    }
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