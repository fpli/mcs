package com.ebay.traffic.chocolate.job

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object AmsClickDiffReport extends App {

  override def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      println("Wrong arguments")
    }
    val clickDt: String=args(0)
    val outputPath:String = args(1)
    val mode: String = args(2)


    val job = new AmsClickDiffReport(clickDt,outputPath, mode)
    job.run()
    job.stop()
  }

}
class AmsClickDiffReport(val clickDt:String,val outputPath: String, val mode: String = "local[4]") extends Serializable {
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

  }
  def getTotalCount():(Long,Long)={
    val newTotalCount:Long=sqlsc.sql("select count(*) from choco_data.ams_click_new_test where click_dt=\""+clickDt+"\"").head().getLong(0)
    val oldTotalCount:Long=sqlsc.sql("select count(*) from choco_data.ams_click_old_test where click_dt=\""+clickDt+"\"").head().getLong(0)
    (newTotalCount,oldTotalCount)
  }
  def getUserIdNotNullPercent:(Double, Double)={
    val newUserIdNotNullPercent:Double=sqlsc.sql("select count(*) from choco_data.ams_click_new_test where click_dt=\""+clickDt+"\"and  lower(brwsr_name) not like '%bot%' and USER_ID <>0 AND USER_ID <>-1 AND  USER_ID IS NOT NULL )" +
      "/(SELECT count(*) from choco_data.ams_click_new_test where click_dt=\""+clickDt+"\" and lower(brwsr_name) not like '%bot%'").head.getDouble(0)
    val oldUserIdNotNullPercent:Double=sqlsc.sql("select count(*) from choco_data.ams_click_old_test where click_dt=\""+clickDt+"\"and  lower(brwsr_name) not like '%bot%' and USER_ID <>0 AND USER_ID <>-1 AND  USER_ID IS NOT NULL )" +
      "/(SELECT count(*) from choco_data.ams_click_old_test where click_dt=\""+clickDt+"\" and lower(brwsr_name) not like '%bot%'").head.getDouble(0)
    (newUserIdNotNullPercent,oldUserIdNotNullPercent)
  }
  def getLastVwdItemIdNotNullPercent:(Double, Double)={
    val newLastVwdItemIdNotNullPercent:Double=sqlsc.sql("select count(*) from choco_data.ams_click_new_test where click_dt=\""+clickDt+"\"and  lower(brwsr_name) not like '%bot%' and LAST_VWD_ITEM_ID is not null )" +
      "/(SELECT count(*) from choco_data.ams_click_new_test where click_dt=\""+clickDt+"\" and lower(brwsr_name) not like '%bot%'").head.getDouble(0)
    val oldLastVwdItemIdNotNullPercent:Double=sqlsc.sql("select count(*) from choco_data.ams_click_old_test where click_dt=\""+clickDt+"\"and  lower(brwsr_name) not like '%bot%' and LAST_VWD_ITEM_ID is not null )" +
      "/(SELECT count(*) from choco_data.ams_click_old_test where click_dt=\""+clickDt+"\" and lower(brwsr_name) not like '%bot%'").head.getDouble(0)
    (newLastVwdItemIdNotNullPercent,oldLastVwdItemIdNotNullPercent)
  }
  def getDiffColumnsAndCount:mutable.HashMap[String,Long]={
    val map=new mutable.HashMap[String,Long]()
    val df: DataFrame =sqlsc.sql("select * from choco_data.epnnrt_click_automation_diff where click_dt=\""+clickDt+"\"")
    val columns: Array[String] = df.columns
    var index:Int=0
    //diff every two columns(new and old), if they are different, add this column to map
    while(index<columns.length) {
      //the columns' name will be like new_click_id and old_click_id, 'trim' function is to get final column name
      map.put(columns(index).substring(4), df.select(columns(index),columns(index+1))
        .filter(col(columns(index))=!=col(columns(index+1)))
        .count())
      index+=2
    }
    map
  }
  /**
    * stop the spark com.xl.traffic.chocolate.job.
    */
  def stop() = {
    spark.stop()
  }
}
