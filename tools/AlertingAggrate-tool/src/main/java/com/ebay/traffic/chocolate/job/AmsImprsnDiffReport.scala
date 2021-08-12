package com.ebay.traffic.chocolate.job

import java.text.SimpleDateFormat
import java.util.Calendar

import com.ebay.traffic.chocolate.job.util.AmsDiffReportGenerator
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object AmsImprsnDiffReport extends App {

  override def main(args: Array[String]): Unit = {
    val params: Parameter = Parameter(args)
    val job = new AmsImprsnDiffReport(params)
    job.run()
    job.stop()
  }

}
class AmsImprsnDiffReport(params: Parameter)  {
  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Whether the spark com.xl.traffic.chocolate.job is in local mode
    */
  @transient lazy val isTest: Boolean = {
    params.mode.indexOf("test") == 0
  }

  /**
    * The spark session, which is the entrance point of DataFrame, DataSet and Spark SQL.
    */
  @transient lazy val spark = {
    val builder: SparkSession.Builder = SparkSession.builder().appName("AmsImprsnDiffReportMain")

    if (isTest) {
      logger.info("Test mode")
      builder.master("local")
        .appName("SparkUnitTesting")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir"))
      // for test, hive support is not enabled. Use in-memory catalog implementation
    }
    builder.enableHiveSupport()
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
  def getCheckDay(): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -5)
    dateFormat.format(cal.getTime)
  }
  def run(): Unit = {
    generateImprsnDiffReport()
  }

  def generateImprsnDiffReport() ={
    val totalCount: (Long, Long) = getTotalCount()
    val userIdPercent: (Double, Double) = getUserIdNotNullPercent()
    val html: String = AmsDiffReportGenerator.getImprsnTable(
      totalCount,
      userIdPercent,
      getDiffColumnsAndCount()
    )
    saveContentToFile(html,params.outputPath)
  }
  def getTotalCount():(Long,Long)={
    val newTotalCount:Long=sqlsc.sql("select count(*) from choco_data.ams_imprsn_new_test where imprsn_dt=\""+getCheckDay()+"\"").head().getLong(0)
    val oldTotalCount:Long=sqlsc.sql("select count(*) from choco_data.ams_imprsn_old_test where imprsn_dt=\""+getCheckDay()+"\"").head().getLong(0)
    (newTotalCount,oldTotalCount)
  }
  def getUserIdNotNullPercent():(Double, Double)={
    val newUserIdNotNullPercent:Double=sqlsc.sql("select round((select count(*) from choco_data.ams_imprsn_new_test where imprsn_dt=\""+getCheckDay()+"\" and  lower(brwsr_name) not like '%bot%' and USER_ID <>0 AND USER_ID <>-1 AND  USER_ID IS NOT NULL)*100" +
      "/(SELECT count(*) from choco_data.ams_imprsn_new_test where imprsn_dt=\""+getCheckDay()+"\" and lower(brwsr_name) not like '%bot%'),2)").head.getDouble(0)*100
    val oldUserIdNotNullPercent:Double=sqlsc.sql("select round((select count(*) from choco_data.ams_imprsn_old_test where imprsn_dt=\""+getCheckDay()+"\" and  lower(brwsr_name) not like '%bot%' and USER_ID <>0 AND USER_ID <>-1 AND  USER_ID IS NOT NULL)*100" +
      "/(SELECT count(*) from choco_data.ams_imprsn_old_test where imprsn_dt=\""+getCheckDay()+"\" and lower(brwsr_name) not like '%bot%'),2)").head.getDouble(0)*100
    (newUserIdNotNullPercent,oldUserIdNotNullPercent)
  }
  def getDiffColumnsAndCount():mutable.LinkedHashMap[String,Long]={
    val map=new mutable.LinkedHashMap[String,Long]()
    val df: DataFrame =sqlsc.sql("select * from choco_data.epnnrt_imprsn_automation_diff where imprsn_dt=\""+getCheckDay()+"\"")
    val columns: Array[String] = df.columns
    var index:Int=0
    //diff every two columns(new and old), if they are different, add this column to map
    while(index+1<columns.length) {
      //the columns' name will be like new_imprsn_id and old_imprsn_id, 'substring' function is to get final column name
      val count: Long =df.select(columns(index),columns(index+1))
        .filter(col(columns(index))=!=col(columns(index+1)))
        .count()
      if(count!=0) {
        val columnName: String = columns(index).substring(4)
        logger.info("{} has different value",columnName)
        map.put(columnName,count)
      }
      index+=2
    }
    map.put("Total",df.count())
    map
  }
  /**
    * stop the spark com.xl.traffic.chocolate.job.
    */
  def stop() = {
    spark.stop()
  }
  def saveContentToFile(content:String,path:String): Unit ={
    var outputStream: FSDataOutputStream = null
    try {
      outputStream = fs.create(new Path(path))
      outputStream.writeChars(content)
      outputStream.flush()
    } finally {
      if (outputStream != null) {
        outputStream.close()
      }
    }
  }
}
