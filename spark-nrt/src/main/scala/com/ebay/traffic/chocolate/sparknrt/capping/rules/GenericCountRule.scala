package com.ebay.traffic.chocolate.sparknrt.capping.rules

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.capping.{CappingRule, Parameter}
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{count, lit, sum}
import org.slf4j.LoggerFactory

/**
  * Created by jialili1 on 5/11/18.
  */
abstract class GenericCountRule(params: Parameter, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob, window: String) extends CappingRule{

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  @transient lazy val hadoopConf = {
    new Configuration()
  }

  @transient lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  //get threshold and timeWindow
  @transient var properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("capping_rule.properties"))
    properties
  }

  lazy val ruleName = this.getClass.getSimpleName + "_" + window
  lazy val ruleType = "timeWindow_" + window
  lazy val threshold = properties.getProperty(ruleName).toInt
  lazy val timeWindow = properties.getProperty(ruleType).toLong

  //preTest
  lazy val fileName = ""
  lazy val baseDir = params.workDir + "/capping/" + params.channel + fileName
  lazy val baseTempDir = baseDir + "/tmp/"
  logger.info("baseDir: " + baseDir)

  override def preTest() = {
    fs.delete(new Path(baseTempDir), true)
    fs.mkdirs(new Path(baseTempDir))
  }

  //test begin
  @transient lazy val sdf = new SimpleDateFormat("yyyy-MM-dd")
  lazy val DATE_COL = "date"
  lazy val cappingBit = bit

  // reduce the number of counting file to 1, and rename file name to include timestamp
  def repartitionAndRename(df: DataFrame, timestamp: Long)= {
    // reduce the number of count file to 1
    val df1 = df.repartition(1)
    val tempPath = baseTempDir + dateFiles.date
    cappingRuleJobObj.saveDFToFiles(df1, tempPath)

    // rename file name to include timestamp
    val fileStatus = fs.listStatus(new Path(tempPath))
    val src = fileStatus.filter(status => status.getPath.getName != "_SUCCESS")(0).getPath
    val target = new Path(tempPath, s"part-${timestamp}.snappy.parquet")
    fs.rename(src, target)
    logger.info("Rename " + src + " to " + target)
  }

  //read previous data and add to count path
  def readCountData(timestamp: Long) = {
    val countTempPathToday = baseTempDir + dateFiles.date
    val countPathToday = baseDir + dateFiles.date
    val cal = Calendar.getInstance
    cal.setTime(sdf.parse((dateFiles.date.asInstanceOf[String]).substring(DATE_COL.length + 1)))
    cal.add(Calendar.DATE, -1)
    val dateBefore1Day = cal.getTime
    val countTempPathYesterday = baseTempDir + DATE_COL + "=" + sdf.format(dateBefore1Day)
    val countPathYesterday = baseDir + DATE_COL + "=" + sdf.format(dateBefore1Day)
    var countPath: List[String] = List()
    countPath = countPath :+ countTempPathToday
    if (fs.exists(new Path(countPathToday))) {
      countPath = countPath :+ countPathToday
    }
    // read yesterday's data
    if (fs.exists(new Path(countPathYesterday))) {
      val fileStatus = fs.listStatus(new Path(countPathYesterday))
          .filter(status => String.valueOf(status.getPath.getName.substring(DATE_COL.length + 1, status.getPath.getName.indexOf("."))) >= (timestamp - timeWindow).toString)
          .map(status => countPath = countPath :+ status.getPath.toString)
    }
    countPath
  }

  import cappingRuleJobObj.spark.implicits._

  val cols: Array[Column]

  //filter click only, and filter according to specific condition
  def dfFilterInJob(filterCol: Column): DataFrame ={
    var df = cappingRuleJobObj.readFilesAsDFEx(dateFiles.files)
        .filter($"channel_action" === "CLICK")
    if (filterCol != null)
      df = df.filter(filterCol)
    df
  }

  //count by specific columns in the job
  def dfCountInJob(dfCount: DataFrame, selectCols: Array[Column]): DataFrame = {
    dfCount.select(selectCols: _*)
        .groupBy(cols: _*).agg(count(lit(1)).alias("count"))
  }

  //Result df for join purpose
  def dfForJoin(addCol: Column, withColumnCol: Column): DataFrame = {
    cappingRuleJobObj.readFilesAsDFEx(dateFiles.files)
        .withColumn(addCol.toString(), withColumnCol)
  }

  //count through whole timeWindow and filter those over threshold
  def dfCountAllAndFilter(dfCount: DataFrame, countPath: List[String]): DataFrame = {
    cappingRuleJobObj.readFilesAsDFEx(countPath.toArray)
        .groupBy(cols: _*)
        .agg(sum("count") as "amnt")
        .filter($"amnt" >= threshold)
        .withColumn("capping", lit(cappingBit))
        .drop("count")
        .drop("amnt")
  }

  //join origin df and counting df
  def dfJoin(df: DataFrame, dfCount: DataFrame, joinCol: Column, dropCol: Column): DataFrame = {
    var df1 = df.join(dfCount, joinCol, "left_outer")
        .select(df.col("*"), dfCount.col("capping"))
    if (dropCol != null)
      df1 = df1.drop(dropCol)
    df1
  }
  //test end

  //postTest
  override def postTest() = {
    // rename tmp files to final files
    val dateOutputPath = new Path(baseDir + dateFiles.date)
    if (!fs.exists(dateOutputPath)) {
      fs.mkdirs(dateOutputPath)
    }
    if(fs.exists(new Path(baseTempDir + dateFiles.date))) {
      val fileStatus = fs.listStatus(new Path(baseTempDir + dateFiles.date))
      val src = fileStatus.filter(status => status.getPath.getName != "_SUCCESS").toList(0).getPath
      val fileName = src.getName
      val dest = new Path(dateOutputPath, fileName)
      fs.rename(src, dest)

      // delete the tmp dir
      fs.delete(new Path(baseTempDir), true)
    }
  }
}
