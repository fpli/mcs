package com.ebay.traffic.chocolate.sparknrt.capping.rules

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.ebay.traffic.monitoring.{ESMetrics, Metrics}
import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.capping.{CappingRule, Parameter}
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.LoggerFactory

/**
  * Created by xiangli4 on 5/29/18.
  * Generic rule defining basic functions
  * Capping rule steps between preTest and postTest:
  * 1. Load capping data in current batch
  * 2. Save capping data in capping dir
  * 3. Get capping data paths containing current batch and look back for data in the time window
  * 4. Read current batch data again, prepare for capping algorithm
  * 5. Apply capping rule using df in 4, get capping failed records df. Only get snapshot_id and capping columns
  * 6. Join current batch data with capping failed records df
  */
abstract class GenericRule(params: Parameter, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob, window: String)
  extends CappingRule with Serializable {
  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  lazy val METRICS_INDEX_PREFIX = "chocolate-metrics-";
  @transient lazy val metrics: Metrics = {
    if (params.elasticsearchUrl != null && !params.elasticsearchUrl.isEmpty) {
      ESMetrics.init(METRICS_INDEX_PREFIX, params.elasticsearchUrl)
      ESMetrics.getInstance()
    } else null
  }

  @transient lazy val hadoopConf = {
    new Configuration()
  }

  @transient lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  //get timeWindow
  @transient lazy val properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("capping_rule.properties"))
    properties
  }

  // used to get threshold
  lazy val ruleName = this.getClass.getSimpleName + "_" + window
  // used to get time window
  lazy val ruleType = "timeWindow_" + window
  // time window
  lazy val timeWindow = properties.getProperty(ruleType).toLong

  // preTest
  lazy val fileName = ""
  lazy val baseDir = params.workDir + "/capping/" + params.channel + fileName
  lazy val baseTempDir = baseDir + "/tmp/"
  lazy val outputDir = params.outputDir + params.channel + "/capping/"
  logger.info("baseDir: " + baseDir)

  override def preTest() = {
    fs.delete(new Path(baseTempDir), true)
    fs.mkdirs(new Path(baseTempDir))
  }

  @transient lazy val sdf = new SimpleDateFormat("yyyy-MM-dd")
  lazy val DATE_COL = "date"
  lazy val DATE_COL_EQUALS = "date="
  lazy val cappingBit = bit

  // filter in job
  def dfFilterInJob(filterCol: Column): DataFrame = {
    var df = cappingRuleJobObj.readFilesAsDFEx(dateFiles.files)
    if (filterCol != null)
      df = df.filter(filterCol)
    df
  }

  // select specific columns in the job
  def dfSelectionInJob(dfSelection: DataFrame, selectCols: Array[Column]): DataFrame = {
    dfSelection.select(selectCols: _*)
  }

  // 1. Load capping data in current batch
  def dfLoadCappingInJob(dfCapping: DataFrame, selectCols: Array[Column]): DataFrame

  // 2. Save capping data in capping dir, and rename file name to include timestamp
  def saveCappingInJob(df: DataFrame, timestamp: Long)= {
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

  // 3. Get capping data paths containing current batch and look back for data in the time window
  def getCappingDataPath(timestamp: Long): List[String] = {
    val cappingTempPathToday = baseTempDir + dateFiles.date
    val cappingPathToday = baseDir + dateFiles.date
    val cal = Calendar.getInstance
    cal.setTime(sdf.parse(dateFiles.date.asInstanceOf[String].substring(DATE_COL.length + 1)))
    cal.add(Calendar.DATE, -1)
    val dateBefore1Day = cal.getTime
    val prevTempPathYesterday = baseTempDir + DATE_COL + "=" + sdf.format(dateBefore1Day)
    val prevPathYesterday = baseDir + DATE_COL + "=" + sdf.format(dateBefore1Day)
    var cappingPath: List[String] = List()
    if (fs.exists(new Path(cappingTempPathToday))) {
      cappingPath = cappingPath :+ cappingTempPathToday
    }
    //read today's data, filter by time window
    if (fs.exists(new Path(cappingPathToday))) {
      if (window == "long")
        cappingPath = cappingPath :+ cappingPathToday
      else {
        val fileStatus = fs.listStatus(new Path(cappingPathToday))
          .filter(status => String.valueOf(status.getPath.getName.substring(DATE_COL.length + 1, status.getPath.getName.indexOf(".")))
            >= (timestamp - timeWindow).toString)
          .map(status => cappingPath = cappingPath :+ status.getPath.toString)
      }
    }
    // read yesterday's data, filter by time window
    if (fs.exists(new Path(prevPathYesterday))) {
      val fileStatus = fs.listStatus(new Path(prevPathYesterday))
        .filter(status => String.valueOf(status.getPath.getName.substring(DATE_COL.length + 1, status.getPath.getName.indexOf(".")))
          >= (timestamp - timeWindow).toString)
        .map(status => cappingPath = cappingPath :+ status.getPath.toString)
    }
    cappingPath
  }

  // 4. Read current batch data again, prepare for capping algorithm
  def dfForJoin(addCol: Column, withColumnCol: Column, selectCols: Array[Column]): DataFrame = {
    if (addCol != null && withColumnCol != null) {
      cappingRuleJobObj.readFilesAsDFEx(dateFiles.files)
      .withColumn(addCol.toString(), withColumnCol)
      .select(selectCols: _*)
    }
    else {
      cappingRuleJobObj.readFilesAsDFEx(dateFiles.files).select(selectCols: _*)
    }
  }

  // for capping rule which need to add two columns
  def dfForJoin2(addCol1: Column, withColumnCol1: Column, addCol2: Column, withColumnCol2: Column,
                 selectCols: Array[Column]): DataFrame = {
    if (addCol1 != null && withColumnCol1 != null && addCol2 != null && withColumnCol2 != null) {
      cappingRuleJobObj.readFilesAsDFEx(dateFiles.files)
        .withColumn(addCol1.toString(), withColumnCol1)
        .withColumn(addCol2.toString(), withColumnCol2)
        .select(selectCols: _*)
    }
    else {
      cappingRuleJobObj.readFilesAsDFEx(dateFiles.files).select(selectCols: _*)
    }
  }

  // 5. Apply capping rule using df in 4, get capping failed records df
  def dfCappingInJob(dfJoin: DataFrame, cappingPath: List[String]): DataFrame

  import cappingRuleJobObj.spark.implicits._

  val cols: Array[Column]

  // return manually when there is no events in the job
  def dfNoEvents(): DataFrame = {
    cappingRuleJobObj.readFilesAsDFEx(dateFiles.files)
      .withColumn("capping", lit(0l))
      .select($"snapshot_id", $"capping")
  }

  // 6. Join current batch data with capping failed records df
  def dfJoin(df: DataFrame, dfCapping: DataFrame, joinCol: Column): DataFrame = {
    var df1 = df.join(dfCapping, joinCol, "left_outer")
      .select($"snapshot_id", $"capping")
    df1
  }

  // postTest
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
