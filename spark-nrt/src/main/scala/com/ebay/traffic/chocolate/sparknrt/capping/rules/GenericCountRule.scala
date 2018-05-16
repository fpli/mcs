package com.ebay.traffic.chocolate.sparknrt.capping.rules

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.capping.{CappingRule, Parameter}
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
  * Created by jialili1 on 5/11/18.
  */
abstract class GenericCountRule(params: Parameter, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob) extends CappingRule{

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  @transient lazy val hadoopConf = {
    new Configuration()
  }

  @transient lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  @transient var properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("capping_rule.properties"))
    properties
  }

  def getThreshold(ruleName: String) = {
    properties.getProperty(ruleName).toInt
  }

  def getTimeWindow(ruleType: String) = {
    properties.getProperty(ruleType).toLong
  }

  lazy val fileName = ""
  lazy val baseDir = params.workDir + "/capping/" + params.channel + fileName
  lazy val baseTempDir = baseDir + "/tmp/"
  logger.info("baseDir: " + baseDir)

  override def preTest() = {
    fs.delete(new Path(baseTempDir), true)
    fs.mkdirs(new Path(baseTempDir))
  }

  @transient lazy val sdf = new SimpleDateFormat("yyyy-MM-dd")
  lazy val DATE_COL = "date"
  lazy val cappingBit = bit

  def reduceAndRename(df: DataFrame, timestamp: Long)= {
    // reduce the number of ip count file to 1
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

  def readCountData(timestamp: Long, timeWindow: Long) = {
    val ipCountTempPathToday = baseTempDir + dateFiles.date
    val ipCountPathToday = baseDir + dateFiles.date
    val cal = Calendar.getInstance
    cal.setTime(sdf.parse((dateFiles.date.asInstanceOf[String]).substring(DATE_COL.length + 1)))
    cal.add(Calendar.DATE, -1)
    val dateBefore1Day = cal.getTime
    val ipCountTempPathYesterday = baseTempDir + DATE_COL + "=" + sdf.format(dateBefore1Day)
    val ipCountPathYesterday = baseDir + DATE_COL + "=" + sdf.format(dateBefore1Day)
    var ipCountPath: List[String] = List()
    ipCountPath = ipCountPath :+ ipCountTempPathToday
    if (fs.exists(new Path(ipCountPathToday))) {
      ipCountPath = ipCountPath :+ ipCountPathToday
    }
    // read yesterday's data
    if (fs.exists(new Path(ipCountPathYesterday))) {
      val fileStatus = fs.listStatus(new Path(ipCountPathYesterday))
          .filter(status => String.valueOf(status.getPath.getName.substring(DATE_COL.length + 1, status.getPath.getName.indexOf("."))) >= (timestamp - timeWindow).toString)
          .map(status => ipCountPath = ipCountPath :+ status.getPath.toString)
    }
    ipCountPath
  }

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
