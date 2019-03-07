package com.ebay.traffic.chocolate.sparknrt.capping.rules

import java.text.SimpleDateFormat
import java.util.Calendar

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.capping.{CappingRule, Parameter}
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/**
  * Created by xiangli4 on 4/8/18.
  */
class IPCappingRule(params: Parameter, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob) extends CappingRule {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  @transient lazy val hadoopConf = {
    new Configuration()
  }

  @transient lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  @transient lazy val sdf = new SimpleDateFormat("yyyy-MM-dd")
  lazy val DATE_COL = "date"

  lazy val baseDir = params.workDir + "/capping/" + params.channel + "/ip/"
  lazy val baseTempDir = baseDir + "/tmp/"

  import cappingRuleJobObj.spark.implicits._

  override def preTest() = {
    fs.delete(new Path(baseTempDir), true)
    fs.mkdirs(new Path(baseTempDir))
  }

  lazy val cappingBit = bit

  //parse IP from request_headers
  def ip(alias: String): Column = {
    $"remote_ip".alias(alias)
  }

  override def test(): DataFrame = {

    // filter click only, count ip and save to tmp file
    var dfIP = cappingRuleJobObj.readFilesAsDFEx(dateFiles.files).filter($"channel_action" === "CLICK")
    val head = dfIP.take(1)
    if(head.length == 0) {
      var df = cappingRuleJobObj.readFilesAsDFEx(dateFiles.files)
          .withColumn("capping", lit(0l))
          .select($"snapshot_id", $"capping")
      df
    }
    else {
      val firstRow = head(0)
      val timestamp = firstRow.getLong(firstRow.fieldIndex("timestamp"));

      dfIP = dfIP.select(ip("IP"))
        .groupBy($"IP").agg(count(lit(1)).alias("count"))

      // reduce the number of ip count file to 1
      dfIP = dfIP.repartition(1)
      val tempPath = baseTempDir + dateFiles.date
      cappingRuleJobObj.saveDFToFiles(dfIP, tempPath)

      // rename file name to include timestamp
      val fileStatus = fs.listStatus(new Path(tempPath))
      val src = fileStatus.filter(status => status.getPath.getName != "_SUCCESS")(0).getPath
      val target = new Path(tempPath, s"part-${timestamp}.snappy.parquet")
      fs.rename(src, target)

      // IP rule
      var df = cappingRuleJobObj.readFilesAsDFEx(dateFiles.files).filter($"channel_action" === "CLICK")
        .select(ip("IP_1"), $"snapshot_id")

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
      // read only 24 hours data
      if (fs.exists(new Path(ipCountPathYesterday))) {
        val fileStatus = fs.listStatus(new Path(ipCountPathYesterday))
          .filter(status => String.valueOf(status.getPath.getName.substring(DATE_COL.length + 1, status.getPath.getName.indexOf("."))) >= (timestamp - 86400000).toString)
          .map(status => ipCountPath = ipCountPath :+ status.getPath.toString)
      }

      dfIP = cappingRuleJobObj.readFilesAsDFEx(ipCountPath.toArray).groupBy($"IP").agg(sum($"count") as "amnt").filter($"amnt" >= params.ipThreshold)
        .withColumn("capping", lit(cappingBit)).drop($"count").drop($"amnt")

      df = df.join(dfIP, $"IP_1" === $"IP", "left_outer")
        .select($"snapshot_id", $"capping")
      df
    }
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
