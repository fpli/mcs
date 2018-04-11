package com.ebay.traffic.chocolate.sparknrt.capping.rules

import java.text.SimpleDateFormat
import java.util.Calendar

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.capping.{CappingRule, Parameter}
import com.ebay.traffic.chocolate.sparknrt.meta.DateFiles
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{count, lit, split, sum}
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

// only for test
class SnidCappingRule(params: Parameter, bit: Long, dateFiles: DateFiles, cappingRuleJobObj: BaseSparkJob) extends CappingRule {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  lazy val baseDir = params.workDir + "/capping/" + params.channel + "/snid/"
  lazy val baseTempDir = baseDir + "/tmp/"

  lazy val DATE_COL = "date"

  @transient lazy val hadoopConf = {
    new Configuration()
  }

  @transient lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  @transient lazy val sdf = new SimpleDateFormat("yyyy-MM-dd")

  override def preTest() = {}

  lazy val cappingBit = bit

  import cappingRuleJobObj.spark.implicits._

  override def test(): DataFrame = {
    // filter click only, count ip and save to tmp file
    var dfIP = cappingRuleJobObj.readFilesAsDFEx(dateFiles.files).filter($"channel_action" === "CLICK")
      .select(split($"request_headers", "X-EBAY-CLIENT-IP: ")(1).alias("tmpIP"))
      .select(split($"tmpIP", """\|""")(0).alias("IP"))
      .groupBy($"IP").agg(count(lit(1)).alias("count"))
      .drop($"request_headers")
      .drop($"tmpIP")
    cappingRuleJobObj.saveDFToFiles(dfIP, baseTempDir + DATE_COL + "=" + dateFiles.date)

    // rename tmp files to final files
    val dateOutputPath = new Path(baseDir + DATE_COL + "=" + dateFiles.date)
    var max = -1
    if (fs.exists(dateOutputPath)) {
      val outputStatus = fs.listStatus(dateOutputPath)
      if (outputStatus.length > 0) {
        max = outputStatus.map(status => {
          val name = status.getPath.getName
          Integer.valueOf(name.substring(5, name.indexOf(".")))
        }).sortBy(i => i).last
      }
    } else {
      fs.mkdirs(dateOutputPath)
    }

    val fileStatus = fs.listStatus(new Path(baseTempDir + DATE_COL + "=" + dateFiles.date))
    val files = fileStatus.filter(status => status.getPath.getName != "_SUCCESS")
      .zipWithIndex
      .map(swi => {
        val src = swi._1.getPath
        val seq = ("%5d" format max + 1 + swi._2).replace(" ", "0")
        val target = new Path(dateOutputPath, s"part-${seq}.snappy.parquet")
        logger.info("Rename from: " + src.toString + " to: " + target.toString)
        fs.rename(src, target)
        target.toString
      })

    // delete the tmp dir
    fs.delete(new Path(baseTempDir), true)

    // IP rule
    var df = cappingRuleJobObj.readFilesAsDFEx(dateFiles.files)
      .withColumn("tmpIP", split($"request_headers", "X-EBAY-CLIENT-IP: ")(1))
      .withColumn("IP_1", split($"tmpIP", """\|""")(0))
      .drop($"tmpIP")

    val ipCountPath1 = baseDir + DATE_COL + "=" + dateFiles.date
    val cal = Calendar.getInstance
    cal.setTime(sdf.parse((dateFiles.date.asInstanceOf[String])))
    cal.add(Calendar.DATE, -1)
    val dateBefore1Day = cal.getTime
    val ipCountPath2 = baseDir + DATE_COL + "=" + sdf.format(dateBefore1Day)
    var ipCountPath: Array[String] = null
    if (fs.exists(new Path(ipCountPath2))) {
      ipCountPath = Array(ipCountPath1, ipCountPath2)
    }
    else {
      ipCountPath = Array(ipCountPath1)
    }
    dfIP = cappingRuleJobObj.readFilesAsDFEx(ipCountPath).groupBy($"IP").agg(sum($"count") as "amnt").filter($"amnt" === 2)
      .withColumn("capping", lit(cappingBit)).drop($"count").drop($"amnt")

    df = df.join(dfIP, $"IP_1" === $"IP", "left_outer")
      .select(df.col("*"), $"capping")
      .drop("IP_1")
      .drop("IP")
    df
  }

  override def postTest() = {}
}
