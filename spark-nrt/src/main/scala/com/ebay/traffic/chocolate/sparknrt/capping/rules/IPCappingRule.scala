package com.ebay.traffic.chocolate.sparknrt.capping.rules

import java.text.SimpleDateFormat
import java.util.Calendar

import com.ebay.traffic.chocolate.spark.BaseSparkJob
import com.ebay.traffic.chocolate.sparknrt.capping.{CappingRule, Parameter}
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, Metadata}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.sum

/**
  * Created by xiangli4 on 4/8/18.
  */
class IPCappingRule(params: Parameter)
  extends BaseSparkJob(params.appName, params.mode) with CappingRule {

  @transient lazy val hadoopConf = {
    new Configuration()
  }

  @transient lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  @transient lazy val inputMetadata = {
    Metadata(params.inputDir, params.channel)
  }

  @transient lazy val outputMetadata = {
    Metadata(params.workDir, params.channel)
  }

  lazy val DATE_COL = "date"

  @transient lazy val sdf = new SimpleDateFormat("yyyy-MM-dd")

  lazy val inputDir = params.inputDir
  lazy val baseDir = params.workDir + "/capping/" + params.channel + "/ip/"
  lazy val baseTempDir = baseDir + "/tmp/"
  lazy val outputDir = params.outputDir

  import spark.implicits._

  override def cleanBaseDir() = {
//    fs.delete(new Path(baseDir), true)
//    fs.mkdirs(new Path(baseDir))
//    fs.mkdirs(new Path(baseTempDir))
  }

  override def test(dateFiles: DateFiles): DataFrame = {

    // filter click only, count ip and save to tmp file
    var dfIP = readFilesAsDFEx(dateFiles.files).filter($"channel_action" === "CLICK")
      .select(split($"request_headers", "X-EBAY-CLIENT-IP: ")(1).alias("tmpIP"))
      .select(split($"tmpIP", """\|""")(0).alias("IP"))
      .groupBy($"IP").agg(count(lit(1)).alias("count"))
      .drop($"request_headers")
      .drop($"tmpIP")
    saveDFToFiles(dfIP, baseTempDir + DATE_COL + "=" + dateFiles.date)

    // IP rule
    var df = readFilesAsDFEx(dateFiles.files)
      .withColumn("tmpIP", split($"request_headers", "X-EBAY-CLIENT-IP: ")(1))
      .withColumn("IP_1", split($"tmpIP", """\|""")(0))
      .drop($"tmpIP")

    val ipCountTempPathToday = baseTempDir + DATE_COL + "=" + dateFiles.date
    val ipCountPathToday = baseDir + DATE_COL + "=" + dateFiles.date
    val cal = Calendar.getInstance
    cal.setTime(sdf.parse((dateFiles.date.asInstanceOf[String])))
    cal.add(Calendar.DATE, -1)
    val dateBefore1Day = cal.getTime
    val ipCountTempPathYesterday = baseTempDir + DATE_COL + "=" + sdf.format(dateBefore1Day)
    val ipCountPathYesterday = baseDir + DATE_COL + "=" + sdf.format(dateBefore1Day)
    var ipCountPath: List[String] = List()
    ipCountPath = ipCountPath:+ipCountTempPathToday
    if(fs.exists(new Path(ipCountPathToday))) {
      ipCountPath=ipCountPath:+ipCountPathToday
    }
    if (fs.exists(new Path(ipCountTempPathYesterday))) {
      ipCountPath=ipCountPath:+ipCountTempPathYesterday
    }
    if(fs.exists(new Path(ipCountPathYesterday))) {
      ipCountPath=ipCountPath:+ipCountPathYesterday
    }

    dfIP = readFilesAsDFEx(ipCountPath.toArray).groupBy($"IP").agg(sum($"count") as "amnt").filter($"amnt" > params.ipThreshold)
      .withColumn("filter_failed_1", lit("IPCapping")).drop($"count").drop($"amnt")

    df = df.join(dfIP, $"IP_1" === $"IP", "left_outer")
      .select(df.col("*"), $"filter_failed_1")
      .drop($"filter_failed")
      .withColumnRenamed("filter_failed_1","filter_failed")
      .drop("IP_1")
      .drop("IP")
      .drop("filter_failed_1")
    df
  }

  override def renameBaseTempFiles(dateFiles: DateFiles) = {
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
  }

  /**
    * No need to run. Just implement base class method.
    */
  override def run(): Unit = {

  }
}
