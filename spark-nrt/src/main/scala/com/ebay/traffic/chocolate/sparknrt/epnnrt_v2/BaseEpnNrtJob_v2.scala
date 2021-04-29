package com.ebay.traffic.chocolate.sparknrt.epnnrt_v2

import java.text.SimpleDateFormat
import java.util.Properties

import com.ebay.app.raptor.chocolate.avro.ChannelType
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics
import org.apache.hadoop.fs.Path

/**
  * Created by zhofan on 6/18/20.
  */
abstract class BaseEpnNrtJob_v2(params: Parameter_v2,
                                override val jobName: String,
                                override val mode: String = "yarn") extends BaseSparkNrtJob(jobName, mode) {

  /**
    * epnnrt job dir
    */
  lazy val outputDir = params.outputDir
  lazy val inputWorkDir = params.inputWorkDir
  lazy val outputWorkDir = params.outputWorkDir
  lazy val epnNrtTempDir = outputDir + "/tmp/"
  lazy val archiveDir = outputWorkDir + "/meta/EPN/output/archive/"

  /**
    * epnnrt job properties
    */
  @transient lazy val properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("epnnrt_v2.properties"))
    properties.load(getClass.getClassLoader.getResourceAsStream("sherlockio.properties"))
    properties
  }

  /**
    * epnnrt job metadata--input
    */
  @transient lazy val inputMetadata: Metadata = {
    val usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("epnnrt.upstream.epn"))
    Metadata(params.inputWorkDir, ChannelType.EPN.toString, usage)
  }

  /**
   * epnnrt job metadata--output
   */
  @transient lazy val outputMetadata: Metadata = {
    val usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("epnnrt.upstream.epn"))
    Metadata(params.outputWorkDir, ChannelType.EPN.toString, usage)
  }

  /**
    * epnnrt job metrics
    */
  @transient lazy val metrics: SherlockioMetrics = {
    SherlockioMetrics.init(properties.getProperty("sherlockio.namespace"),properties.getProperty("sherlockio.endpoint"),properties.getProperty("sherlockio.user"))
    val sherlockioMetrics = SherlockioMetrics.getInstance()
    sherlockioMetrics.setJobName(params.appName)
    sherlockioMetrics
  }

  def deleteMetaTmpDir(tmpDir: String): Unit = {
    val tmpPath = new Path(tmpDir)
    if (fs.exists(tmpPath)) {
      fs.delete(tmpPath, true)
    }
    fs.mkdirs(tmpPath)
  }

  def renameMeta(srcTmpDir: String, destDir: String): Unit = {
    val tmpPath = new Path(srcTmpDir)
    if (fs.exists(tmpPath)) {
      val outputStatus = fs.listStatus(tmpPath)
      if (outputStatus.nonEmpty) {
        outputStatus.map(status => {
          val srcFile = status.getPath
          val destFile = new Path(destDir + status.getPath.getName)
          fs.rename(srcFile, destFile)
        })
      }
    }
  }

  def retry[T](n: Int)(fn: => T): T = {
    try {
      fn
    } catch {
      case e =>
        if (n > 1) retry(n - 1)(fn)
        else throw e
    }
  }

  def renameFile(outputDir: String, sparkDir: String, date: String, prefix: String) = {
    // rename result to output dir
    val dateOutputPath = new Path(outputDir + "/date=" + date)
    var max = -1
    if (fs.exists(dateOutputPath)) {
      val outputStatus = fs.listStatus(dateOutputPath)
      if (outputStatus.nonEmpty) {
        max = outputStatus.map(status => {
          val name = status.getPath.getName
          val number = name.substring(name.lastIndexOf("_"))
          Integer.valueOf(number.substring(1, number.indexOf(".")))
        }).sortBy(i => i).last
      }
    } else {
      fs.mkdirs(dateOutputPath)
    }

    val fileStatus = fs.listStatus(new Path(sparkDir))
    val files = fileStatus.filter(status => status.isFile && status.getPath.getName != "_SUCCESS")
      .zipWithIndex
      .map(swi => {
        val src = swi._1.getPath
        val seq = ("%5d" format max + 1 + swi._2).replace(" ", "0")
        val target = new Path(dateOutputPath, prefix +
          date.replaceAll("-", "") + "_" + sc.applicationId + "_" + seq + ".dat.gz")
        fs.rename(src, target)
        target.toString
      })
    files
  }

  def getDate(date: String): String = {
    val splitted = date.split("=")
    if (splitted != null && splitted.nonEmpty) splitted(1)
    else throw new Exception("Invalid date field in metafile.")
  }

  def getTimeStamp(date: String): Long = {
    try {
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      sdf.parse(date).getTime
    } catch {
      case e: Exception => {
        logger.error("Error while parsing timestamp " + e)
        0L
      }
    }
  }
}

