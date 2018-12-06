package com.ebay.traffic.chocolate.sparknrt.epnnrt

import java.util.Properties

import com.ebay.app.raptor.chocolate.avro.ChannelType
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path

object EpnNrtJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new EpnNrtJob(params)

    job.run()
    job.stop()
  }
}

class EpnNrtJob(params: Parameter) extends BaseSparkNrtJob(params.appName, params.mode) {

  lazy val outputDir = properties.getProperty("epnnrt.outputdir")

  lazy val epnNrtTempDir = outputDir + "/tmp/"

  @transient lazy val properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("epnnrt.properties"))
    properties
  }

  @transient lazy val metadata: Metadata = {
    val usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("epnnrt.upstream.epn"))
    Metadata(params.workDir, ChannelType.EPN.toString, usage)
  }

  @transient lazy val batchSize: Int = {
    val batchSize = properties.getProperty("epnnrt.metafile.batchsize")
    if (StringUtils.isNumeric(batchSize)) {
      Integer.parseInt(batchSize)
    } else {
      10 // default to 10 metafiles
    }
  }

  @transient lazy val epnNrtCommon: EpnNrtCommon = {
    val common = new EpnNrtCommon(params)
    common
  }

  override def run(): Unit = {
    //1. load meta files
    logger.info("load metadata...")

    var cappingMeta = metadata.readDedupeOutputMeta(".epnnrt")

    if (cappingMeta.length > batchSize) {
      cappingMeta = cappingMeta.slice(0, batchSize)
    }

    cappingMeta.foreach(metaIter => {
      val file = metaIter._1
      val datesFiles = metaIter._2
      datesFiles.foreach(datesFile => {
        //2. load DataFrame
        val date = epnNrtCommon.getDate(datesFile._1)
        val df = readFilesAsDFEx(datesFile._2)
        logger.info("load DataFrame, date=" + date + ", with files=" + datesFile._2.mkString(","))

        println("load DataFrame, date=" + date + ", with files=" + datesFile._2.mkString(","))

        //3. build impression dataframe  save dataframe to files and rename files
        val impressionDf = new ImpressionDataFrame(df, epnNrtCommon).build()
        saveDFToFiles(impressionDf, epnNrtTempDir + "/impression/", "gzip", "csv", "tab")
        renameFile(outputDir + "/impression/", epnNrtTempDir + "/impression/", date, "dw_ams.ams_imprsn_cntnr_cs_")

        //4. build click dataframe  save dataframe to files and rename files
        val clickDf = new ClickDataFrame(df, epnNrtCommon).build()
        saveDFToFiles(clickDf, epnNrtTempDir + "/click/", "gzip", "csv", "tab")
        renameFile(outputDir + "/click/", epnNrtTempDir + "/click/", date, "dw_ams.ams_clicks_cs_")

        // 5.delete the finished meta files
        metadata.deleteDedupeOutputMeta(file)
      })
    })
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
          Integer.valueOf(name.substring(name.lastIndexOf("-") + 1).substring(0, name.indexOf(".")))
        }).sortBy(i => i).last
      }
    } else {
      fs.mkdirs(dateOutputPath)
    }

    val fileStatus = fs.listStatus(new Path(sparkDir))
    val files = fileStatus.filter(status => status.getPath.getName != "_SUCCESS")
      .zipWithIndex
      .map(swi => {
        val src = swi._1.getPath
        val seq = swi._2
        val target = new Path(dateOutputPath, prefix +
          date.replaceAll("-", "") + "_" + sc.applicationId + "_" + seq + ".dat.gz")
        fs.rename(src, target)
        target.toString
      })
    files
  }
}