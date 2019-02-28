package com.ebay.traffic.chocolate.sparknrt.epnnrt

import java.util.Properties

import com.ebay.app.raptor.chocolate.avro.ChannelType
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.col

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
        val date = getDate(datesFile._1)
        val df = readFilesAsDFEx(datesFile._2)
        val epnNrtCommon = new EpnNrtCommon(params, df)
        logger.info("load DataFrame, date=" + date + ", with files=" + datesFile._2.mkString(","))

        val df_click = df.filter(col("channel_action") === "CLICK")
        val df_impression = df.filter(col("channel_action") === "IMPRESSION")

        //3. build impression dataframe  save dataframe to files and rename files
        var impressionDf = new ImpressionDataFrame(df_impression, epnNrtCommon).build()
        impressionDf = impressionDf.repartition(params.partitions)
        saveDFToFiles(impressionDf, epnNrtTempDir + "/impression/", "gzip", "csv", "tab")
        renameFile(outputDir + "/impression/", epnNrtTempDir + "/impression/", date, "dw_ams.ams_imprsn_cntnr_cs_")

        //4. build click dataframe  save dataframe to files and rename files
        var clickDf = new ClickDataFrame(df_click, epnNrtCommon).build()
        clickDf = clickDf.repartition(params.partitions)
        saveDFToFiles(clickDf, epnNrtTempDir + "/click/", "gzip", "csv", "tab")
      //  renameFile(outputDir + "/click/", epnNrtTempDir + "/click/", date, "dw_ams.ams_clicks_cs_")

        val files = renameFile(outputDir + "/click/", epnNrtTempDir + "/click/", date, "dw_ams.ams_clicks_cs_")

        // 5.delete the finished meta files
        metadata.deleteDedupeOutputMeta(file)

        //6. write the epn-nrt meta output file to hdfs
        val metaFile = new MetaFiles(Array(DateFiles(date, files)))
        metadata.writeOutputMeta(metaFile, properties.getProperty("epnnrt.result.meta.outputdir"), Array(".epnnrt"))
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
          val number = name.substring(name.lastIndexOf("_"))
          Integer.valueOf(number.substring(1, number.indexOf(".")))
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
}