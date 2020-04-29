package com.ebay.traffic.chocolate.sparknrt.epnnrt

import java.text.SimpleDateFormat
import java.util.Properties

import com.ebay.app.raptor.chocolate.avro.ChannelType
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.couchbase.CorpCouchbaseClient
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import com.ebay.traffic.chocolate.sparknrt.utils.TableSchema
import com.ebay.traffic.monitoring.{ESMetrics, Field, Metrics}
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

 lazy val outputDir = params.outputDir
  lazy val workDir = params.workDir
  lazy val epnNrtTempDir = outputDir + "/tmp/"
  // meta tmp dir
  lazy val epnNrtResultMetaClickTempDir = outputDir + "/tmp_result_meta_click/"
  lazy val epnNrtScpMetaClickTempDir = outputDir + "/tmp_scp_meta_click/"
  lazy val epnNrtResultMetaImpTempDir = outputDir + "/tmp_result_meta_imp/"
  lazy val epnNrtScpMetaImpTempDir = outputDir + "/tmp_scp_meta_imp/"

  //meta final dir
  lazy val epnNrtResultMetaClickDir = workDir + "/meta/EPN/output/epnnrt_click/"
  lazy val epnNrtScpMetaClickDir = workDir + "/meta/EPN/output/epnnrt_scp_click/"
  lazy val epnNrtResultMetaImpDir = workDir + "/meta/EPN/output/epnnrt_imp/"
  lazy val epnNrtScpMetaImpDir = workDir + "/meta/EPN/output/epnnrt_scp_imp/"

  lazy val METRICS_INDEX_PREFIX = "chocolate-metrics-"
  lazy val archiveDir = workDir + "/meta/EPN/output/archive/"


  @transient lazy val schema_epn_click_table = TableSchema("df_epn_click.json")

  @transient lazy val schema_epn_impression_table = TableSchema("df_epn_impression.json")

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

  @transient lazy val metrics: Metrics = {
    val url  = properties.getProperty("epnnrt.elasticsearchUrl")
    if (url != null && url.nonEmpty) {
      ESMetrics.init(METRICS_INDEX_PREFIX, url)
      ESMetrics.getInstance()
    } else null
  }

  override def run(): Unit = {
    //1. load meta files
    logger.info("load metadata...")

    var cappingMeta = metadata.readDedupeOutputMeta(".epnnrt")

    if (cappingMeta.length > batchSize) {
      cappingMeta = cappingMeta.slice(0, batchSize)
    }

    //init couchbase datasource
    CorpCouchbaseClient.dataSource = properties.getProperty("epnnrt.datasource")

    var timestamp = -1L

    cappingMeta.foreach(metaIter => {
      val file = metaIter._1
      val datesFiles = metaIter._2
      datesFiles.foreach(datesFile => {
        import util.control.Breaks._
        breakable {
          //2. load DataFrame
          val date = getDate(datesFile._1)
          var df = readFilesAsDFEx(datesFile._2)
          val size = datesFile._2.length
          //if the dataframe is empty, just continue
          if (df.take(1).isEmpty)
            break
          // do not repartition to improve read performance. no need to shuffle here. if input is too large, it reduces the performance.
          //df = df.repartition(properties.getProperty("epnnrt.repartition").toInt)
          val epnNrtCommon = new EpnNrtCommon(params, df)
          logger.info("load DataFrame, date=" + date + ", with files=" + datesFile._2.mkString(","))

          timestamp = df.first().getAs[Long]("timestamp")

          logger.info("Processing " + size + " datesFile in metaFile " + file)
          metrics.meter("DateFileCount", size, timestamp)
          metrics.meter("InComingCount", df.count(), timestamp)

          // filter publisher 5574651234
          df = df.withColumn("publisher_filter", epnNrtCommon.filter_specific_pub_udf(col("referer"), col("publisher_id")))
          df = df.filter(col("publisher_filter") === "0")

          // filter uri && referer are ebay sites (long term traffic from ebay sites)
          df = df.filter(epnNrtCommon.filter_longterm_ebaysites_ref_udf(col("uri"), col("referer")))

          // filter click and impression data, and if there is filterTime, filter the data older than filter time
          // repartition click only to increase the parallism
          var df_click = df.filter(col("channel_action") === "CLICK")
            .repartition(properties.getProperty("epnnrt.repartition").toInt)
            .cache()
          // no need repartition on impression as the logic in impression is just simply save them
          var df_impression = df.filter(col("channel_action") === "IMPRESSION")

          val debug = properties.getProperty("epnnrt.debug").toBoolean

          var df_click_count_before_filter = 0L
          var df_impression_count_before_filter = 0L

          if (debug) {
            df_click_count_before_filter = df_click.count()
            df_impression_count_before_filter = df_impression.count()
          }

          logger.info("Current filter timestamp is: " + params.filterTime)
          var filtered = false
          if (!params.filterTime.equalsIgnoreCase("") && !params.filterTime.equalsIgnoreCase("0"))
            filtered = true

          if (filtered) {
            try {
              df_click = df_click.filter( r=> {
                r.getAs[Long]("timestamp") >= params.filterTime.toLong
              })
              df_impression = df_impression.filter( r=> {
                r.getAs[Long]("timestamp") >= params.filterTime.toLong
              })
            } catch {
              case e: Exception =>
                logger.error("Illegal filter timestamp: " + params.filterTime + e)
            }
          }


          var df_click_count_after_filter = 0L
          var df_impression_count_after_filter = 0L
          if (debug) {
            df_click_count_after_filter = df_click.count()
            df_impression_count_after_filter = df_impression.count()
            metrics.meter("ClickFilterCount", df_click_count_before_filter - df_click_count_after_filter)
            metrics.meter("ImpressionFilterCount", df_impression_count_before_filter - df_impression_count_after_filter)
          }

          //3. build impression dataframe  save dataframe to files and rename files
          var impressionDf = new ImpressionDataFrame(df_impression, epnNrtCommon).build()
          impressionDf = impressionDf.repartition(params.partitions)
           saveDFToFiles(impressionDf, epnNrtTempDir + "/impression/", "gzip", "csv", "tab")

          val countImpDf = readFilesAsDF(epnNrtTempDir + "/impression/", schema_epn_impression_table.dfSchema, "csv", "tab", false)

          metrics.meter("SuccessfulCount", countImpDf.count(), timestamp, Field.of[String, AnyRef]("channelAction", "IMPRESSION"))

          //write to EPN NRT output meta files
          val imp_files = renameFile(outputDir + "/impression/", epnNrtTempDir + "/impression/", date, "dw_ams.ams_imprsn_cntnr_cs_")
          val imp_metaFile = new MetaFiles(Array(DateFiles(date, imp_files)))

          retry(3) {
            deleteMetaTmpDir(epnNrtResultMetaImpTempDir)
            metadata.writeOutputMeta(imp_metaFile, epnNrtResultMetaImpTempDir, "epnnrt_imp", Array(".epnnrt"))
            deleteMetaTmpDir(epnNrtScpMetaImpTempDir)
            metadata.writeOutputMeta(imp_metaFile, epnNrtScpMetaImpTempDir, "epnnrt_scp_imp", Array(".epnnrt_etl", ".epnnrt_reno", ".epnnrt_hercules"))
            logger.info("successfully write EPN NRT impression output meta to HDFS")
            metrics.meter("OutputMetaSuccessful", params.partitions, Field.of[String, AnyRef]("channelAction", "IMPRESSION"))
          }

         /* try {

           // metadata.writeOutputMeta(imp_metaFile, epnNrtMetaImpTempDir, properties.getProperty("epnnrt.scp.meta.imp.outputdir"),"epnnrt_imp", Array(".epnnrt", ".epnnrt_etl", ".epnnrt_reno", ".epnnrt_hercules"))
          //  metadata.writeOutputMeta(imp_metaFile, epnNrtMetaImpTempDir,)
          //  renameMetaFiles(epnNrtMetaImpTempDir, properties.getProperty("epnnrt.scp.meta.imp.outputdir"), Array(".epnnrt_etl", ".epnnrt_reno", ".epnnrt_hercules"))

            deleteMetaTmpDir(epnNrtResultMetaImpTempDir)
            metadata.writeOutputMeta(imp_metaFile, epnNrtResultMetaImpTempDir, "epnnrt_imp", Array(".epnnrt"))
          //  metadata.writeOutputMeta(imp_metaFile, properties.getProperty("epnnrt.result.meta.imp.outputdir"), "epnnrt_imp", Array(".epnnrt"))
            //write meta file for EPN SCP job to copy result to ETL and reno
            deleteMetaTmpDir(epnNrtScpMetaImpTempDir)
            metadata.writeOutputMeta(imp_metaFile, epnNrtScpMetaImpTempDir, "epnnrt_scp_imp", Array(".epnnrt_etl", ".epnnrt_reno", ".epnnrt_hercules"))
         //   metadata.writeOutputMeta(imp_metaFile, properties.getProperty("epnnrt.scp.meta.imp.outputdir"), "epnnrt_scp_imp", Array(".epnnrt_etl", ".epnnrt_reno", ".epnnrt_hercules"))

            logger.info("successfully write EPN NRT impression output meta to HDFS")
            metrics.meter("OutputMetaSuccessful", params.partitions, Field.of[String, AnyRef]("channelAction", "IMPRESSION"))

          } catch {
            case e: Exception => {
              logger.error("Error while writing EPN NRT impression output meta files" + e)
            }
          }*/

          //4. build click dataframe  save dataframe to files and rename files
          var clickDf = new ClickDataFrame(df_click, epnNrtCommon).build()
          clickDf = clickDf.repartition(params.partitions)

          saveDFToFiles(clickDf, epnNrtTempDir + "/click/", "gzip", "csv", "tab")

          val countClickDf = readFilesAsDF(epnNrtTempDir + "/click/", schema_epn_click_table.dfSchema, "csv", "tab", false)

          metrics.meter("SuccessfulCount", countClickDf.count(), timestamp, Field.of[String, AnyRef]("channelAction", "CLICK"))

          val clickFiles = renameFile(outputDir + "/click/", epnNrtTempDir + "/click/", date, "dw_ams.ams_clicks_cs_")


          //5. write the epn-nrt meta output file to hdfs
          val click_metaFile = new MetaFiles(Array(DateFiles(date, clickFiles)))

          retry(3) {
            deleteMetaTmpDir(epnNrtResultMetaClickTempDir)
            metadata.writeOutputMeta(click_metaFile, epnNrtResultMetaClickTempDir, "epnnrt_click", Array(".epnnrt_1", ".epnnrt_2"))
            deleteMetaTmpDir(epnNrtScpMetaClickTempDir)
            metadata.writeOutputMeta(click_metaFile, epnNrtScpMetaClickTempDir, "epnnrt_scp_click", Array(".epnnrt_etl", ".epnnrt_reno", ".epnnrt_hercules"))
            metrics.meter("OutputMetaSuccessful", params.partitions * 2, Field.of[String, AnyRef]("channelAction", "CLICK"))
            logger.info("successfully write EPN NRT Click output meta to HDFS, job finished")
          }
          /*try {
            deleteMetaTmpDir(epnNrtResultMetaClickTempDir)
            metadata.writeOutputMeta(click_metaFile, epnNrtResultMetaClickTempDir, "epnnrt_click", Array(".epnnrt_1", ".epnnrt_2"))
          //  metadata.writeOutputMeta(click_metaFile, properties.getProperty("epnnrt.result.meta.click.outputdir"), "epnnrt_click", Array(".epnnrt_1", ".epnnrt_2"))
            //write meta file for EPN SCP job to copy result to ETL and reno
            deleteMetaTmpDir(epnNrtScpMetaClickTempDir)
            metadata.writeOutputMeta(click_metaFile, epnNrtScpMetaClickTempDir, "epnnrt_scp_click", Array(".epnnrt_etl", ".epnnrt_reno", ".epnnrt_hercules"))
           // metadata.writeOutputMeta(click_metaFile, properties.getProperty("epnnrt.scp.meta.click.outputdir"), "epnnrt_scp_click", Array(".epnnrt_etl", ".epnnrt_reno", ".epnnrt_hercules"))
            metrics.meter("OutputMetaSuccessful", params.partitions * 2, Field.of[String, AnyRef]("channelAction", "CLICK"))
            logger.info("successfully write EPN NRT Click output meta to HDFS, job finished")
          } catch {
            case e: Exception => {
              logger.error("Error while writing EPN NRT Click output meta files" + e)
            }
          }*/
          //rename meta files
          renameMeta(epnNrtResultMetaImpTempDir, epnNrtResultMetaImpDir)
          renameMeta(epnNrtScpMetaImpTempDir, epnNrtScpMetaImpDir)
          renameMeta(epnNrtResultMetaClickTempDir, epnNrtResultMetaClickDir)
          renameMeta(epnNrtScpMetaClickTempDir, epnNrtScpMetaClickDir)
        }
      })

      // 6. archive the meta file
      logger.info(s"archive metafile=$file")
      archiveMetafile(file, archiveDir)

      // 7.delete the finished meta files
      logger.info(s"delete metafile=$file")
      metadata.deleteDedupeOutputMeta(file)

      logger.info("Successfully processed the meta file: + " + file)
      metrics.meter("MetaFileCount", 1, timestamp)

      if (metrics != null)
        metrics.flush()
    })
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