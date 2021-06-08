package com.ebay.traffic.chocolate.sparknrt.epnnrt_v2


import com.ebay.traffic.chocolate.sparknrt.couchbase.CorpCouchbaseClient
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles}
import com.ebay.traffic.monitoring.Field
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.col

object EpnNrtImpressionJob_v2 extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter_v2(args)

    val job = new EpnNrtImpressionJob_v2(params)

    job.run()
    job.stop()
  }
}
class EpnNrtImpressionJob_v2(params: Parameter_v2) extends BaseEpnNrtJob_v2(params, params.appName, params.mode) {

  // meta tmp dir
  lazy val epnNrtResultMetaImpTempDir = outputDir + "/tmp_result_meta_imp/"
  lazy val epnNrtScpMetaImpTempDir = outputDir + "/tmp_scp_meta_imp/"

  // meta final dir
  lazy val epnNrtResultMetaImpDir = outputWorkDir + "/meta/EPN/output/epnnrt_imp/"
  lazy val epnNrtScpMetaImpDir = outputWorkDir + "/meta/EPN/output/epnnrt_scp_imp/"

  lazy val IMPRESSION_DIR = "/impression/"


  @transient lazy val batchSize: Int = {
    val batchSize = properties.getProperty("epnnrt.impression.metafile.batchsize")
    if (StringUtils.isNumeric(batchSize)) {
      Integer.parseInt(batchSize)
    } else {
      10 // default to 10 metafiles
    }
  }

  override def run(): Unit = {
    //1. load meta files
    logger.info("load metadata...")

    var cappingMeta = inputMetadata.readDedupeOutputMeta(".epnnrtimp_v2")

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
          if (df.rdd.isEmpty)
            break
          df = df.repartition(properties.getProperty("epnnrt.impression.repartition").toInt)
          val epnNrtCommon = new EpnNrtCommon_v2(params, df)
          logger.info("load DataFrame, date=" + date + ", with files=" + datesFile._2.mkString(","))

          timestamp = df.first().getAs[Long]("timestamp")

          logger.info("Processing " + size + " datesFile in metaFile " + file)
          metrics.meter("DateFileCount", size,  Field.of[String, AnyRef]("channelAction", "IMPRESSION"))
          metrics.meter("InComingCount", df.count(),  Field.of[String, AnyRef]("channelAction", "IMPRESSION"))

          // filter publisher 5574651234
          df = df.withColumn("publisher_filter", epnNrtCommon.filter_specific_pub_udf(col("referer"), col("publisher_id")))
          df = df.filter(col("publisher_filter") === "0")

          // filter uri && referer are ebay sites (long term traffic from ebay sites)
          df = df.filter(epnNrtCommon.filter_longterm_ebaysites_ref_udf(col("uri"), col("referer")))

          // filter impression data, and if there is filterTime, filter the data older than filter time
          var df_impression = df.filter(col("channel_action") === "IMPRESSION")

          val debug = properties.getProperty("epnnrt.debug").toBoolean

          var df_impression_count_before_filter = 0L

          if (debug) {
            df_impression_count_before_filter = df_impression.count()
          }

          logger.info("Current filter timestamp is: " + params.filterTime)
          var filtered = false
          if (!params.filterTime.equalsIgnoreCase("") && !params.filterTime.equalsIgnoreCase("0"))
            filtered = true

          if (filtered) {
            try {
              df_impression = df_impression.filter( r=> {
                r.getAs[Long]("timestamp") >= params.filterTime.toLong
              })
            } catch {
              case e: Exception =>
                logger.error("Illegal filter timestamp: " + params.filterTime + e)
            }
          }

          var df_impression_count_after_filter = 0L
          if (debug) {
            df_impression_count_after_filter = df_impression.count()
            metrics.meter("ImpressionFilterCount", df_impression_count_before_filter - df_impression_count_after_filter)
          }

          //3. build impression dataframe  save dataframe to files and rename files
          var impressionDf = new ImpressionDataFrame_v2(df_impression, epnNrtCommon).build()
          impressionDf = impressionDf.repartition(params.partitions)
          saveDFToFiles(impressionDf, epnNrtTempDir + IMPRESSION_DIR)

          val countImpDf = readFilesAsDF(epnNrtTempDir + IMPRESSION_DIR)

          metrics.meter("SuccessfulCount", countImpDf.count(), Field.of[String, AnyRef]("channelAction", "IMPRESSION"))

          //write to EPN NRT output meta files
          val imp_files = renameFile(outputDir + IMPRESSION_DIR, epnNrtTempDir + IMPRESSION_DIR, date, "dw_ams.ams_imprsn_cntnr_cs_")
          val imp_metaFile = new MetaFiles(Array(DateFiles(date, imp_files)))

          retry(3) {
            deleteMetaTmpDir(epnNrtResultMetaImpTempDir)
            outputMetadata.writeOutputMeta(imp_metaFile, epnNrtResultMetaImpTempDir, "epnnrt_imp", Array(".epnnrt"))
            deleteMetaTmpDir(epnNrtScpMetaImpTempDir)
            outputMetadata.writeOutputMeta(imp_metaFile, epnNrtScpMetaImpTempDir, "epnnrt_scp_imp", Array(".epnnrt_etl", ".epnnrt_reno", ".epnnrt_hercules"))
            logger.info("successfully write EPN NRT impression output meta to HDFS")
            metrics.meter("OutputMetaSuccessful", params.partitions, Field.of[String, AnyRef]("channelAction", "IMPRESSION"))
          }

          //rename meta files
          renameMeta(epnNrtResultMetaImpTempDir, epnNrtResultMetaImpDir)
          renameMeta(epnNrtScpMetaImpTempDir, epnNrtScpMetaImpDir)
        }
      })

      // 6. archive the meta file
      logger.info(s"archive metafile=$file")
      archiveMetafile(file, archiveDir)

      // 7.delete the finished meta files
      logger.info(s"delete metafile=$file")
      inputMetadata.deleteDedupeOutputMeta(file)

      logger.info("Successfully processed the meta file: + " + file)
      metrics.meter("MetaFileCount", 1,  Field.of[String, AnyRef]("channelAction", "IMPRESSION"))

      if (metrics != null)
        metrics.flush()
    })
  }
}
