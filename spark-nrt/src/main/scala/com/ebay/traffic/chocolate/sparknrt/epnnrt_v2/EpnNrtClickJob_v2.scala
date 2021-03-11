package com.ebay.traffic.chocolate.sparknrt.epnnrt_v2

import com.ebay.traffic.chocolate.sparknrt.couchbase_v2.CorpCouchbaseClient_v2
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles}
import com.ebay.traffic.chocolate.sparknrt.utils.TableSchema
import com.ebay.traffic.monitoring.Field
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.col

object EpnNrtClickJob_v2 extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter_v2(args)

    val job = new EpnNrtClickJob_v2(params)

    job.run()
    job.stop()
  }
}
class EpnNrtClickJob_v2(params: Parameter_v2) extends BaseEpnNrtJob_v2(params, params.appName, params.mode) {

  // meta tmp dir
  lazy val epnNrtResultMetaClickTempDir = outputDir + "/tmp_result_meta_click/"
  lazy val epnNrtScpMetaClickTempDir = outputDir + "/tmp_scp_meta_click/"

  //meta final dir
  lazy val epnNrtResultMetaClickDir = workDir + "/meta/EPN/output/epnnrt_click/"
  lazy val epnNrtScpMetaClickDir = workDir + "/meta/EPN/output/epnnrt_scp_click/"

  lazy val clickDir = "/click/"

  @transient lazy val schema_epn_click_table = TableSchema("df_epn_click.json")

  @transient lazy val batchSize: Int = {
    val batchSize = properties.getProperty("epnnrt.click.metafile.batchsize")
    if (StringUtils.isNumeric(batchSize)) {
      Integer.parseInt(batchSize)
    } else {
      10 // default to 10 metafiles
    }
  }

  override def run(): Unit = {
    //1. load meta files
    logger.info("load metadata...")

    var cappingMeta = metadata.readDedupeOutputMeta(".epnnrt_v2")

    if (cappingMeta.length > batchSize) {
      cappingMeta = cappingMeta.slice(0, batchSize)
    }

    //init couchbase datasource
    CorpCouchbaseClient_v2.dataSource = properties.getProperty("epnnrt.datasource")

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
          df = df.repartition(properties.getProperty("epnnrt.click.repartition").toInt)
          val epnNrtCommon = new EpnNrtCommon_v2(params, df)
          logger.info("load DataFrame, date=" + date + ", with files=" + datesFile._2.mkString(","))

          timestamp = df.first().getAs[Long]("timestamp")

          logger.info("Processing " + size + " datesFile in metaFile " + file)
          metrics.meter("DateFileCount", size,  Field.of[String, AnyRef]("channelAction", "CLICK"))
          metrics.meter("InComingCount", df.count(),  Field.of[String, AnyRef]("channelAction", "CLICK"))

          // filter publisher 5574651234
          df = df.withColumn("publisher_filter", epnNrtCommon.filter_specific_pub_udf(col("referer"), col("publisher_id")))
          df = df.filter(col("publisher_filter") === "0")

          // filter uri && referer are ebay sites (long term traffic from ebay sites)
          df = df.filter(epnNrtCommon.filter_longterm_ebaysites_ref_udf(col("uri"), col("referer")))

          // filter click data, and if there is filterTime, filter the data older than filter time
          var df_click = df.filter(col("channel_action") === "CLICK")

          val debug = properties.getProperty("epnnrt.debug").toBoolean

          var df_click_count_before_filter = 0L

          if (debug) {
            df_click_count_before_filter = df_click.count()
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
            } catch {
              case e: Exception =>
                logger.error("Illegal filter timestamp: " + params.filterTime + e)
            }
          }

          var df_click_count_after_filter = 0L
          if (debug) {
            df_click_count_after_filter = df_click.count()
            metrics.meter("ClickFilterCount", df_click_count_before_filter - df_click_count_after_filter)
          }

          //3. build click dataframe  save dataframe to files and rename files
          var clickDf = new ClickDataFrame_v2(df_click, epnNrtCommon).build()
          clickDf = clickDf.repartition(params.partitions)

          saveDFToFiles(clickDf, epnNrtTempDir + clickDir, "gzip", "parquet", "tab")

          val countClickDf = readFilesAsDF(epnNrtTempDir + clickDir, schema_epn_click_table.dfSchema, "parquet", "tab", false)

          metrics.meter("SuccessfulCount", countClickDf.count(),  Field.of[String, AnyRef]("channelAction", "CLICK"))

          val clickFiles = renameFile(outputDir + clickDir, epnNrtTempDir + clickDir, date, "dw_ams.ams_clicks_cs_")


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

          //rename meta files
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
      metrics.meter("MetaFileCount", 1,  Field.of[String, AnyRef]("channelAction", "CLICK"))

      if (metrics != null)
        metrics.flush()
    })
  }
}