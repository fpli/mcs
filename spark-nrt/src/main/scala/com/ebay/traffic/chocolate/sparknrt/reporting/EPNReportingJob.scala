package com.ebay.traffic.chocolate.sparknrt.reporting

import java.text.SimpleDateFormat

import com.ebay.traffic.monitoring.{ESReporting, Field, Reporting}
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.imkDump.TableSchema
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import collection.JavaConverters._

object EPNReportingJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = EPNParameter(args)

    val job = new EPNReportingJob(params)

    job.run()
    job.stop()
  }
}

class EPNReportingJob (params: EPNParameter)
  extends BaseSparkNrtJob(params.appName, params.mode) {

  @transient lazy val esReporting: Reporting = {
    if (params.elasticsearchUrl != null && !params.elasticsearchUrl.isEmpty) {
      ESReporting.init("chocolate-report-", params.elasticsearchUrl)
      ESReporting.getInstance()
    } else {
      throw new Exception("no es url")
    }
  }
  @transient lazy val schema_epn_click = TableSchema("df_epn_click.json")
  @transient lazy val schema_epn_impression = TableSchema("df_epn_impression.json")
  @transient lazy val schema_reporting = TableSchema("df_reporting.json")

  lazy val archiveDir: String = params.archiveDir + "/EPN/reporting/"

  //import spark.implicits._

  override def run(): Unit = {
    if (params.action.equals("click")) {
      logger.info("start generate report for EPN click")
      val clickMetadata: Metadata = Metadata(params.workDir, "EPN", MetadataEnum.epnnrt_click)
      generateReportForEPN(clickMetadata, "click")
      logger.info("finish generate report for EPN click")
    } else {
      logger.info("start generate report for EPN impression")
      val impressionMetadata: Metadata = Metadata(params.workDir, "EPN", MetadataEnum.epnnrt_imp)
      generateReportForEPN(impressionMetadata, "impression")
      logger.info("finish generate report for EPN impression")
    }
  }

  def generateReportForEPN(metaData: Metadata, action: String): Unit = {
    logger.info("load metadata...")
    var dedupeOutputMeta = metaData.readDedupeOutputMeta()
    if (dedupeOutputMeta.length > params.batchSize) {
      dedupeOutputMeta = dedupeOutputMeta.slice(0, params.batchSize)
    }

    dedupeOutputMeta.foreach(metaIter => {
      val file = metaIter._1
      val datesFiles = metaIter._2
      datesFiles.foreach(datesFile => {
        val dataFiles = datesFile._2
        val absoluteDataFiles = dataFiles.map(file => {
          if(file.startsWith("hdfs")){
            file
          } else {
            params.hdfsUri + file
          }
        })
        val df = {
          if (action.equals("click")) {
            getClickDf(absoluteDataFiles)
          } else {
            getImpressionDf(absoluteDataFiles)
          }
        }
        val resultDf = df.groupBy("event_dt", "is_mob", "is_filtered", "publisher_id", "campaign_id")
          .agg(count("id").alias("count"), min("timestamp").alias("timestamp"))

        resultDf.foreachPartition(iter => {
          while (iter.hasNext) {
            val row = iter.next()
            upsertElasticSearch(row)
          }
        })
      })
      // 5. archive metafile that is processed for replay
      logger.info(s"archive metafile=$file")
      archiveMetafile(file, archiveDir)
    })
  }

  val isMobUdf: UserDefinedFunction = udf((browserName: String) => StringUtils.isNotEmpty(browserName) && browserName.contains("Mobi"))
  val isFilteredUdf: UserDefinedFunction = udf((fltrYnInd: String) => StringUtils.isNotEmpty(fltrYnInd) && !fltrYnInd.equals("0"))
  val getDtFromTsUdf: UserDefinedFunction = udf((timestamp: String) => timestamp.substring(0, 10))
  val getLongTsFromStringUdf: UserDefinedFunction = udf((ts: String) => getTimestampFromTS(ts))

  /**
    * get click dataframe
    * @param dataFiles data files path from meta file
    * @return
    */
  def getClickDf(dataFiles: Array[String]): DataFrame = {
    val df = readFilesAsDFEx(dataFiles, schema_epn_click.dfSchema, "csv", "tab")

    df.withColumn("id", col("CLICK_ID"))
      .withColumn("event_dt", getDtFromTsUdf(col("CLICK_TS")))
      .withColumn("timestamp", getLongTsFromStringUdf(col("CLICK_TS")))
      .withColumn("is_mob", isMobUdf(col("BRWSR_NAME")))
      .withColumn("is_filtered", isFilteredUdf(col("FLTR_YN_IND")))
      .withColumn("publisher_id", col("PBLSHR_ID"))
      .withColumn("campaign_id", col("AMS_PBLSHR_CMPGN_ID"))
      .withColumn("rotation_id", col("PLCMNT_DATA_TXT"))
      .withColumn("channel", col("EPN"))
      .withColumn("channel_action", lit("click"))
      .select(schema_reporting.dfColumns: _*)
  }

  /**
    * get impression dataframe
    * @param dataFiles data files path from meta file
    * @return
    */
  def getImpressionDf(dataFiles: Array[String]): DataFrame = {
    val df = readFilesAsDFEx(dataFiles, schema_epn_impression.dfSchema, "csv", "tab")

    df.withColumn("id", col("IMPRSN_CNTNR_ID"))
      .withColumn("event_dt", getDtFromTsUdf(col("IMPRSN_TS")))
      .withColumn("timestamp", getLongTsFromStringUdf(col("IMPRSN_TS")))
      .withColumn("is_mob", isMobUdf(col("BRWSR_NAME")))
      .withColumn("is_filtered", isFilteredUdf(col("FILTER_YN_IND")))
      .withColumn("publisher_id", col("PBLSHR_ID"))
      .withColumn("campaign_id", col("AMS_PBLSHR_CMPGN_ID"))
      .withColumn("rotation_id", col("PLCMNT_DATA_TXT"))
      .withColumn("channel", col("EPN"))
      .withColumn("channel_action", lit("impression"))
      .select(schema_reporting.dfColumns: _*)
  }

  /**
    * upsert record to ES
    * @param row record
    */
  def upsertElasticSearch(row: Row): Unit = {
    retry(3) {
      val docId = row.mkString("-")

      val fieldsMap = row.getValuesMap(row.schema.fieldNames)
      var fields = new Array[Field[String, AnyRef]](0)

      fieldsMap.foreach(field => {
        fields = fields :+ Field.of[String, AnyRef](field._1, field._2)
      })

      esReporting.send("CHOCOLATE_REPORT", row.getAs("count").toString.toLong, docId,
        row.getAs("timestamp").toString.toLong, fields:_*
      )
    }
  }

  val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
  /**
    * get long timestamp from timestamp string
    * @param ts sring ts
    * @return
    */
  def getTimestampFromTS(ts: String): Long = {
    format.parse(ts).getTime
  }

  def retry[T](n: Int)(fn: => T): T = {
    try {
      fn
    } catch {
      case e:Exception =>
        if (n > 1) retry(n - 1)(fn)
        else throw e
    }
  }
}
