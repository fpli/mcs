/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.delta.imk

import java.net.URLDecoder
import java.time.{Instant, ZoneId, ZonedDateTime}

import org.apache.spark.sql.{DataFrame, SaveMode}
import com.ebay.traffic.chocolate.sparknrt.delta.{BaseDeltaLakeNrtJob, Parameter}
import com.ebay.traffic.chocolate.sparknrt.imkDump.Tools
import com.ebay.traffic.chocolate.sparknrt.utils.TableSchema
import com.ebay.traffic.monitoring.{ESMetrics, Field, Metrics}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}

import scala.io.Source

/**
  * @author Xiang Li
  * @since 2020/08/18
  * Imk NRT job to extract data from master table and sink into IMK table
  */
object ImkDeltaNrtJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new ImkDeltaNrtJob(params)

    job.run()
    job.stop()
  }
}

/**
  * IMK NRT job
  * @param params input parameters
  * @param enableHiveSupport enable hive support for spark sql table query
  */
class ImkDeltaNrtJob(params: Parameter, override val enableHiveSupport: Boolean = true)
  extends BaseDeltaLakeNrtJob(params, enableHiveSupport) {

  lazy val imkChannels = Set("Paid Search", "Display", "Social Media", "ROI")
  lazy val METRICS_INDEX_PREFIX = "imk-etl-metrics-"

  @transient lazy val metrics: Metrics = {
    if (params.elasticsearchUrl != null && !params.elasticsearchUrl.isEmpty) {
      ESMetrics.init(METRICS_INDEX_PREFIX, params.elasticsearchUrl)
      ESMetrics.getInstance()
    } else null
  }

  @transient lazy val mfe_name_id_map: Map[String, String] = {
    val mapData = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("mfe_name_id_map.txt")).getLines
    mapData.map(line => line.split("\\|")(0) -> line.split("\\|")(1)).toMap
  }

  @transient lazy val schema_apollo: TableSchema = TableSchema("df_imk_apollo.json")

  import spark.implicits._


  val tools: Tools = new Tools(METRICS_INDEX_PREFIX, "")
  val getQueryParamsUdf: UserDefinedFunction = udf((uri: String) => tools.getQueryString(uri))
  val getBatchIdUdf: UserDefinedFunction = udf(() => tools.getBatchId)
  val getCmndTypeUdf: UserDefinedFunction = udf((channelType: String) => tools.getCommandType(channelType))
  val getBrowserTypeUdf: UserDefinedFunction = udf((userAgent: String) => tools.getBrowserType(userAgent))
  val getLandingPageDomainUdf: UserDefinedFunction = udf((uri: String) => tools.getDomain(uri))
  val getUserQueryUdf: UserDefinedFunction = udf((referer: String, query: String) => tools.getUserQuery(referer, query))
  val replaceMkgroupidMktypeUdf: UserDefinedFunction = udf((channelType: String, uri: String) => replaceMkgroupidMktype(channelType, uri))
  val replaceMkgroupidMktypeUdfAndParseMpreFromRoverUdf: UserDefinedFunction = udf((channelType: String, uri: String) => replaceMkgroupidMktypeAndParseMpreFromRover(channelType, uri))
  val getDateTimeUdf: UserDefinedFunction = udf((timestamp: Long) => tools.getDateTimeFromTimestamp(timestamp))
  val getKeywordUdf: UserDefinedFunction = udf((query: String) => tools.getParamFromQuery(query, tools.keywordParams))
  val getDefaultNullNumParamValueFromUrlUdf: UserDefinedFunction = udf((query: String, key: String) => tools.getDefaultNullNumParamValueFromQuery(query, key))
  val getParamFromQueryUdf: UserDefinedFunction = udf((query: String, key: String) => tools.getParamValueFromQuery(query, key))
  val getUserMapIndUdf: UserDefinedFunction = udf((userId: String) => tools.getUserMapInd(userId))
  val getMfeIdUdf: UserDefinedFunction = udf((mfe_name: String) => getMfeIdByMfeName(mfe_name))

  /**
    * get mfe id by mfe name
    * @param mfeName mfe name
    * @return
    */
  def getMfeIdByMfeName(mfeName: String): String = {
    if (StringUtils.isNotEmpty(mfeName)) {
      mfe_name_id_map.getOrElse(mfeName, "-999")
    } else {
      "-999"
    }
  }

  /**
    * set apollo item_id filed by tfs item_id and roi_item_id
    * @param roi_item_id roi_item_id
    * @param item_id item_id
    * @return apollo item_id
    */
  def getApolloItemId(roi_item_id: String, item_id: String): String = {
    if (StringUtils.isNotEmpty(roi_item_id) && StringUtils.isNumeric(roi_item_id) && roi_item_id.toLong != -999) {
      roi_item_id
    } else if (StringUtils.isNotEmpty(item_id) && item_id.length <= 18) {
      item_id
    } else{
      ""
    }
  }

  /**
    * Campaign Manager changes the url template for all PLA accounts, replace adtype=pla and*adgroupid=65058347419* with
    * new parameter mkgroupid={adgroupid} and mktype={adtype}. Tracking’s MCS data pipeline job replace back to adtype
    * and adgroupid and persist into IMK so there wont be impact to downstream like data and science.
    * See <a href="https://jirap.corp.ebay.com/browse/XC-1464">replace landing page url and rvr_url's mktype and mkgroupid</a>
    * @param channelType channel
    * @param uri tracking url
    * @return new tracking url
    */
  def replaceMkgroupidMktype(channelType:String, uri: String): String = {
    var newUri = ""
    if (StringUtils.isNotEmpty(uri)) {
      try {
        newUri = uri.replace("mkgroupid", "adgroupid")
          .replace("mktype", "adtype")
      } catch {
        case e: Exception => {
          if (metrics != null) {
            metrics.meter("imk.dump.malformed", 1, Field.of[String, AnyRef]("channelType", channelType))
          }
          logger.warn("MalformedUrl", e)
        }
      }
    }

    newUri
  }

  /**
    * Parse mpre from if it's rover url
    * @param channelType channel type
    * @param uri uri
    * @return mpre
    */
  def replaceMkgroupidMktypeAndParseMpreFromRover(channelType:String, uri: String): String = {
    var newUri = replaceMkgroupidMktype(channelType, uri)
    // parse mpre if url is rover
    if (newUri.startsWith("http://rover.ebay.com") || newUri.startsWith("https://rover.ebay.com")) {
      val query = tools.getQueryString(newUri)
      val landingPageUrl = tools.getParamValueFromQuery(query, "mpre")
      if (StringUtils.isNotEmpty(landingPageUrl)) {
        try{
          newUri = URLDecoder.decode(landingPageUrl,"UTF-8")
        } catch {
          case e: Exception => {
            if(metrics != null) {
              metrics.meter("imk.dump.error.parseMpreFromRoverError", 1)
            }
            logger.warn("MalformedUrl", e)
          }
        }

      }
    }

    newUri
  }

  val setDefaultValueForDstClientIdUdf: UserDefinedFunction = udf((dstClientId: String) => {
    if (StringUtils.isEmpty(dstClientId)) {
      "0"
    } else {
      dstClientId
    }
  })

  val getClientIdUdf: UserDefinedFunction = udf((channelType: String, tempUriQuery: String, ridParamName: String, uri: String) => {
    channelType match {
      case "ROI" => tools.getClientIdFromRoverUrl(uri)
      case _ => tools.getClientIdFromRotationId(tools.getParamValueFromQuery(tempUriQuery, ridParamName))
    }
  })

  /**
    * Read everything need from the source table
    * @param inputDateTime input date time
    */
  override def readSource(inputDateTime: ZonedDateTime): DataFrame = {
    val fromDateTime = getLastDoneFileDateTimeAndDelay(inputDateTime, deltaDoneFileDir)._1
    val fromDateString = fromDateTime.format(dtFormatter)
    val startTimestamp = fromDateTime.toEpochSecond * 1000
    val sql = "select * from " + inputSource + " where dt >= '" + fromDateString + "' and eventtimestamp >='" + startTimestamp +"'"
    val sourceDf = sqlsc.sql(sql)
    var imkDf = sourceDf
      .filter(col("snapshot_id").isNotNull)
      .filter(col("channel_type").isin(imkChannels))
      .withColumn("temp_uri_query", getQueryParamsUdf(getParamFromQueryUdf(col("applicationpayload"), lit("url_mpre"))))
      .withColumn("batch_id", getBatchIdUdf())
      .withColumn("rvr_id", col("snapshot_id"))
      .withColumn("event_dt", col("dt"))
      .withColumn("rvr_cmnd_type_cd", col("channel_action"))
      .withColumn("rvr_chnl_type_cd", col("channel_type"))
      .withColumn("cguid", getParamFromQueryUdf(col("applicationpayload"), lit("cguid")))
      .withColumn("guid", col("guid"))
      .withColumn("user_id", getParamFromQueryUdf(col("applicationpayload"), lit("u")))
      .withColumn("clnt_remote_ip", getBrowserTypeUdf(getParamFromQueryUdf(col("client_data"), lit("RemoteIP"))))
      .withColumn("brwsr_type_id", getBrowserTypeUdf(getParamFromQueryUdf(col("client_data"), lit("Agent"))))
      .withColumn("brwsr_name", getParamFromQueryUdf(col("client_data"), lit("Agent")))
      .withColumn("rfrr_dmn_name", getLandingPageDomainUdf(col("referer")))
      .withColumn("rfrr_url", col("referer"))
      .withColumn("lndng_page_dmn_name", getLandingPageDomainUdf(getParamFromQueryUdf(col("applicationpayload"), lit("url_mpre"))))
      .withColumn("lndng_page_url", getParamFromQueryUdf(col("applicationpayload"), lit("url_mpre")))
      .withColumn("user_query", getUserQueryUdf(col("referer"), col("temp_uri_query")))
      .withColumn("event_ts", getDateTimeUdf(col("event_timestamp")))
      .withColumn("src_rotation_id", getBrowserTypeUdf(getParamFromQueryUdf(col("applicationpayload"), lit("rotid"))))
      .withColumn("dst_rotation_id", getBrowserTypeUdf(getParamFromQueryUdf(col("applicationpayload"), lit("rotid"))))
      .withColumn("user_map_ind", getParamFromQueryUdf(col("applicationpayload"), lit("u")))
      .withColumn("dst_client_id", setDefaultValueForDstClientIdUdf(getClientIdUdf(col("channel_type"), col("temp_uri_query"), lit("mkrid"), col("uri"))))
      // not in imk will be removed after column selection
      .withColumn("mfe_name", getParamFromQueryUdf(col("temp_uri_query"), lit("crlp")))
      .withColumn("mfe_id", getMfeIdUdf(getParamFromQueryUdf(col("temp_uri_query"), lit("crlp"))))
      .withColumn("keyword", getKeywordUdf(col("temp_uri_query")))
      .withColumn("mt_id", getDefaultNullNumParamValueFromUrlUdf(col("temp_uri_query"), lit("mt_id")))
      .withColumn("crlp", getParamFromQueryUdf(col("temp_uri_query"), lit("crlp")))
      .withColumn("geo_id", getParamFromQueryUdf(col("temp_uri_query"), lit("geo_id")))
      .withColumn("item_id", getParamFromQueryUdf(col("applicationpayload"), lit("itemid")))
      .withColumn("transaction_type", getParamFromQueryUdf(col("applicationpayload"), lit("trans_type")))
      .withColumn("transaction_id", getParamFromQueryUdf(col("applicationpayload"), lit("trans_id")))
      .withColumn("cart_id", getParamFromQueryUdf(col("applicationpayload"), lit("cart_id")))
      .withColumn("ebay_site_id", col("site_id"))
      .withColumn("rvr_url", replaceMkgroupidMktypeUdf(col("channel_type"), getParamFromQueryUdf(col("applicationpayload"), lit("url_mpre"))))
      .na.fill(schema_apollo.defaultValues).cache()

    // set default values for some columns
    schema_apollo.filterNotColumns(imkDf.columns).foreach(e => {
      imkDf = imkDf.withColumn(e, lit(schema_apollo.defaultValues(e)))
    })
    sourceDf
  }

  /**
    * Function to write file to output dir
    * @param df dataframe to write
    * @param dtString date partition
    */
  override def writeToOutput(df: DataFrame, dtString: String): Unit = {
    // save to final output
    this.saveDFToFiles(df, outputPath = outputDir, compressFormat= "gzip", outputFormat = "csv", delimiter = "bel",
      writeMode = SaveMode.Append, partitionColumn = dt)
  }

  /**
    * Entry of this spark job
    */
  override def run(): Unit = {
    val now = ZonedDateTime.now(defaultZoneId)
    updateDelta(now)
    updateOutput(now)
  }
}
