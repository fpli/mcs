/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.traffic.chocolate.sparknrt.delta.imk

import java.net.URLDecoder
import java.time.ZonedDateTime

import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum
import org.apache.spark.sql.{DataFrame, SaveMode}
import com.ebay.traffic.chocolate.sparknrt.delta.{BaseDeltaLakeNrtJob, Parameter}
import com.ebay.traffic.chocolate.sparknrt.imkDump.Tools
import com.ebay.traffic.chocolate.sparknrt.utils.TableSchema
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}

import scala.io.Source

/**
  * @author Xiang Li
  * @since 2020/08/18
  *        Imk NRT job to extract data from master table and sink into IMK table
  * Before first running. Initialize the delta table by the schema.
  * 1. Debug this class ut to generate a parquet with delta table schema
  * 2. Upload this this parquet file to apollo
  * 3. Run spark shell locally to create a delta table
  * 3.1 read df from the parquet
  * 3.2 withColumn("dt", lit(some date))
  * 3.3 df.write.format("delta").mode("overwrite").partitionBy("dt").save("your path")
  * After all above done, we have a complete env to start the job
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
  *
  * @param params            input parameters
  * @param enableHiveSupport enable hive support for spark sql table query
  */
class ImkDeltaNrtJob(params: Parameter, override val enableHiveSupport: Boolean = true)
  extends BaseDeltaLakeNrtJob(params, enableHiveSupport) {

  private lazy val IMK_CHANNELS: Seq[String] = Set("PAID_SEARCH", "DISPLAY", "SOCIAL_MEDIA", "ROI").toSeq
  lazy val METRICS_INDEX_PREFIX = "imk-etl-metrics-"
  lazy val SNAPSHOT_ID = "snapshotid"
  lazy val CHANNEL_TYPE = "channeltype"
  lazy val DECODED_URL = "decoded_url"
  lazy val TEMP_URI_QUERY = "temp_uri_query"
  lazy val DT = "dt"
  lazy val CGUID = "cguid"
  lazy val GUID = "guid"
  lazy val CLIENT_DATA = "clientdata"
  lazy val APPLICATION_PAYLOAD = "applicationpayload"
  lazy val REFERRER = "referrer"
  lazy val AGENT = "Agent"
  lazy val ROTID = "rotid"
  lazy val CRLP = "crlp"
  lazy val MTID = "mt_id"
  lazy val EVENT_TIMESTAMP = "eventtimestamp"
  lazy val GEOID = "geo_id"

  @transient lazy val mfe_name_id_map: Map[String, String] = {
    val mapData = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("mfe_name_id_map.txt")).getLines
    mapData.map(line => line.split("\\|")(0) -> line.split("\\|")(1)).toMap
  }

  /**
    * the delta schema has only one difference, which is the timestamp.
    * In order to work with super class timestamp comparison logic, the timestamp should be epoch millisecond format.
    * At the final stage of this job, the timestamp will be reformated to the final table timestamp format.
    */
  @transient lazy val schema_delta_apollo: TableSchema = TableSchema("df_imk_delta.json")
  @transient lazy val schema_apollo: TableSchema = TableSchema("df_imk_delta_final.json")

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
  val getChannelActionEnumUdf: UserDefinedFunction = udf((channelAction: String) => getChannelActionEnum(channelAction))
  val getChannelTypeEnumUdf: UserDefinedFunction = udf((channelType: String) => getChannelTypeEnum(channelType))
  val decodeUrlUdf: UserDefinedFunction = udf((url: String) => URLDecoder.decode(url, "utf-8"))
  val getPerfTrackNameValueUdf: UserDefinedFunction = udf((query: String) => tools.getPerfTrackNameValue(query))
  /**
    * get mfe id by mfe name
    *
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
    * Campaign Manager changes the url template for all PLA accounts, replace adtype=pla and*adgroupid=65058347419* with
    * new parameter mkgroupid={adgroupid} and mktype={adtype}. Tracking’s MCS data pipeline job replace back to adtype
    * and adgroupid and persist into IMK so there wont be impact to downstream like data and science.
    * See <a href="https://jirap.corp.ebay.com/browse/XC-1464">replace landing page url and rvr_url's mktype and mkgroupid</a>
    *
    * @param channelType channel
    * @param uri         tracking url
    * @return new tracking url
    */
  def replaceMkgroupidMktype(channelType: String, uri: String): String = {
    var newUri = ""
    if (StringUtils.isNotEmpty(uri)) {
      try {
        newUri = uri.replace("mkgroupid", "adgroupid")
          .replace("mktype", "adtype")
      } catch {
        case e: Exception => {
          logger.warn("MalformedUrl", e)
        }
      }
    }
    newUri
  }

  /**
    * Parse mpre from if it's rover url
    *
    * @param channelType channel type
    * @param uri         uri
    * @return mpre
    */
  def replaceMkgroupidMktypeAndParseMpreFromRover(channelType: String, uri: String): String = {
    var newUri = replaceMkgroupidMktype(channelType, uri)
    // parse mpre if url is rover
    if (newUri.startsWith("http://rover.ebay.com") || newUri.startsWith("https://rover.ebay.com")) {
      val query = tools.getQueryString(newUri)
      val landingPageUrl = tools.getParamValueFromQuery(query, "mpre")
      if (StringUtils.isNotEmpty(landingPageUrl)) {
        try {
          newUri = URLDecoder.decode(landingPageUrl, "UTF-8")
        } catch {
          case e: Exception => {
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

  /**
    * Get client id. For ROI, the client id is always 0. No downstream is using it.
    * For the other channel, the client is parsed from the rotation id.
    */
  val getClientIdUdf: UserDefinedFunction = udf((channelType: String, tempUriQuery: String, ridParamName: String, uri: String) => {
    channelType match {
      case "ROI" => tools.getClientIdFromRoverUrl(uri)
      case _ => tools.getClientIdFromRotationId(tools.getParamValueFromQuery(tempUriQuery, ridParamName))
    }
  })

  /**
    * Get rvr_cmnd_type_cd. There are 3 types: click, serve, roi.
    * There is no impression in IMK table
    *
    * @param channelAction Channel Action in String
    * @return number representing the channel action
    */
  def getChannelActionEnum(channelAction: String): String = {
    channelAction match {
      case "CLICK" => "1"
      case "SERVE" => "4"
      case "ROI" => "2"
      case _ => "1"
    }
  }

  /**
    * Get channel id enum. By historical reason, display and dap are mixed. Need to handle the exception.
    * @param channelType input channel type in string
    * @return channel id
    */
  def getChannelTypeEnum(channelType: String): String = {
    if(channelType.equals("DISPLAY")) {
      return "4"
    }
    var channelId = "2"
    try {
      channelId = ChannelIdEnum.valueOf(channelType).getValue
    } catch {
      case e: Exception => {
        logger.warn("Wrong channel type: " + channelType, e)
      }
    }
    channelId
  }

  /**
    * Read everything need from the source table
    *
    * @param inputDateTime input date time
    */
  override def readSource(inputDateTime: ZonedDateTime): DataFrame = {
    val fromDateTime = getLastDoneFileDateTimeAndDelay(inputDateTime, deltaDoneFileDir)._1
    val fromDateString = fromDateTime.format(dtFormatter)
    val startTimestamp = fromDateTime.toEpochSecond * 1000
    val sql = "select * from " + inputSource + " where dt >= '" + fromDateString + "' and eventtimestamp >='" + startTimestamp + "'"
    val sourceDf = sqlsc.sql(sql)
    var imkDf = sourceDf
      .filter(col(SNAPSHOT_ID).isNotNull)
      .filter(col(CHANNEL_TYPE).isin(IMK_CHANNELS: _*))
      .withColumn(DECODED_URL, decodeUrlUdf(getParamFromQueryUdf(col(APPLICATION_PAYLOAD), lit("url_mpre"))))
      .withColumn(TEMP_URI_QUERY, col(DECODED_URL))
      .withColumn("batch_id", getBatchIdUdf())
      .withColumn("file_id", lit(0))
      .withColumn("file_schm_vrsn", lit(4))
      .withColumn(SNAPSHOT_ID, col(SNAPSHOT_ID))
      .withColumn(DT, col(DT))
      .withColumn("srvd_pstn", lit(0))
      .withColumn("rvr_cmnd_type_cd", getChannelActionEnumUdf(col("channelaction")))
      .withColumn("rvr_chnl_type_cd", getChannelTypeEnumUdf(col(CHANNEL_TYPE)))
      .withColumn("cntry_cd", lit(""))
      .withColumn("lang_cd", lit(""))
      .withColumn("trckng_prtnr_id", lit(0))
      .withColumn(CGUID, getParamFromQueryUdf(col(APPLICATION_PAYLOAD), lit(CGUID)))
      .withColumn(GUID, col(GUID))
      .withColumn("user_id", getParamFromQueryUdf(col(APPLICATION_PAYLOAD), lit("u")))
      .withColumn("clnt_remote_ip", getBrowserTypeUdf(getParamFromQueryUdf(col(CLIENT_DATA), lit("RemoteIP"))))
      .withColumn("brwsr_type_id", getBrowserTypeUdf(getParamFromQueryUdf(col(CLIENT_DATA), lit(AGENT))))
      .withColumn("brwsr_name", getParamFromQueryUdf(col(CLIENT_DATA), lit(AGENT)))
      .withColumn("rfrr_dmn_name", getLandingPageDomainUdf(col(REFERRER)))
      .withColumn("rfrr_url", col(REFERRER))
      .withColumn("url_encrptd_yn_ind", lit(0))
      .withColumn("pblshr_id", lit(""))
      .withColumn("lndng_page_dmn_name", getLandingPageDomainUdf(col(DECODED_URL)))
      .withColumn("lndng_page_url", replaceMkgroupidMktypeUdfAndParseMpreFromRoverUdf(col(CHANNEL_TYPE), col("decoded_url")))
      .withColumn("user_query", getUserQueryUdf(col(REFERRER), col(TEMP_URI_QUERY)))
      .withColumn("rule_bit_flag_strng", lit(""))
      .withColumn(EVENT_TIMESTAMP, col(EVENT_TIMESTAMP))
      .withColumn("dflt_bhrv_id", lit(""))
      .withColumn("src_rotation_id", getBrowserTypeUdf(getParamFromQueryUdf(col(APPLICATION_PAYLOAD), lit(ROTID))))
      .withColumn("dst_rotation_id", getBrowserTypeUdf(getParamFromQueryUdf(col(APPLICATION_PAYLOAD), lit(ROTID))))
      .withColumn("user_map_ind", getParamFromQueryUdf(col(APPLICATION_PAYLOAD), lit("u")))
      .withColumn("dst_client_id", setDefaultValueForDstClientIdUdf(getClientIdUdf(
        col(CHANNEL_TYPE), col(TEMP_URI_QUERY), lit("mkrid"),
        col(DECODED_URL))))
      .withColumn("creative_id", lit(-999))
      .withColumn("test_ctrl_flag", lit(0))
      // not in imk will be removed after column selection
      .withColumn("mfe_name", getParamFromQueryUdf(col(TEMP_URI_QUERY), lit(CRLP)))
      .withColumn("mfe_id", getMfeIdUdf(getParamFromQueryUdf(col(TEMP_URI_QUERY), lit(CRLP))))
      .withColumn("kw_id", lit("-999"))
      .withColumn("keyword", getKeywordUdf(col(TEMP_URI_QUERY)))
      .withColumn(MTID, getDefaultNullNumParamValueFromUrlUdf(col(TEMP_URI_QUERY), lit(MTID)))
      .withColumn(CRLP, getParamFromQueryUdf(col(TEMP_URI_QUERY), lit(CRLP)))
      .withColumn(GEOID, getParamFromQueryUdf(col(TEMP_URI_QUERY), lit(GEOID)))
      .withColumn("item_id", getParamFromQueryUdf(col(APPLICATION_PAYLOAD), lit("itemid")))
      .withColumn("transaction_type", getParamFromQueryUdf(col(APPLICATION_PAYLOAD), lit("trans_type")))
      .withColumn("transaction_id", getParamFromQueryUdf(col(APPLICATION_PAYLOAD), lit("trans_id")))
      .withColumn("cart_id", getParamFromQueryUdf(col(APPLICATION_PAYLOAD), lit("cart_id")))
      .withColumn("extrnl_cookie", lit(""))
      .withColumn("ebay_site_id", col("siteid"))
      .withColumn("rvr_url", replaceMkgroupidMktypeUdf(col(CHANNEL_TYPE), col(DECODED_URL)))
      .withColumn("cre_date", lit(""))
      .withColumn("cre_user", lit(""))
      .withColumn("upd_date", lit(""))
      .withColumn("upd_user", lit(""))
      .withColumn("flex_field_vrsn_num", lit("0"))

    // flex fields
    for (i <- 1 to 20) {
      val columnName = "flex_field_" + i
      val paramName = "ff" + i
      imkDf = imkDf.withColumn(columnName, getParamFromQueryUdf(col("temp_uri_query"), lit(paramName)))
    }

    imkDf = imkDf
      // perf track name value
      .withColumn("perf_track_name_value", getPerfTrackNameValueUdf(col("temp_uri_query")))
      .na.fill(schema_delta_apollo.defaultValues).cache()

    // set default values for some columns
    schema_delta_apollo.filterNotColumns(imkDf.columns).foreach(e => {
      imkDf = imkDf.withColumn(e, lit(schema_delta_apollo.defaultValues(e)))
    })
    imkDf
  }

  /**
    * Function to write file to output dir
    *
    * @param df       dataframe to write
    * @param dtString date partition
    */
  override def writeToOutput(df: DataFrame, dtString: String): Unit = {
    // regenerate the final table timestamp format
    // dedupe by rvr_id
    df.show()
    val finalDf = df
      .withColumn("event_ts", getDateTimeUdf(col(EVENT_TIMESTAMP)))
      .select(schema_apollo.dfColumns: _*)
      .dropDuplicates(SNAPSHOT_ID)
      .withColumnRenamed(SNAPSHOT_ID, "rvr_id")
    // save to final output
    finalDf.show()
    this.saveDFToFiles(finalDf, outputPath = outputDir, writeMode = SaveMode.Append, partitionColumn = dt)
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