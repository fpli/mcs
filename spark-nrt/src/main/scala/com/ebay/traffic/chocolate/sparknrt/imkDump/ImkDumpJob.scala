package com.ebay.traffic.chocolate.sparknrt.imkDump

import java.net.{InetAddress, URL}
import java.util.Properties

import com.ebay.app.raptor.chocolate.common.ShortSnapshotId
import com.ebay.traffic.chocolate.monitoring.ESMetrics
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.imkDump.Tools.keywordParams
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
  * Created by ganghuang on 12/3/18.
  * read capping result and generate files for imk table
  */
object ImkDumpJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new ImkDumpJob(params)

    job.run()
    job.stop()
  }
}

class ImkDumpJob(params: Parameter) extends BaseSparkNrtJob(params.appName, params.mode){
  lazy val outputDir: String = params.outPutDir + "/" + params.channel + "/"

  lazy val imkWorkDir: String = params.imkWorkDir + "/" + params.channel + "/"

  lazy val METRICS_INDEX_PREFIX = "chocolate-metrics-"

  @transient var properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("imk_dump.properties"))
    properties
  }

  @transient lazy val inputMetadata: Metadata = {
    val usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("imkdump.upstream.ps"))
    Metadata(params.workDir, params.channel, usage)
  }

  @transient lazy val batchSize: Int = {
    val batchSize = properties.getProperty("imkdump.metafile.batchsize")
    if (StringUtils.isNumeric(batchSize)) {
      Integer.parseInt(batchSize)
    } else {
      10 // default to 10 metafiles
    }
  }

  @transient lazy val metrics: ESMetrics = {
    if (params.elasticsearchUrl != null && !params.elasticsearchUrl.isEmpty) {
      ESMetrics.init(METRICS_INDEX_PREFIX, params.elasticsearchUrl)
      ESMetrics.getInstance()
    } else null
  }

  @transient lazy val outPutFileVersion: String = {
    val fileVersion = properties.getProperty("imkdump.output.file.version")
    if (StringUtils.isNotEmpty(fileVersion)) {
      fileVersion
    } else {
      "4"
    }
  }

  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to run the spark job.
    */
  override def run(): Unit = {
    var dedupeOutputMeta = inputMetadata.readDedupeOutputMeta(".imk")
    if (dedupeOutputMeta.length > batchSize) {
      dedupeOutputMeta = dedupeOutputMeta.slice(0, batchSize)
    }
    val imkWorkDirPath = new Path(imkWorkDir)
    if (!fs.exists(imkWorkDirPath)) {
      fs.mkdirs(imkWorkDirPath)
    }

    dedupeOutputMeta.foreach(metaIter => {
      val file = metaIter._1
      val datesFiles = metaIter._2
      datesFiles.foreach(datesFile => {
        val date = getDate(datesFile._1)
        val df = readFilesAsDFEx(datesFile._2)
        logger.info("load DataFrame, date=" + date +", with files=" + datesFile._2.mkString(","))

        val imkDf = imkDumpCore(df)

        saveDFToFiles(imkDf, imkWorkDir, "gzip", "csv", "bel", headerHint = true)
        renameFile("imk_rvr_trckng_")

        inputMetadata.deleteDedupeOutputMeta(file)
      })
      metrics.meter("imk.dump.spark.out", datesFiles.size)
    })
    metrics.flushMetrics()
  }

  def imkDumpCore(df: DataFrame): DataFrame = {
    df
      .withColumn("batch_id", getBatchIdUdf())
      .withColumn("file_id", lit(""))
      .withColumn("file_schm_vrsn", lit("4"))
      .withColumn("rvr_id", getShortSnapShotIdUdf(col("snapshot_id")))
      .withColumn("event_dt", getDateUdf(col("timestamp")))
      .withColumn("srvd_pstn", lit("0"))
      .withColumn("rvr_cmnd_type_cd", getCmndTypeUdf(col("channel_action")))
      .withColumn("rvr_chnl_type_cd", getParamFromQueryUdf(col("uri"), lit("cid")))
      .withColumn("cntry_cd", lit(""))
      .withColumn("lang_cd", lit(""))
      .withColumn("trckng_prtnr_id", lit("0"))
      .withColumn("cguid", getFromHeaderUdf(col("request_headers"), lit("X-EBAY-C-TRACKING"), lit("cguid")))
      .withColumn("guid", getFromHeaderUdf(col("request_headers"), lit("X-EBAY-C-TRACKING"), lit("tguid")))
      .withColumn("user_id", getValueFromHeaderUdf(col("request_headers"), lit("userid")))
      .withColumn("clnt_remote_ip", getValueFromHeaderUdf(col("request_headers"), lit("X-eBay-Client-IP")))
      .withColumn("brwsr_type_id", getBrowserTypeUdf(col("request_headers")))
      .withColumn("brwsr_name", getFromHeaderUdf(col("request_headers"), lit("X-EBAY-C-ENDUSERCTX"), lit("userAgent")))
      .withColumn("rfrr_dmn_name", getDomainFromHeaderUdf(col("request_headers"), lit("Referer")))
      .withColumn("rfrr_url", getValueFromHeaderUdf(col("request_headers"), lit("Referer")))
      .withColumn("url_encrptd_yn_ind", lit("0"))
      .withColumn("src_rotation_id", getRotationIdUdf(col("uri")))
      .withColumn("dst_rotation_id", getRotationIdUdf(col("uri")))
      .withColumn("dst_client_id", getClientIdUdf(col("uri")))
      .withColumn("pblshr_id", lit(""))
      .withColumn("lndng_page_dmn_name", getLandingPageDomainUdf(col("uri")))
      .withColumn("lndng_page_url", col("uri"))
      .withColumn("user_query", getUserQueryUdf(col("uri")))
      .withColumn("rule_bit_flag_strng", col("rt_rule_flags"))
      .withColumn("flex_field_vrsn_num", lit(""))
      .withColumn("flex_field_1", lit(""))
      .withColumn("flex_field_2", lit(""))
      .withColumn("flex_field_3", lit(""))
      .withColumn("flex_field_4", lit(""))
      .withColumn("flex_field_5", lit(""))
      .withColumn("flex_field_6", lit(""))
      .withColumn("flex_field_7", lit(""))
      .withColumn("flex_field_8", lit(""))
      .withColumn("flex_field_9", lit(""))
      .withColumn("flex_field_10", lit(""))
      .withColumn("flex_field_11", lit(""))
      .withColumn("flex_field_12", lit(""))
      .withColumn("flex_field_13", lit(""))
      .withColumn("flex_field_14", lit(""))
      .withColumn("flex_field_15", lit(""))
      .withColumn("flex_field_16", lit(""))
      .withColumn("flex_field_17", lit(""))
      .withColumn("flex_field_18", lit(""))
      .withColumn("flex_field_19", lit(""))
      .withColumn("flex_field_20", lit(""))
      .withColumn("event_ts", getDateTimeUdf(col("timestamp")))
      .withColumn("dflt_bhrv_id", lit("0"))
      .withColumn("perf_track_name_value", lit(""))
      .withColumn("keyword", getKeywordUdf(col("uri")))
      .withColumn("mt_id", getParamFromQueryUdf(col("uri"), lit("mt_id")))
      .withColumn("geo_id", getValueFromHeaderUdf(col("request_headers"), lit("geoid")))
      .withColumn("crlp", getParamFromQueryUdf(col("uri"), lit("crlp")))
      .withColumn("mfe_name", lit(""))
      .withColumn("user_map_ind", getUserMapIndUdf(col("request_headers")))
      .withColumn("creative_id", lit("-999"))
      .withColumn("test_ctrl_flag", lit("0"))
      .withColumn("transaction_type", lit(""))
      .withColumn("transaction_id", lit(""))
      .withColumn("item_id", getItemIdUdf(col("uri")))
      .withColumn("roi_item_id", lit(""))
      .withColumn("cart_id", lit(""))
      .withColumn("extrnl_cookie", lit(""))
      .withColumn("ebay_site_id", lit(""))
      .withColumn("rvr_url", lit(""))
      .withColumn("mgvalue", lit(""))
      .withColumn("mgvaluereason", lit(""))
      .drop("snapshot_id", "timestamp", "publisher_id", "campaign_id", "request_headers",
        "uri", "response_headers", "rt_rule_flags", "nrt_rule_flags", "channel_action", "channel_type",
        "http_method", "snid", "is_tracked")
  }

  val getUserQueryUdf: UserDefinedFunction = udf((uri: String) => Tools.getQueryString(uri))
  val getValueFromHeaderUdf: UserDefinedFunction = udf((header: String, key: String) => Tools.getValueFromRequestHeader(header, key))
  val getDateTimeUdf: UserDefinedFunction = udf((timestamp: Long) => Tools.getDateTimeFromTimestamp(timestamp))
  val getDateUdf: UserDefinedFunction = udf((timestamp: Long) => Tools.getDateFromTimestamp(timestamp))
  val getClientIdUdf: UserDefinedFunction = udf((uri: String) => Tools.getClientIdFromRotationId(Tools.getParamValueFromUrl(uri, "rid")))
  val getDomainFromHeaderUdf: UserDefinedFunction = udf((request_headers: String, headerKey: String) => Tools.getDomain(Tools.getValueFromRequestHeader(request_headers, headerKey)))
  val getItemIdUdf: UserDefinedFunction = udf((uri: String) => Tools.getItemIdFromUri(uri))
  val getKeywordUdf: UserDefinedFunction = udf((uri: String) => Tools.getParamFromQuery(uri, keywordParams))
  val getLandingPageDomainUdf: UserDefinedFunction = udf((uri: String) => Tools.getDomain(uri))
  val getUserMapIndUdf: UserDefinedFunction = udf((header: String) => Tools.getUserMapInd(Tools.getValueFromRequestHeader(header, "userid")))
  val getParamFromQueryUdf: UserDefinedFunction = udf((uri: String, key: String) => Tools.getParamValueFromUrl(uri, key))
  val getBrowserTypeUdf: UserDefinedFunction = udf((requestHeader: String) => Tools.getBrowserType(Tools.getFromHeader(requestHeader, "X-EBAY-C-ENDUSERCTX", "userAgent")))
  val getCmndTypeUdf: UserDefinedFunction = udf((channelType: String) => Tools.getCommandType(channelType))
  val getShortSnapShotIdUdf: UserDefinedFunction = udf((snapShotId: Long) => new ShortSnapshotId(snapShotId).getRepresentation.toString)
  val getRotationIdUdf: UserDefinedFunction = udf((uri: String) => Tools.convertRotationId(Tools.getParamValueFromUrl(uri, "rid")))
  val getBatchIdUdf: UserDefinedFunction = udf(() => Tools.getBatchId)
  val getFromHeaderUdf: UserDefinedFunction = udf((request_header: String, headerField: String, key: String) => Tools.getFromHeader(request_header, headerField, key))

  def renameFile(prefix: String):Unit = {
    // rename result to output dir
    val outputDirPath = new Path(outputDir)
    if (!fs.exists(outputDirPath)) {
      fs.mkdirs(outputDirPath)
    }

    val hostName = InetAddress.getLocalHost.getHostName

    val fileStatus = fs.listStatus(new Path(imkWorkDir))
    fileStatus.filter(status => status.getPath.getName != "_SUCCESS").foreach(fileStatus => {
      val src = fileStatus.getPath
//      imk_rvr_trckng_????????_??????.V4.*dat.gz
      val target = new Path(outputDir, prefix + Tools.getOutPutFileDate + ".V" + outPutFileVersion + "." + hostName + "." + params.channel + ".dat.gz")
      logger.info("Rename from: " + src.toString + " to: " + target.toString)
      fs.rename(src, target)
    })

  }

  def getDate(date: String): String = {
    val splitted = date.split("=")
    if (splitted != null && splitted.nonEmpty) splitted(1)
    else throw new Exception("Invalid date field in metafile.")
  }

}
