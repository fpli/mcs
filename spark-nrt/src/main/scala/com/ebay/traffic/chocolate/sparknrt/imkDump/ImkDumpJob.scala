package com.ebay.traffic.chocolate.sparknrt.imkDump

import java.net.URL
import java.util.Properties

import com.ebay.app.raptor.chocolate.common.ShortSnapshotId
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.imkDump.Tools.keywordParams
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

/**
  * Created by ganghuang on 12/3/18.
  * read capping result and generate files for imk table
  */

class ImkDumpJob(params: Parameter) extends BaseSparkNrtJob(params.appName, params.mode){
  lazy val outputDir: String = params.outPutDir + "/" + params.channel + "/"

  lazy val epnNrtTempDir: String = params.tmpDir + "/" + params.channel + "/"

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
  /**
    * :: DeveloperApi ::
    * Implemented by subclasses to run the spark job.
    */
  override def run(): Unit = {
    var dedupeOutputMeta = inputMetadata.readDedupeOutputMeta(".imk")
    if (dedupeOutputMeta.length > batchSize) {
      dedupeOutputMeta = dedupeOutputMeta.slice(0, batchSize)
    }

    dedupeOutputMeta.foreach(metaIter => {
      val file = metaIter._1
      val datesFiles = metaIter._2
      datesFiles.foreach(datesFile => {
        val date = getDate(datesFile._1)
        val df = readFilesAsDFEx(datesFile._2)
        logger.info("load DataFrame, date=" + date +", with files=" + datesFile._2.mkString(","))

        val imkDf = nrtCore(df)

        saveDFToFiles(imkDf, epnNrtTempDir, "gzip", "csv", "tab")
        renameFile(outputDir, epnNrtTempDir, date, "dw_imk.imk_")

        inputMetadata.deleteDedupeOutputMeta(file)
      })
    })

  }

  def nrtCore(df: DataFrame): DataFrame = {
    df.withColumn("EVENT_TS", getDateTimeUdf(col("timestamp")))
      .withColumn("USER_QUERY", getUserQueryUdf(col("uri")))
      .withColumn("UPD_USER", lit(""))
      .withColumn("UPD_DATE", lit(""))
      .withColumn("CRE_USER", lit("B_IMK"))
      .withColumn("CRE_DATE", getDateUdf(col("timestamp")))
      .withColumn("RVR_URL", lit(""))
      .withColumn("EBAY_SITE_ID", lit(""))
      .withColumn("TRANSACTION_ID", lit(""))
      .withColumn("TRANSACTION_TYPE", lit(""))
      .withColumn("DFLT_BHRV_ID", lit("0"))
      .withColumn("URL_ENCRPTD_YN_IND", lit("0"))
      .withColumn("RFRR_DMN_NAME", getDomainFromHeaderUdf(col("request_headers"), lit("Referer")))
      .withColumn("TRCKNG_PRTNR_ID", lit("0"))
      .withColumn("SRVD_PSTN", lit("0"))
      .withColumn("FILE_SCHM_VRSN", lit("4"))
      .withColumn("FILE_ID", lit(""))
      .withColumn("ITEM_ID", getItemIdUdf(col("uri")))
      .withColumn("GEO_ID", getValueFromHeaderUdf(col("request_headers"), lit("geoid")))
      .withColumn("KEYWORD", getKeywordUdf(col("uri")))
      .withColumn("MFE_ID", lit("-999"))
      .withColumn("TEST_CTRL_FLAG", lit("0"))
      .withColumn("CREATIVE_ID", lit("-999"))
      .withColumn("LNDNG_PAGE_URL", col("uri"))
      .withColumn("LNDNG_PAGE_DMN_NAME", getLandingPageDomainUdf(col("uri")))
      .withColumn("PBLSHR_ID", lit(""))
      .withColumn("USER_MAP_IND", getUserMapIndUdf(col("request_headers")))
      .withColumn("USER_ID", getUserIdUdf(col("request_headers")))
      .withColumn("GUID", getCguidUdf(col("request_headers"), lit("tguid")))
      .withColumn("CGUID", getCguidUdf(col("request_headers"), lit("cguid")))
      .withColumn("LANG_CD", lit(""))
      .withColumn("CNTRY_CD", lit(""))
      .withColumn("EVENT_DT", getDateUdf(col("timestamp")))
      .withColumn("RVR_CMND_TYPE_CD", getCmndTypeUdf(col("channel_action")))
      .withColumn("SRC_ROTATION_ID", getRotationIdUdf(col("uri")))
      .withColumn("DST_ROTATION_ID", getRotationIdUdf(col("uri")))
      .withColumn("BRWSR_NAME", getValueFromHeaderUdf(col("request_headers"), lit("User-Agent")))
      .withColumn("RVR_ID", getShortSnapShotIdUdf(col("snapshot_id")))
      .withColumn("BATCH_ID", lit(""))
      .withColumn("CRLP", getParamFromQueryUdf(col("uri"), lit("crlp")))
      .withColumn("KW_ID", lit(""))
      .withColumn("CLNT_REMOTE_IP", getValueFromHeaderUdf(col("request_headers"), lit("X-eBay-Client-IP")))
      .withColumn("RVR_CHNL_TYPE_CD", getParamFromQueryUdf(col("uri"), lit("cid")))
      .withColumn("DST_CLIENT_ID", getClientIdUdf(col("uri")))
      .withColumn("BRWSR_TYPE_ID", getBrowserTypeUdf(col("request_headers")))
      .withColumn("RFRR_URL", getValueFromHeaderUdf(col("request_headers"), lit("Referer")))
      .withColumn("EXTRNL_COOKIE", getExtrnlCookieUdf(col("request_headers")))
      .withColumn("MT_ID", getParamFromQueryUdf(col("uri"), lit("mt_id")))
      .withColumn("CART_ID", lit(""))
      .withColumn("RULE_BIT_FLAG_STRNG", col("rt_rule_flags"))
      .drop("snapshot_id", "timestamp", "publisher_id", "campaign_id", "request_headers",
        "uri", "response_headers", "rt_rule_flags", "nrt_rule_flags", "channel_action", "channel_type",
        "http_method", "snid", "is_tracked")
  }

  val getUserQueryUdf: UserDefinedFunction = udf((uri: String) => Tools.getQueryString(uri))
  val getValueFromHeaderUdf: UserDefinedFunction = udf((header: String, key: String) => Tools.getValueFromRequestHeader(header, key))
  val getDateTimeUdf: UserDefinedFunction = udf((timestamp: Long) => Tools.getDateTimeFromTimestamp(timestamp))
  val getDateUdf: UserDefinedFunction = udf((timestamp: Long) => Tools.getDateFromTimestamp(timestamp))
  val getClientIdUdf: UserDefinedFunction = udf((uri: String) => Tools.getClientIdFromRotationId(Tools.getParamValueFromUrl(uri, "rid")))
  val getDomainFromHeaderUdf: UserDefinedFunction = udf((request_headers: String, headerKey: String) => new URL(Tools.getValueFromRequestHeader(request_headers, headerKey)).getHost)
  val getItemIdUdf: UserDefinedFunction = udf((uri: String) => Tools.getItemIdFromUri(uri))
  val getKeywordUdf: UserDefinedFunction = udf((uri: String) => Tools.getParamFromQuery(uri, keywordParams))
  val getLandingPageDomainUdf: UserDefinedFunction = udf((uri: String) => new URL(uri).getHost)
  val getUserMapIndUdf: UserDefinedFunction = udf((header: String) => Tools.getUserMapInd(Tools.getUserIdFromHeader(header)))
  val getUserIdUdf: UserDefinedFunction = udf((header: String) => Tools.getUserIdFromHeader(header))
  val getParamFromQueryUdf: UserDefinedFunction = udf((uri: String, key: String) => Tools.getParamValueFromUrl(uri, key))
  val getBrowserTypeUdf: UserDefinedFunction = udf((requestHeader: String) => Tools.getBrowserType(requestHeader))
  val getCguidUdf: UserDefinedFunction = udf((requestHeader: String, cguid: String) => Tools.getCGuidFromCookie(requestHeader, cguid))
  val getExtrnlCookieUdf: UserDefinedFunction = udf((requestHeader: String) => Tools.getSvidFromCookie(requestHeader))
  val getCmndTypeUdf: UserDefinedFunction = udf((channelType: String) => Tools.getCommandType(channelType))
  val getShortSnapShotIdUdf: UserDefinedFunction = udf((snapShotId: Long) => new ShortSnapshotId(snapShotId).getRepresentation)
  val getRotationIdUdf: UserDefinedFunction = udf((uri: String) => Tools.convertRotationId(Tools.getParamValueFromUrl(uri, "rid")))

  def renameFile(outputDir: String, sparkDir: String, date: String, prefix: String) = {
    // rename result to output dir
    val dateOutputPath = new Path(outputDir + "/" + date)
    var max = -1
    if (fs.exists(dateOutputPath)) {
      val outputStatus = fs.listStatus(dateOutputPath)
      if (outputStatus.length > 0) {
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
        // val seq = ("%5d" format max + 1 + swi._2).replace(" ", "0")
        val seq = swi._2
        val target = new Path(dateOutputPath, prefix +
          date.replaceAll("-", "") + "_" + sc.applicationId + "_" + seq + ".dat.gz")
        logger.info("Rename from: " + src.toString + " to: " + target.toString)
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
