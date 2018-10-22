package com.ebay.traffic.chocolate.sparknrt.epnnrt

import java.net.URL
import java.text.SimpleDateFormat
import java.util.Properties

import com.couchbase.client.java.Bucket
import com.couchbase.client.java.document.JsonDocument
import com.ebay.app.raptor.chocolate.avro.ChannelType
import com.ebay.app.raptor.chocolate.common.ShortSnapshotId
import com.ebay.dukes.CacheClient
import com.ebay.dukes.base.BaseDelegatingCacheClient
import com.ebay.dukes.couchbase2.Couchbase2CacheClient
import com.ebay.globalenv.util.DateTime
import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import com.google.gson.Gson
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object EpnNrtJob extends App {

}

class EpnNrtJob(params: Parameter) extends BaseSparkNrtJob(params.appName, params.mode) {

  lazy val outputDir = params.workDir + "/epn_nrt/"

  lazy val epnNrtTempDir = outputDir + "/tmp/"

  var properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("epnnrt.properties"))
    properties
  }

  @transient lazy val metadata: Metadata = {
    val usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("epnnrt.upstream.epn"));
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

  lazy val ams_map: HashMap[Int, Array[Int]] = {
    val value = Array(Array(2, 1), Array(6, 1), Array(4, 1), Array(10, 1), Array(16, 1), Array(9, 1), Array(5, 1),
      Array(15, 1), Array(3, 1), Array(14, 1), Array(17, 2), Array(12, 1), Array(11, 1), Array(8, 1), Array(13, 1),
      Array(1, 1), Array(7, 1))
    val key = Array(5282, 4686, 705, 709, 1346, 3422, 1553, 710, 5221, 5222, 8971, 724, 707, 3423, 1185, 711, 706)
    var map = new HashMap[Int, Array[Int]]

    for(i <- key.indices) {
      map = map + (key(i) -> value(i))
    }
    map
  }

  lazy val user_agent_map: HashMap[String, Int] = {
    var map = new HashMap[String, Int]
    val agent = Array("msie", "firefox", "chrome", "safari", "opera", "netscape", "navigator", "aol", "mac", "msntv", "webtv",
    "trident", "bingbot", "adsbot-google", "ucweb", "facebookexternalhit", "dvlvik", "ahc", "tubidy", "roku", "ymobile",
    "pycurl", "dailyme", "ebayandroid", "ebayiphone", "ebayipad", "ebaywinphocore", "NULL_USERAGENT", "UNKNOWN_USERAGENT")
    val agentEnum = Array(2, 5, 11, 4, 7, 1, 1, 3, 8, 9, 6, 2, 12, 19, 25, 20, 26, 13, 14, 15, 16, 17, 18, 21, 22, 23, 24, 10, -99)
    for(i <- agent.indices) {
      map = map + (agent(i) -> agentEnum(i))
    }
    map
  }

  lazy val landing_page_pageId_map: HashMap[String, String] = {
    var map = new HashMap[String, String]
    val stream = fs.open(new Path(params.resourceDir + "/" + properties.getProperty("epnnrt.landingpage.type")))
    def readLine = Stream.cons(stream.readLine(), Stream.continually(stream.readLine))
    readLine.takeWhile(_ != null).foreach(line => {
      val parts = line.split("\t")
      map = map + (parts(4) -> parts(1))
    })
    map
  }

  lazy val referer_domain_map: HashMap[String, String] = {
    var map = new HashMap[String, String]
    val stream = fs.open(new Path(params.resourceDir + "/" + properties.getProperty("epnnrt.refng.pblsh")))
    def readLine = Stream.cons(stream.readLine(), Stream.continually(stream.readLine))
    readLine.takeWhile(_ != null).foreach(line => {
      val parts = line.split("\t")
      map = map + (parts(2) -> parts(1))
    })
    map
  }

  var ams_flag_map : mutable.HashMap[String, Int] = {
    var map = new mutable.HashMap[String, Int]
    map += ("google_fltr_do_flag" -> 0)
    map += ("ams_fltr_roi_value" -> 0)
    map += ("traffic_source_code" -> 0)
    map
  }

  @transient lazy val config_flag_map : HashMap[Int, Int] = {
    var map = new HashMap[Int, Int]
    map += (1 -> 3)
    map += (2 -> 0)
    map
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
        logger.info("load DataFrame, date=" + date +", with files=" + datesFile._2.mkString(","))

        println("load DataFrame, date=" + date +", with files=" + datesFile._2.mkString(","))

     //   val dfTest = readFilesAsDFEx(Array(properties.getProperty("epnnrt.landingpage.type")),null,"csv","tab", false)

        //3. build impression dataframe
        val impressionDf = nrtCore_impression(df)
        saveDFToFiles(impressionDf, epnNrtTempDir + "/impression/", "gzip", "csv", "tab")
        renameFile(outputDir + "/impression/", epnNrtTempDir + "/impression/", date, "dw_ams.ams_imprsn_cntnr_cs_")
        //impressionDf.show()

        //4. build click dataframe
        val clickDf = nrtCore_click(df)
        saveDFToFiles(clickDf, epnNrtTempDir + "/click/", "gzip", "csv", "tab")
        renameFile(outputDir + "/click/", epnNrtTempDir + "/click/", date, "dw_ams.ams_clicks_cs_")
        //clickDf.show()

        //5. rename files

        //4. save dataframe to txt
       // saveDFToFiles(impressionDf, outputDir, "gzip", "txt", "tab")

        // 5.delete the finished meta files
        metadata.deleteDedupeOutputMeta(file)
      })
    })
  }

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

  def nrtCore_impression(df: DataFrame): DataFrame = {

    val impressionDf = df.withColumn("IMPRSN_CNTNR_ID", snapshotIdUdf(col("snapshot_id")))
      .withColumn("FILE_SCHM_VRSN_NUM", lit(4))
      .withColumn("FILE_ID", lit(1995))
      .withColumn("BATCH_ID", lit(1994))
      .withColumn("CHNL_ID", getRoverUriInfoUdf(col("uri"), lit(4).cast(IntegerType)))
      .withColumn("CRLTN_GUID_TXT", getGUIDUdf(col("request_headers"), lit("cguid")))
      .withColumn("GUID_TXT", getGUIDUdf(col("request_headers"), lit("tguid")))
      .withColumn("USER_ID", lit(""))
      .withColumn("CLNT_RMT_IP", getValueFromRequestUdf(col("request_headers"), lit("X-eBay-Client-IP")))
      .withColumn("BRWSR_TYPE_NUM", get_browser_type_udf(col("request_headers")))
      .withColumn("BRWSR_NAME", getValueFromRequestUdf(col("request_headers"), lit("User-Agent")))
      .withColumn("RFR_URL_NAME", getValueFromRequestUdf(col("request_headers"), lit("Referer")))
      .withColumn("PRVS_RFR_DMN_NAME", lit(""))
      .withColumn("USER_QUERY_TXT", getUserQueryTextUdf(col("uri")))
      .withColumn("PLCMNT_DATA_TXT", getRoverUriInfoUdf(col("uri"), lit(3).cast(IntegerType)))
      .withColumn("PBLSHR_ID", col("publisher_id"))
      .withColumn("AMS_PBLSHR_CMPGN_ID", col("campaign_id"))
      .withColumn("AMS_TOOL_ID", getToolIdUdf(col("uri")))
      .withColumn("CSTM_ID", getCustomIdUdf(col("uri")))
      .withColumn("FLEX_FLD_VRSN_NUM", lit(0))
      .withColumn("FLEX_FLD_1_TXT",  getFFValueUdf(col("uri"), lit(1)))
      .withColumn("FLEX_FLD_2_TXT",  getFFValueUdf(col("uri"), lit(2)))
      .withColumn("FLEX_FLD_3_TXT",  getFFValueUdf(col("uri"), lit(3)))
      .withColumn("FLEX_FLD_4_TXT",  getFFValueUdf(col("uri"), lit(4)))
      .withColumn("FLEX_FLD_5_TXT",  getFFValueUdf(col("uri"), lit(5)))
      .withColumn("FLEX_FLD_6_TXT",  getFFValueUdf(col("uri"), lit(6)))
      .withColumn("FLEX_FLD_7_TXT",  getFFValueUdf(col("uri"), lit(7)))
      .withColumn("FLEX_FLD_8_TXT",  getFFValueUdf(col("uri"), lit(8)))
      .withColumn("FLEX_FLD_9_TXT",  getFFValueUdf(col("uri"), lit(9)))
      .withColumn("FLEX_FLD_10_TXT",  getFFValueUdf(col("uri"), lit(10)))
      .withColumn("FLEX_FLD_11_TXT",  getFFValueUdf(col("uri"), lit(11)))
      .withColumn("FLEX_FLD_12_TXT",  getFFValueUdf(col("uri"), lit(12)))
      .withColumn("FLEX_FLD_13_TXT",  getFFValueUdf(col("uri"), lit(13)))
      .withColumn("FLEX_FLD_14_TXT",  getFFValueUdf(col("uri"), lit(14)))
      .withColumn("FLEX_FLD_15_TXT",  getFFValueUdf(col("uri"), lit(15)))
      .withColumn("FLEX_FLD_16_TXT",  getFFValueUdf(col("uri"), lit(16)))
      .withColumn("FLEX_FLD_17_TXT",  getFFValueUdf(col("uri"), lit(17)))
      .withColumn("FLEX_FLD_18_TXT",  getFFValueUdf(col("uri"), lit(18)))
      .withColumn("FLEX_FLD_19_TXT",  getFFValueUdf(col("uri"), lit(19)))
      .withColumn("FLEX_FLD_20_TXT",  getFFValueUdf(col("uri"), lit(20)))
      .withColumn("CTX_TXT", getCtxUdf(col("uri")))
      .withColumn("CTX_CLD_TXT", getCtxCalledUdf(col("uri")))
      .withColumn("CTX_RSLT_TXT", lit(""))
      .withColumn("RFR_DMN_NAME", getRefererHostUdf(col("request_headers")))
      .withColumn("IMPRSN_TS", getDateTimeUdf(col("timestamp")))
      .withColumn("AMS_PRGRM_ID", get_ams_prgrm_id_Udf(col("uri"))(0))
      .withColumn("ADVRTSR_ID", get_ams_prgrm_id_Udf(col("uri"))(1))
      .withColumn("AMS_CLICK_FLTR_TYPE_ID", lit(3))
      .withColumn("FILTER_YN_IND", get_filter_yn_ind_udf(col("rt_rule_flags"), col("nrt_rule_flags"), lit("impression")))
      .withColumn("ROVER_URL_TXT", col("uri"))
      .withColumn("MPLX_TIMEOUT_FLAG", lit(""))
      .withColumn("CB_KW", getcbkwUdf(col("uri")))
      .withColumn("CB_CAT", getcbcatUdf(col("uri")))
      .withColumn("CB_EX_KW", get_cb_ex_kw_Udf(col("uri")))
      .withColumn("FB_USED", get_fb_used_Udf(col("uri")))
      .withColumn("AD_FORMAT", get_ad_format_Udf(col("uri")))
      .withColumn("AD_CONTENT_TYPE", get_ad_content_type_Udf(col("uri")))
      .withColumn("LOAD_TIME", get_load_time_udf(col("uri")))
      .withColumn("APP_ID", lit(""))
      .withColumn("APP_PACKAGE_NAME", lit(""))
      .withColumn("APP_NAME", lit(""))
      .withColumn("APP_VERSION", lit(""))
      .withColumn("DEVICE_NAME", lit(""))
      .withColumn("OS_NAME", lit(""))
      .withColumn("OS_VERSION", lit(""))
      .withColumn("UDID", get_udid_Udf(col("uri")))
      .withColumn("SDK_NAME", lit(""))
      .withColumn("SDK_VERSION", lit(""))
      .withColumn("AMS_TRANS_RSN_CD", get_impression_reason_code_udf(col("uri"), col("publisher_id"), col("campaign_id"), col("rt_rule_flags"), col("nrt_rule_flags")))
      .withColumn("TRFC_SRC_CD", get_trfc_src_cd_impression_udf(col("rt_rule_flags")))
      .withColumn("RT_RULE_FLAG1", get_rule_flag_udf(col("rt_rule_flags"), lit(10)))
      .withColumn("RT_RULE_FLAG2", get_rule_flag_udf(col("rt_rule_flags"), lit(0)))
      .withColumn("RT_RULE_FLAG3", get_rule_flag_udf(col("rt_rule_flags"), lit(2)))
      .withColumn("RT_RULE_FLAG4", get_rule_flag_udf(col("rt_rule_flags"), lit(2)))
      .withColumn("RT_RULE_FLAG5", get_rule_flag_udf(col("rt_rule_flags"), lit(9)))
      .withColumn("RT_RULE_FLAG6", get_rule_flag_udf(col("rt_rule_flags"), lit(4)))
      .withColumn("RT_RULE_FLAG7", get_rule_flag_udf(col("rt_rule_flags"), lit(1)))
      .withColumn("RT_RULE_FLAG8", get_rule_flag_udf(col("rt_rule_flags"), lit(11)))
      .withColumn("RT_RULE_FLAG9", lit(0))
      .withColumn("RT_RULE_FLAG10", get_rule_flag_udf(col("rt_rule_flags"), lit(5)))
      .withColumn("RT_RULE_FLAG11", get_rule_flag_udf(col("rt_rule_flags"), lit(3)))
      .withColumn("RT_RULE_FLAG12", lit(0))
      .withColumn("RT_RULE_FLAG13", lit(0))
      .withColumn("RT_RULE_FLAG14", lit(0))
      .withColumn("RT_RULE_FLAG15", lit(0))
      .withColumn("RT_RULE_FLAG16", lit(0))
      .withColumn("RT_RULE_FLAG17", lit(0))
      .withColumn("RT_RULE_FLAG18", lit(0))
      .withColumn("RT_RULE_FLAG19", lit(0))
      .withColumn("RT_RULE_FLAG20", lit(0))
      .withColumn("RT_RULE_FLAG21", lit(0))
      .withColumn("RT_RULE_FLAG22", lit(0))
      .withColumn("RT_RULE_FLAG23", lit(0))
      .withColumn("RT_RULE_FLAG24", lit(0))
      .withColumn("NRT_RULE_FLAG1", lit(0))
      .withColumn("NRT_RULE_FLAG2", lit(0))
      .withColumn("NRT_RULE_FLAG3", lit(0))
      .withColumn("NRT_RULE_FLAG4", lit(0))
      .withColumn("NRT_RULE_FLAG5", lit(0))
      .withColumn("NRT_RULE_FLAG6", lit(0))
      .withColumn("NRT_RULE_FLAG7", lit(0))
      .withColumn("NRT_RULE_FLAG8", lit(0))
      .withColumn("NRT_RULE_FLAG9", lit(0))
      .withColumn("NRT_RULE_FLAG10", lit(0))
      .withColumn("NRT_RULE_FLAG11", lit(0))
      .withColumn("NRT_RULE_FLAG12", lit(0))
      .withColumn("NRT_RULE_FLAG13", lit(0))
      .withColumn("NRT_RULE_FLAG14", lit(0))
      .withColumn("NRT_RULE_FLAG15", lit(0))
      .withColumn("NRT_RULE_FLAG16", lit(0))
      .withColumn("NRT_RULE_FLAG17", lit(0))
      .withColumn("NRT_RULE_FLAG18", lit(0))
      .withColumn("NRT_RULE_FLAG19", lit(0))
      .withColumn("NRT_RULE_FLAG20", lit(0))
      .withColumn("NRT_RULE_FLAG21", lit(0))
      .withColumn("NRT_RULE_FLAG22", lit(0))
      .withColumn("NRT_RULE_FLAG23", lit(0))
      .withColumn("NRT_RULE_FLAG24", lit(0))
      .withColumn("NRT_RULE_FLAG25", lit(0))
      .withColumn("NRT_RULE_FLAG26", lit(0))
      .withColumn("NRT_RULE_FLAG27", lit(0))
      .withColumn("NRT_RULE_FLAG28", lit(0))
      .withColumn("NRT_RULE_FLAG29", lit(0))
      .withColumn("NRT_RULE_FLAG30", lit(0))
      .withColumn("NRT_RULE_FLAG31", lit(0))
      .withColumn("NRT_RULE_FLAG32", lit(0))
      .withColumn("NRT_RULE_FLAG33", lit(0))
      .withColumn("NRT_RULE_FLAG34", lit(0))
      .withColumn("NRT_RULE_FLAG35", lit(0))
      .withColumn("NRT_RULE_FLAG36", lit(0))
      .withColumn("NRT_RULE_FLAG37", get_rule_flag_udf(col("nrt_rule_flags"), lit(0)))
      .withColumn("NRT_RULE_FLAG38", lit(0))
      .withColumn("NRT_RULE_FLAG39", get_rule_flag_udf(col("nrt_rule_flags"), lit(1)))
      .withColumn("NRT_RULE_FLAG40", lit(0))
      .withColumn("NRT_RULE_FLAG41", lit(0))
      .withColumn("NRT_RULE_FLAG42", lit(0))
      .withColumn("NRT_RULE_FLAG43", get_rule_flag_udf(col("nrt_rule_flags"), lit(2)))
      .withColumn("NRT_RULE_FLAG44", lit(0))
      .withColumn("NRT_RULE_FLAG45", lit(0))
      .withColumn("NRT_RULE_FLAG46", lit(0))
      .withColumn("NRT_RULE_FLAG47", lit(0))
      .withColumn("NRT_RULE_FLAG48", lit(0))
      .withColumn("NRT_RULE_FLAG49", lit(0))
      .withColumn("NRT_RULE_FLAG50", lit(0))
      .withColumn("NRT_RULE_FLAG51", get_rule_flag_udf(col("nrt_rule_flags"), lit(4)))
      .withColumn("NRT_RULE_FLAG52", lit(0))
      .withColumn("NRT_RULE_FLAG53", get_rule_flag_udf(col("nrt_rule_flags"), lit(5)))
      .withColumn("NRT_RULE_FLAG54", get_rule_flag_udf(col("nrt_rule_flags"), lit(3)))
      .withColumn("NRT_RULE_FLAG55", lit(0))
      .withColumn("NRT_RULE_FLAG56", get_rule_flag_udf(col("nrt_rule_flags"), lit(4)))
      .withColumn("NRT_RULE_FLAG57", lit(0))
      .withColumn("NRT_RULE_FLAG58", lit(0))
      .withColumn("NRT_RULE_FLAG59", lit(0))
      .withColumn("NRT_RULE_FLAG60", lit(0))
      .withColumn("NRT_RULE_FLAG61", lit(0))
      .withColumn("NRT_RULE_FLAG62", lit(0))
      .withColumn("NRT_RULE_FLAG63", lit(0))
      .withColumn("NRT_RULE_FLAG64", lit(0))
     // .withColumn("Test", test_udf(lit("amspubdomain_7000000000")))
      .cache()


    //impressionDf.select("ROVER_URL_TXT").take(10).foreach(x => println(x))
   // impressionDf.select("RT_RULE_FLAG1", "RT_RULE_FLAG2", "RT_RULE_FLAG3").show()
    //impressionDf.show()
      impressionDf
   // saveDFToFiles(impressionDf, outputDir, "snappy", "txt")
  }



  def nrtCore_click(df: DataFrame): DataFrame = {
    val clickDf  = df.withColumn("IMPRSN_CNTNR_ID", lit(""))
      .withColumn("FILE_SCHM_VRSN_NUM", lit(4))
      .withColumn("FILE_ID", lit(1995))
      .withColumn("BATCH_ID", lit(1994))
      .withColumn("CLICK_ID", snapshotIdUdf(col("snapshot_id")))
      .withColumn("CHNL_ID", getRoverUriInfoUdf(col("uri"), lit(4).cast(IntegerType)))
      .withColumn("CRLTN_GUID_TXT", getGUIDUdf(col("request_headers"), lit("cguid")))
      .withColumn("GUID_TXT", getGUIDUdf(col("request_headers"), lit("tguid")))
      .withColumn("USER_ID", lit(""))
      .withColumn("CLNT_RMT_IP", getValueFromRequestUdf(col("request_headers"), lit("X-eBay-Client-IP")))
      .withColumn("BRWSR_TYPE_NUM",  get_browser_type_udf(col("request_headers")))
      .withColumn("BRWSR_NAME", getValueFromRequestUdf(col("request_headers"), lit("User-Agent")))
      .withColumn("RFR_URL_NAME", getValueFromRequestUdf(col("request_headers"), lit("Referer")))
      .withColumn("ENCRYPTD_IND", lit(0))
      .withColumn("PLCMNT_DATA_TXT", getRoverUriInfoUdf(col("uri"), lit(3).cast(IntegerType)))
      .withColumn("PBLSHR_ID", col("publisher_id"))
      .withColumn("AMS_PBLSHR_CMPGN_ID", col("campaign_id"))
      .withColumn("AMS_TOOL_ID", getToolIdUdf(col("uri")))
      .withColumn("CSTM_ID", getCustomIdUdf(col("uri")))
      .withColumn("LND_PAGE_URL_NAME", getValueFromRequestUdf(col("response_headers"), lit("Location")))
      .withColumn("USER_QUERY_TXT",  getUserQueryTextUdf(col("uri")))
      .withColumn("FLEX_FLD_VRSN_NUM",  lit(0))
      .withColumn("FLEX_FLD_1_TXT",  getFFValueUdf(col("uri"), lit(1)))
      .withColumn("FLEX_FLD_2_TXT",  getFFValueUdf(col("uri"), lit(2)))
      .withColumn("FLEX_FLD_3_TXT",  getFFValueUdf(col("uri"), lit(3)))
      .withColumn("FLEX_FLD_4_TXT",  lit(""))
      .withColumn("IMPRSN_TS",  lit(""))
      .withColumn("CLICK_TS",  getDateTimeUdf(col("timestamp")))
      .withColumn("LAST_VWD_ITEM_ID",  lit(""))
      .withColumn("LAST_VWD_ITEM_TS",  lit(""))
      .withColumn("LAST_ADN_CLICK_ID",  lit(""))
      .withColumn("LAST_ADN_CLICK_TS",  lit(""))
      .withColumn("LAST_VWD_ITEM_ID",  lit(""))
      .withColumn("FLEX_FLD_5_TXT",  lit(""))
      .withColumn("FLEX_FLD_6_TXT",  getFFValueUdf(col("uri"), lit(6)))
      .withColumn("FLEX_FLD_7_TXT",  getFFValueUdf(col("uri"), lit(7)))
      .withColumn("FLEX_FLD_8_TXT",  getFFValueUdf(col("uri"), lit(8)))
      .withColumn("FLEX_FLD_9_TXT",  getFFValueUdf(col("uri"), lit(9)))
      .withColumn("FLEX_FLD_10_TXT",  getFFValueUdf(col("uri"), lit(10)))
      .withColumn("FLEX_FLD_11_TXT",  getFFValueUdf(col("uri"), lit(11)))
      .withColumn("FLEX_FLD_12_TXT",  getFFValueUdf(col("uri"), lit(12)))
      .withColumn("FLEX_FLD_13_TXT",  getFFValueUdf(col("uri"), lit(13)))
      .withColumn("FLEX_FLD_14_TXT",  getFFValueUdf(col("uri"), lit(14)))
      .withColumn("FLEX_FLD_15_TXT",  getFFValueUdf(col("uri"), lit(15)))
      .withColumn("FLEX_FLD_16_TXT",  getFFValueUdf(col("uri"), lit(16)))
      .withColumn("FLEX_FLD_17_TXT",  getFFValueUdf(col("uri"), lit(17)))
      .withColumn("FLEX_FLD_18_TXT",  getFFValueUdf(col("uri"), lit(18)))
      .withColumn("FLEX_FLD_19_TXT",  getFFValueUdf(col("uri"), lit(19)))
      .withColumn("FLEX_FLD_20_TXT",  getFFValueUdf(col("uri"), lit(20)))
      .withColumn("ICEP_FLEX_FLD_VRSN_ID",  lit(""))
      .withColumn("ICEP_FLEX_FLD_1_TXT",  lit(0))
      .withColumn("ICEP_FLEX_FLD_2_TXT",  lit(""))
      .withColumn("ICEP_FLEX_FLD_3_TXT",  lit(""))
      .withColumn("ICEP_FLEX_FLD_4_TXT",  lit(""))
      .withColumn("ICEP_FLEX_FLD_5_TXT",  lit(""))
      .withColumn("ICEP_FLEX_FLD_6_TXT",  lit(""))
      .withColumn("AMS_PRGRM_ID", get_ams_prgrm_id_Udf(col("uri"))(0))
      .withColumn("ADVRTSR_ID", get_ams_prgrm_id_Udf(col("uri"))(1))
      .withColumn("AMS_CLICK_FLTR_TYPE_ID", get_ams_clk_fltr_type_id_udf(col("publisher_id")))
      .withColumn("IMPRSN_LOOSE_MATCH_IND", lit(0))
      .withColumn("FLTR_YN_IND", get_filter_yn_ind_udf(col("rt_rule_flags"), col("nrt_rule_flags"), lit("click")))
      .withColumn("AMS_TRANS_RSN_CD", get_click_reason_code_udf(col("uri"), col("publisher_id"), col("campaign_id"), col("rt_rule_flags"), col("nrt_rule_flags")))
      .withColumn("AMS_PAGE_TYPE_MAP_ID", get_page_id_udf(col("response_headers")))
      .withColumn("RFRNG_DMN_NAME", getRefererHostUdf(col("request_headers")))
      .withColumn("TFS_RFRNG_DMN_NAME",  lit(""))
      .withColumn("GEO_TRGTD_RSN_CD", lit(7))
      .withColumn("SRC_PLCMNT_DATA_TXT", getRoverUriInfoUdf(col("uri"), lit(3).cast(IntegerType)))
      .withColumn("GEO_TRGTD_CNTRY_CD", get_country_locale_udf(col("request_headers")))
      .withColumn("TOOL_LVL_OPTN_IND", get_lego_udf(col("uri")))
      .withColumn("ACNT_LVL_OPTN_IND",  lit(""))
      .withColumn("GEO_TRGTD_IND", lit(""))
      .withColumn("PBLSHR_ACPTD_PRGRM_IND", lit(""))
      .withColumn("INCMNG_CLICK_URL_VCTR_ID", get_icep_vectorid_udf(col("uri")))
      .withColumn("STR_NAME_TXT", get_icep_store_udf(col("uri")))
      .withColumn("ITEM_ID", get_item_id_udf(col("uri")))
      .withColumn("CTGRY_ID", get_cat_id_udf(col("uri")))
      .withColumn("KEYWORD_TXT", get_kw_udf(col("uri")))
      .withColumn("PRGRM_EXCPTN_LIST_IND", lit(""))
      .withColumn("ROI_FLTR_YN_IND", get_roi_fltr_yn_ind_udf(col("uri"), col("publisher_id"), col("request_headers")))
      .withColumn("SELLER_NAME", get_seller_udf(col("uri")))
      .withColumn("ROVER_URL", col("uri"))
      .withColumn("MPLX_TIMEOUT_FLAG",  lit(""))
      .withColumn("APP_ID",  lit(""))
      .withColumn("APP_PACKAGE_NAME",  lit(""))
      .withColumn("APP_NAME",  lit(""))
      .withColumn("APP_VERSION",  lit(""))
      .withColumn("DEVICE_NAME",  lit(""))
      .withColumn("OS_NAME",  lit(""))
      .withColumn("OS_VERSION",  lit(""))
      .withColumn("UDID", get_udid_Udf(col("uri")))
      .withColumn("SDK_NAME",  lit(""))
      .withColumn("SDK_VERSION",  lit(""))
      .withColumn("TRFC_SRC_CD",  get_trfc_src_cd_click_udf(col("rt_rule_flags")))
      .withColumn("ROI_RULE_VALUE",  get_roi_rule_value_udf(col("uri"), col("publisher_id"), col("request_headers")))
      .withColumn("RT_RULE_FLAG1", get_rule_flag_udf(col("rt_rule_flags"), lit(10)))
      .withColumn("RT_RULE_FLAG2", get_rule_flag_udf(col("rt_rule_flags"), lit(0)))
      .withColumn("RT_RULE_FLAG3", get_rule_flag_udf(col("rt_rule_flags"), lit(2)))
      .withColumn("RT_RULE_FLAG4", get_rule_flag_udf(col("rt_rule_flags"), lit(2)))
      .withColumn("RT_RULE_FLAG5", get_rule_flag_udf(col("rt_rule_flags"), lit(9)))
      .withColumn("RT_RULE_FLAG6", get_rule_flag_udf(col("rt_rule_flags"), lit(4)))
      .withColumn("RT_RULE_FLAG7", get_rule_flag_udf(col("rt_rule_flags"), lit(1)))
      .withColumn("RT_RULE_FLAG8", get_rule_flag_udf(col("rt_rule_flags"), lit(11)))
      .withColumn("RT_RULE_FLAG9", lit(0))
      .withColumn("RT_RULE_FLAG10", get_rule_flag_udf(col("rt_rule_flags"), lit(5)))
      .withColumn("RT_RULE_FLAG11", get_rule_flag_udf(col("rt_rule_flags"), lit(3)))
      .withColumn("RT_RULE_FLAG12", lit(0))
      .withColumn("RT_RULE_FLAG13", lit(0))
      .withColumn("RT_RULE_FLAG14", lit(0))
      .withColumn("RT_RULE_FLAG15", lit(0))
      .withColumn("RT_RULE_FLAG16", lit(0))
      .withColumn("RT_RULE_FLAG17", lit(0))
      .withColumn("RT_RULE_FLAG18", lit(0))
      .withColumn("RT_RULE_FLAG19", lit(0))
      .withColumn("RT_RULE_FLAG20", lit(0))
      .withColumn("RT_RULE_FLAG21", lit(0))
      .withColumn("RT_RULE_FLAG22", lit(0))
      .withColumn("RT_RULE_FLAG23", lit(0))
      .withColumn("RT_RULE_FLAG24", lit(0))
      .withColumn("NRT_RULE_FLAG1", lit(0))
      .withColumn("NRT_RULE_FLAG2", lit(0))
      .withColumn("NRT_RULE_FLAG3", lit(0))
      .withColumn("NRT_RULE_FLAG4", lit(0))
      .withColumn("NRT_RULE_FLAG5", lit(0))
      .withColumn("NRT_RULE_FLAG6", lit(0))
      .withColumn("NRT_RULE_FLAG7", lit(0))
      .withColumn("NRT_RULE_FLAG8", lit(0))
      .withColumn("NRT_RULE_FLAG9", lit(0))
      .withColumn("NRT_RULE_FLAG10", lit(0))
      .withColumn("NRT_RULE_FLAG11", lit(0))
      .withColumn("NRT_RULE_FLAG12", lit(0))
      .withColumn("NRT_RULE_FLAG13", lit(0))
      .withColumn("NRT_RULE_FLAG14", lit(0))
      .withColumn("NRT_RULE_FLAG15", lit(0))
      .withColumn("NRT_RULE_FLAG16", lit(0))
      .withColumn("NRT_RULE_FLAG17", lit(0))
      .withColumn("NRT_RULE_FLAG18", lit(0))
      .withColumn("NRT_RULE_FLAG19", lit(0))
      .withColumn("NRT_RULE_FLAG20", lit(0))
      .withColumn("NRT_RULE_FLAG21", lit(0))
      .withColumn("NRT_RULE_FLAG22", lit(0))
      .withColumn("NRT_RULE_FLAG23", lit(0))
      .withColumn("NRT_RULE_FLAG24", lit(0))
      .withColumn("NRT_RULE_FLAG25", lit(0))
      .withColumn("NRT_RULE_FLAG26", lit(0))
      .withColumn("NRT_RULE_FLAG27", lit(0))
      .withColumn("NRT_RULE_FLAG28", lit(0))
      .withColumn("NRT_RULE_FLAG29", lit(0))
      .withColumn("NRT_RULE_FLAG30", lit(0))
      .withColumn("NRT_RULE_FLAG31", lit(0))
      .withColumn("NRT_RULE_FLAG32", lit(0))
      .withColumn("NRT_RULE_FLAG33", lit(0))
      .withColumn("NRT_RULE_FLAG34", lit(0))
      .withColumn("NRT_RULE_FLAG35", lit(0))
      .withColumn("NRT_RULE_FLAG36", lit(0))
      .withColumn("NRT_RULE_FLAG37", get_rule_flag_udf(col("nrt_rule_flags"), lit(0)))
      .withColumn("NRT_RULE_FLAG38", lit(0))
      .withColumn("NRT_RULE_FLAG39", get_rule_flag_udf(col("nrt_rule_flags"), lit(1)))
      .withColumn("NRT_RULE_FLAG40", lit(0))
      .withColumn("NRT_RULE_FLAG41", lit(0))
      .withColumn("NRT_RULE_FLAG42", lit(0))
      .withColumn("NRT_RULE_FLAG43", get_rule_flag_udf(col("nrt_rule_flags"), lit(2)))
      .withColumn("NRT_RULE_FLAG44", lit(0))
      .withColumn("NRT_RULE_FLAG45", lit(0))
      .withColumn("NRT_RULE_FLAG46", lit(0))
      .withColumn("NRT_RULE_FLAG47", lit(0))
      .withColumn("NRT_RULE_FLAG48", lit(0))
      .withColumn("NRT_RULE_FLAG49", lit(0))
      .withColumn("NRT_RULE_FLAG50", lit(0))
      .withColumn("NRT_RULE_FLAG51", get_rule_flag_udf(col("nrt_rule_flags"), lit(4)))
      .withColumn("NRT_RULE_FLAG52", lit(0))
      .withColumn("NRT_RULE_FLAG53", get_rule_flag_udf(col("nrt_rule_flags"), lit(5)))
      .withColumn("NRT_RULE_FLAG54", get_rule_flag_udf(col("nrt_rule_flags"), lit(3)))
      .withColumn("NRT_RULE_FLAG55", lit(0))
      .withColumn("NRT_RULE_FLAG56", get_rule_flag_udf(col("nrt_rule_flags"), lit(4)))
      .withColumn("NRT_RULE_FLAG57", lit(0))
      .withColumn("NRT_RULE_FLAG58", lit(0))
      .withColumn("NRT_RULE_FLAG59", lit(0))
      .withColumn("NRT_RULE_FLAG60", lit(0))
      .withColumn("NRT_RULE_FLAG61", lit(0))
      .withColumn("NRT_RULE_FLAG62", lit(0))
      .withColumn("NRT_RULE_FLAG63", lit(0))
      .withColumn("NRT_RULE_FLAG64", lit(0))
      .withColumn("NRT_RULE_FLAG65", lit(0))
      .withColumn("NRT_RULE_FLAG66", lit(0))
      .withColumn("NRT_RULE_FLAG67", lit(0))
      .withColumn("NRT_RULE_FLAG68", lit(0))
      .withColumn("NRT_RULE_FLAG69", lit(0))
      .withColumn("NRT_RULE_FLAG70", lit(0))
      .withColumn("NRT_RULE_FLAG71", lit(0))
      .withColumn("NRT_RULE_FLAG72", lit(0))
      .withColumn("NRT_RULE_FLAG73", lit(0))
      .withColumn("NRT_RULE_FLAG74", lit(0))
      .withColumn("NRT_RULE_FLAG75", lit(0))
      .withColumn("NRT_RULE_FLAG76", lit(0))
      .withColumn("NRT_RULE_FLAG77", lit(0))
      .withColumn("NRT_RULE_FLAG78", lit(0))
      .withColumn("NRT_RULE_FLAG79", lit(0))
      .withColumn("NRT_RULE_FLAG80", lit(0))
      .cache()

   // clickDf.select("ITEM_ID", "TOOL_LVL_OPTN_IND").show()
    clickDf
  }

  // all udf
  val snapshotIdUdf = udf((snapshotId: String) => {
    new ShortSnapshotId(snapshotId.toLong).getRepresentation.toString
  })

 // val getRoverChannelIdUdf = udf((uri: String) => getRoverUriInfo(uri, 4))
  val getRoverUriInfoUdf = udf((uri: String, index: Int) => getRoverUriInfo(uri, index))

  val getGUIDUdf = udf((requestHeader: String, guid: String) => getGUIDFromCookie(requestHeader, guid))
  val getValueFromRequestUdf = udf((requestHeader: String, key: String) => getValueFromRequest(requestHeader, key))
  val getUserQueryTextUdf = udf((url: String) => getUserQueryTxt(url))
  val getToolIdUdf = udf((url: String) => getQueryParam(url, "toolid"))
  val getCustomIdUdf = udf((url: String) => getQueryParam(url, "customid"))
  val getFFValueUdf = udf((url: String, index: String) => getFFValue(url, index))
  val getCtxUdf = udf((url: String) => getQueryParam(url, "ctx"))
  val getCtxCalledUdf = udf((url: String) => getQueryParam(url, "ctx_called"))
  val getcbkwUdf = udf((url: String) => getQueryParam(url, "cb_kw"))
  val getRefererHostUdf = udf((requestHeaders: String) => getRefererHost(requestHeaders))
  val getDateTimeUdf = udf((timestamp: Long) => getDateTimeFromTimestamp(timestamp))
  val getcbcatUdf = udf((url: String) => getQueryParam(url, "cb_cat"))
  val get_ams_prgrm_id_Udf = udf((uri: String) => getPrgrmIdAdvrtsrIdFromAMSClick(getRoverUriInfo(uri, 3)))
  val get_cb_ex_kw_Udf = udf((url: String) => getQueryParam(url, "cb_ex_kw"))
  val get_fb_used_Udf = udf((url: String) => getQueryParam(url, "fb_used"))
  val get_ad_format_Udf = udf((url: String) => getQueryParam(url, "ad_format"))
  val get_ad_content_type_Udf = udf((url: String) => getQueryParam(url, "ad_content_type"))
  val get_load_time_udf = udf((url: String) => getQueryParam(url, "load_time"))
  val get_udid_Udf = udf((url: String) => getQueryParam(url, "udid"))
  val get_rule_flag_udf = udf((ruleFlag: Long, index: Int) => getRuleFlag2(ruleFlag, index))
  val get_country_locale_udf = udf((requestHeader: String) => getCountryLocaleFromHeader(requestHeader))
  val get_lego_udf = udf((uri: String) => getToolLvlOptn(uri))
  val get_icep_vectorid_udf = udf((uri: String) => getQueryParam(uri, "icep_vectorid"))
  val get_icep_store_udf = udf((uri: String) => getQueryParam(uri, "icep_store"))
  val get_item_id_udf = udf((uri: String) => getItemId(uri))
  val get_cat_id_udf = udf((uri: String) => getQueryParam(uri, "catId"))
  val get_kw_udf = udf((uri: String) => getQueryParam(uri, "kw"))
  val get_seller_udf = udf((uri: String) => getQueryParam(uri, "icep_sellerId"))
  val get_trfc_src_cd_click_udf = udf((ruleFlag: Long) => get_TRFC_SRC_CD(ruleFlag, "click"))
  val get_trfc_src_cd_impression_udf = udf((ruleFlag: Long) => get_TRFC_SRC_CD(ruleFlag, "impression"))
  val get_browser_type_udf = udf((requestHeader: String) => getBrowserType(requestHeader))
  val get_filter_yn_ind_udf = udf((rt_rule_flag: Long, nrt_rule_flag: Long, action: String) => getFilter_Yn_Ind(rt_rule_flag, nrt_rule_flag, action))
  val get_page_id_udf = udf((responseHeaders: String) => getPageIdByLandingPage(responseHeaders))
  val get_roi_rule_value_udf = udf((uri: String, publisherId: String, requestHeader: String) => getRoiRuleValue(getRoverUriInfo(uri, 3), publisherId, getValueFromRequest(requestHeader, "Referer"))._1)
  val get_roi_fltr_yn_ind_udf = udf((uri: String, publisherId: String, requestHeader: String) => getRoiRuleValue(getRoverUriInfo(uri, 3), publisherId, getValueFromRequest(requestHeader, "Referer"))._2)
  val get_ams_clk_fltr_type_id_udf = udf((publisherId: String) => getclickFilterTypeId(publisherId))
  val get_click_reason_code_udf = udf((uri: String, publisherId: String, campaignId: String, rt_rule_flag: Long, nrt_rule_flag: Long) => getReasonCode("click", getRoverUriInfo(uri, 3), publisherId, campaignId, rt_rule_flag, nrt_rule_flag))
  val get_impression_reason_code_udf = udf((uri: String, publisherId: String, campaignId: String, rt_rule_flag: Long, nrt_rule_flag: Long) => getReasonCode("impression", getRoverUriInfo(uri, 3), publisherId, campaignId, rt_rule_flag, nrt_rule_flag))

  val test_udf = udf((test: String) => getclickFilterTypeId(test))


  def getDateTimeFromTimestamp(timestamp: Long): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    df.format(timestamp)
  }

  def getRefererHost(requestHeaders: String): String = {
    new URL(getValueFromRequest(requestHeaders, "Referer")).getHost()
  }

  def getFFValue(uri: String, index: String): String = {
    val key = "ff" + index
    getQueryParam(uri, key)
  }

  def getRoverUriInfo(uri: String, index: Int): String = {
    val path = new URL(uri).getPath()
    if (path != null && path != "" && index >= 0 && index <= 4) {
      return path.split("/")(index)
    }
    ""
  }

  def getGUIDFromCookie(requestHeader: String, guid: String) : String = {
    if (requestHeader != null) {
      val cookie = getValueFromRequest(requestHeader, "Cookie")
      if (cookie != null) {
        val index = cookie.indexOf(guid)
        return cookie.substring(index + 6, index + 38)
      }
    }
    ""
  }

  def getValueFromRequest(request: String, key: String): String = {
    if (request != null) {
      val parts = request.split("\\|")
      for (i <- 0 until parts.length) {
        val part = parts(i)
        val splits = part.split(":")
        if (splits.length == 2 && splits(0).trim.equalsIgnoreCase(key))
          return splits(1)
        else if (splits.length == 3 && splits(0).trim.equalsIgnoreCase(key))
          return splits(1).concat(":").concat(splits(2))
      }
    }
    ""
  }

  def getUserQueryTxt(uri: String): String = {
    var value = getQueryParam(uri, "item")
    if (value != null && !value.equals(""))
      return value
    value = getQueryParam(uri, "uq")
    if (value != null && !value.equals(""))
      return value
    value = getQueryParam(uri, "ext")
    if (value != null && !value.equals(""))
      return value
    value = getQueryParam(uri, "satitle")
    if (value != null && !value.equals(""))
      return value
    ""
  }

  def getQueryParam(uri: String, param: String): String = {
    if (uri != null) {
      val params = new URL(uri).getQuery().split("&")
      for (i <- params) {
        val key = i.split("=")(0)
        if (key.equalsIgnoreCase(param))
          return i.split("=")(1)
      }
    }
    ""
  }

  def getPrgrmIdAdvrtsrIdFromAMSClick(rotationId: String): Array[Int] = {
    if (rotationId == null || rotationId.equals(""))
      return Array.empty[Int]
    val parts = rotationId.split("-")
    if (parts.length == 4)
      return ams_map(parts(0).toInt)
    Array.empty[Int]
  }

  def getRuleFlag2(flag: Long, index: Int): Int = {
    if ((flag & 1L << index) == (1L << index))
      return 1
    0
  }

  def getCountryLocaleFromHeader(requestHeader: String): String = {
    var accept = getValueFromRequest(requestHeader, "accept-language")
    if (accept != null)
      accept = accept.split(",")(0)
    if (accept != null)
      accept = accept.split("-")(1)
    accept
  }

  def getToolLvlOptn(uri: String): Int = {
    val value = getQueryParam(uri, "lgeo")
    if (value != null && !value.equals(""))
      if (value.equals("1") || value.equals("0"))
        return 1
    0
  }

  def getItemId(uri: String): String = {
    val array = Array("icep_item", "icep_itemid", "icep_item_id", "item", "itemid")
    for (i <- 0 until array.length) {
      val value = getQueryParam(uri, array(i))
      if (!value.equals("")) {
        return value
      }
    }
    ""
  }

  def get_TRFC_SRC_CD(flag: Long, action: String): Int = {
    if (action.equalsIgnoreCase("click")) {
      val res = getRuleFlag2(flag, 9)
      if (res == 0)
        return 0
      else {
        ams_flag_map("traffic_source_code") = 2
        return 2
      }
    }
    if (action.equalsIgnoreCase("impression")) {
      return 0
    }
    0
  }

  def getBrowserType(requestHeader: String): Int = {
    val userAgentStr = getValueFromRequest(requestHeader, "User-Agent")
    if (userAgentStr == null)
      return user_agent_map("NULL_USERAGENT")
    val agentStr = userAgentStr.toLowerCase()
    for ((k, v) <- user_agent_map) {
      if (agentStr.contains(k))
        return v
    }
    user_agent_map("UNKNOWN_USERAGENT")
  }

  def getFilter_Yn_Ind(rt_rule_flag: Long, nrt_rule_flag: Long, action: String): Int = {
    if (action.equalsIgnoreCase("impression")) {
      return getRuleFlag2(rt_rule_flag, 0) | getRuleFlag2(rt_rule_flag, 2) |
        getRuleFlag2(rt_rule_flag, 9) | getRuleFlag2(rt_rule_flag, 4) | getRuleFlag2(rt_rule_flag, 1) |
        getRuleFlag2(rt_rule_flag, 11) | getRuleFlag2(rt_rule_flag, 5) | getRuleFlag2(rt_rule_flag, 3)
    }
    if(action.equalsIgnoreCase("click")) {
      return getRuleFlag2(rt_rule_flag, 0) | getRuleFlag2(rt_rule_flag, 2) |
        getRuleFlag2(rt_rule_flag, 9) | getRuleFlag2(rt_rule_flag, 4) | getRuleFlag2(rt_rule_flag, 1) |
        getRuleFlag2(rt_rule_flag, 11) | getRuleFlag2(rt_rule_flag, 5) | getRuleFlag2(rt_rule_flag, 3) |
        getRuleFlag2(nrt_rule_flag, 1) | getRuleFlag2(nrt_rule_flag, 2) | getRuleFlag2(nrt_rule_flag, 4) |
        getRuleFlag2(nrt_rule_flag, 5) | getRuleFlag2(nrt_rule_flag, 3)
    }
    0
  }

  def getPageIdByLandingPage(responseHeader: String): String = {
    val location = getValueFromRequest(responseHeader, "Location")
    if (location == null || location.equals(""))
      return ""
    val splits = location.trim.split("/")
    if (splits(2).startsWith("cgi6") && landing_page_pageId_map.contains("http://" + splits(2) + "/ws/eBayISAPI.dll?ViewStoreV4&name=")) {
      return landing_page_pageId_map("http://" + splits(2) + "/ws/eBayISAPI.dll?ViewStoreV4&name=")
    } else if (splits(2).startsWith("cgi") && landing_page_pageId_map.contains("http://" + splits(2) + "/ws/eBayISAPI.dll?ViewItem")) {
      return landing_page_pageId_map("http://" + splits(2) + "/ws/eBayISAPI.dll?ViewItem")
    }
    if (splits.length >= 3) {
      if (landing_page_pageId_map.contains("http://" + splits(2) + "/")){
        return landing_page_pageId_map("http://" + splits(2) + "/")
      }
      if (landing_page_pageId_map.contains("http://" + splits(2) + "/" + splits(3) + "/")) {
        return landing_page_pageId_map("http://" + splits(2) + "/" + splits(3) + "/")
      }
    }
    ""
  }

  def getclickFilterTypeId(publisherId: String): Int = {
    var clickFilterTypeId = 3
    val list = getAdvClickFilterMap(publisherId)
    list.foreach(e => {
      if (e.getStatus_enum != null) {
        if (e.getStatus_enum.equals("1") || e.getStatus_enum.equals("2"))
          if (e.getAms_clk_fltr_type_id != 0)
            clickFilterTypeId = e.getAms_clk_fltr_type_id
      }
    })
    clickFilterTypeId
  }

  def getRoiRuleValue(rotationId: String, publisherId: String, referer_domain: String): (Int, Int) = {
    var temp_roi_values = 0
    var roiRuleValues = 0
    var amsFilterRoiValue = 0
    var roi_fltr_yn_ind = 0

    if (isDefinedPublisher(publisherId) && isDefinedAdvertiserId(rotationId)) {
      if(callRoiRulesSwitch(publisherId, getPrgrmIdAdvrtsrIdFromAMSClick(rotationId)(1).toString).equals("2")) {
        val roiRuleList = lookupAdvClickFilterMapAndROI(publisherId, getPrgrmIdAdvrtsrIdFromAMSClick(rotationId)(1).toString)
        roiRuleList(0).setRule_result(callRoiSdkRule(roiRuleList(0).getIs_rule_enable, roiRuleList(0).getIs_pblshr_advsr_enable_rule, 0))
        roiRuleList(1).setRule_result(callRoiEbayReferrerRule(roiRuleList(1).getIs_rule_enable, roiRuleList(1).getIs_pblshr_advsr_enable_rule, 0))
        roiRuleList(2).setRule_result(callRoiNqBlacklistRule(roiRuleList(2).getIs_rule_enable, roiRuleList(2).getIs_pblshr_advsr_enable_rule, 0))
        roiRuleList(3).setRule_result(callRoiNqWhitelistRule(publisherId, roiRuleList(2).getIs_rule_enable, roiRuleList(2).getIs_pblshr_advsr_enable_rule, referer_domain))
        roiRuleList(4).setRule_result(callRoiMissingReferrerUrlRule(roiRuleList(4).getIs_rule_enable, roiRuleList(4).getIs_pblshr_advsr_enable_rule, referer_domain))
        roiRuleList(5).setRule_result(callRoiNotRegisteredRule(publisherId, roiRuleList(5).getIs_rule_enable, roiRuleList(5).getIs_pblshr_advsr_enable_rule, referer_domain))

        for (i <- roiRuleList.indices) {
          temp_roi_values = temp_roi_values + (roiRuleList(i).getRule_result << i)
        }
      }
    }
    ams_flag_map("google_fltr_do_flag") = lookupRefererDomain(referer_domain, isDefinedPublisher(publisherId), publisherId)
    roiRuleValues = temp_roi_values + ams_flag_map("google_fltr_do_flag") << 6
    if (roiRuleValues != 0) {
      amsFilterRoiValue = 1
      roi_fltr_yn_ind = 1
    }
    ams_flag_map("ams_fltr_roi_value") = amsFilterRoiValue

    (roiRuleValues, roi_fltr_yn_ind)
  }

  def lookupRefererDomain(referer_domain: String, is_defined_publisher: Boolean, publisherId: String):Int = {
    var result = 0
    var loop = true
    if ((!(referer_domain == "")) && is_defined_publisher) {
      if (referer_domain_map.contains(publisherId)) {
        val domains = referer_domain_map(publisherId)
          domains.foreach(e => {
            if (loop) {
              if (e.equals(referer_domain)) {
                result = 1
                loop = false
              }
            }
          })
      }
    }
    result
  }

  def callRoiSdkRule(is_rule_enable: Int, is_pblshr_advsr_enable_rule: Int, rt_rule_19_value: Int):Int =
    if (is_rule_enable == 1 && is_pblshr_advsr_enable_rule == 1 && rt_rule_19_value == 0) 1
    else 0

  def callRoiEbayReferrerRule(is_rule_enable: Int, is_pblshr_advsr_enable_rule: Int, rt_rule_9_value: Int): Int =
    if (is_rule_enable == 1 && is_pblshr_advsr_enable_rule == 1 && rt_rule_9_value == 1) 1
  else 0

  def callRoiNqBlacklistRule(is_rule_enable: Int, is_pblshr_advsr_enable_rule: Int, rt_rule_15_value: Int): Int =
    if (is_rule_enable == 1 && is_pblshr_advsr_enable_rule == 1 && rt_rule_15_value == 1) 1
  else 0

  def callRoiMissingReferrerUrlRule(is_rule_enable: Int, is_pblshr_advsr_enable_rule: Int, referer_url: String): Int =
    if (is_rule_enable == 1 && is_pblshr_advsr_enable_rule == 1 && referer_url == "") 1
  else 0


  def callRoiNqWhitelistRule(publisherId: String, is_rule_enable: Int, is_pblshr_advsr_enable_rule: Int, referer_domain: String): Int = {
    callRoiRuleCommon(publisherId, is_rule_enable, is_pblshr_advsr_enable_rule, referer_domain, "NETWORK_QUALITY_WHITELIST")
  }

  def callRoiNotRegisteredRule(publisherId: String, is_rule_enable: Int, is_pblshr_advsr_enable_rule: Int, referer_domain: String): Int = {
    callRoiRuleCommon(publisherId, is_rule_enable, is_pblshr_advsr_enable_rule, referer_domain, "APP_REGISTERED")
  }

  def callRoiRuleCommon(publisherId: String, is_rule_enable: Int, is_pblshr_advsr_enable_rule: Int, referer_domain: String, dimensionLookupConstants: String): Int = {
    var result = 0
    if (is_rule_enable == 1 && is_pblshr_advsr_enable_rule == 1) {
      if (ams_flag_map("traffic_source_code") == 0 || ams_flag_map("traffic_source_code") == 2) {
        if (referer_domain.equals(""))
          result = 0
        else {
          val pubdomainlist = amsPubDomainLookup(publisherId, dimensionLookupConstants)
          if (pubdomainlist.isEmpty)
            result = 1
          else {
            result = 1
            var loop = true
            var clean_ref_url = referer_domain.trim.toLowerCase
            if (!clean_ref_url.startsWith("."))
              clean_ref_url = ".".concat(clean_ref_url)
            pubdomainlist.foreach(e => {
              if (loop) {
                var domain_entry = e.getUrl_domain.trim
                if (!domain_entry.startsWith("."))
                  domain_entry = ".".concat(domain_entry)
                if (clean_ref_url.endsWith(domain_entry.toLowerCase)) {
                  result = 0
                  loop = false
                }
              }
            })
          }
        }
      }
    }
    result
  }

  def amsPubDomainLookup(publisherId: String, roi_rule: String): ListBuffer[PubDomainInfo] = {
    var list: ListBuffer[PubDomainInfo] = ListBuffer.empty[PubDomainInfo]
    if (publisherId != null && !publisherId.equals("")) {
      val map = CouchbaseClient.getBucket()._2.get("amspubdomain_" + publisherId, classOf[JsonDocument])
      if (map == null)
        return list
      for (i <- 0 until map.content().getNames.size()) {
        list += new Gson().fromJson(String.valueOf(map.content().get(i.toString)), classOf[PubDomainInfo])
      }
    }
    if (roi_rule.equals("NETWORK_QUALITY_WHITELIST"))
      list.filterNot(e => !e.getDomain_status_enum.equals("1") || !e.getWhitelist_status_enum.equals("1") && e.getIs_registered.equals("0"))
    list
  }

  def lookupAdvClickFilterMapAndROI(publisherId: String, advertiserId: String): ListBuffer[RoiRule] = {
    val roiList = getRoiRuleList()
    val clickFilterMapList = getAdvClickFilterMap(publisherId)
    var loop = true
    clickFilterMapList.filter(e => e.getAms_advertiser_id.equalsIgnoreCase(advertiserId))
    clickFilterMapList.foreach(e => {
      roiList.foreach(k => {
        if (k.getAms_clk_fltr_type_id.equals(e.getAms_clk_fltr_type_id) && loop) {
          if (e.getStatus_enum != null) {
            if (e.getStatus_enum.equals("1")) {
              k.setIs_pblshr_advsr_enable_rule(0)
              loop = false
            }
          }
          if (e.getStatus_enum != null) {
            if (e.getStatus_enum.equals("2")) {
              k.setIs_pblshr_advsr_enable_rule(1)
              loop = false
            }
          }
        }
      })
    })
    roiList
  }

  def getRoiRuleList(): ListBuffer[RoiRule] = {
    var list: ListBuffer[RoiRule] = ListBuffer.empty[RoiRule]
    for (i <- 0 until 6) {
      var value = 0
      val rr = new RoiRule
      value = clickFilterTypeLookup(i + 1, ams_flag_map("traffic_source_code"))
      if (value == 0) {
        rr.setIs_rule_enable(0)
        rr.setAms_clk_fltr_type_id(0)
      } else {
        rr.setIs_rule_enable(1)
        rr.setAms_clk_fltr_type_id(1)
      }
      list += rr
    }
    list
  }

  def clickFilterTypeLookup(rule_id: Int, traffic_source: Int): Int = {
    var clickFilterTypeLookupEnum = 0
    val cfste = ClickFilterSubTypeEnum.get(rule_id)
    val tse = TrafficSourceEnum.get(traffic_source)
    import scala.collection.JavaConversions._
    for (c <- ClickFilterTypeEnum.getClickFilterTypes(cfste)) {
      if (c.getTrafficSourceEnum.getId == tse.getId)
        clickFilterTypeLookupEnum = c.getId
    }
    clickFilterTypeLookupEnum
  }

  def callRoiRulesSwitch(publisherId: String, advertiserId: String): String = {
    var result = "1"
    val list = getAdvClickFilterMap(publisherId)
    list.filter(e => e.getAms_advertiser_id.equalsIgnoreCase(advertiserId))
    if (list.nonEmpty) {
      list.foreach( e => {
        if (e.getAms_clk_fltr_type_id.equals("100"))
          result = e.getStatus_enum
      })
    }
    result
  }

  def isDefinedAdvertiserId(rotationId: String): Boolean = {
    if (rotationId != null && !rotationId.equals("")) {
      val parts = rotationId.split("-")
      if (parts.length == 4)
        return ams_map.contains(parts(0).toInt)
    }
    false
  }

  def isDefinedPublisher(publisherId: String): Boolean = {
    if (publisherId != null && publisherId.toLong != -1L)
      return true
    false
  }

  def getAdvClickFilterMap(publisherId: String): ListBuffer[PubAdvClickFilterMapInfo] = {
    var list: ListBuffer[PubAdvClickFilterMapInfo] = ListBuffer.empty[PubAdvClickFilterMapInfo]
    if (publisherId != null && !publisherId.equals("")) {
      val map = CouchbaseClient.getBucket()._2.get("amspubfilter_" + publisherId, classOf[JsonDocument])
      for (i <- 0 until map.content().getNames.size()) {
        list += new Gson().fromJson(String.valueOf(map.content().get(i.toString)), classOf[PubAdvClickFilterMapInfo])
      }
    }
    list
  }

  def getReasonCode(action: String, rotationId: String, publisherId: String, campaignId: String, rt_rule_flag: Long, nrt_rule_flag: Long) : String = {
    var rsn_cd = ReasonCodeEnum.REASON_CODE0.getReasonCode
    var config_flag = 0
    val campaign_sts = getcampaignStatus(campaignId)
    val progPubMapStatus = getProgMapStatsu(publisherId, rotationId)
    val publisherStatus = getPublisherStatus(publisherId)
    val res = getPrgrmIdAdvrtsrIdFromAMSClick(rotationId)
    val filter_yn_ind = getFilter_Yn_Ind(rt_rule_flag, nrt_rule_flag, action)
    if (!res.isEmpty) {
      config_flag = res(1) & 1
    }
    if (action.equalsIgnoreCase("click") && ams_flag_map("google_fltr_do_flag") == 1)
      rsn_cd = ReasonCodeEnum.REASON_CODE10.getReasonCode
    else if (publisherId == null || publisherId.equalsIgnoreCase("") || publisherId.equalsIgnoreCase("999"))
      rsn_cd = ReasonCodeEnum.REASON_CODE3.getReasonCode
    else if (campaign_sts == null || campaign_sts.equalsIgnoreCase("2") || campaign_sts.equalsIgnoreCase(""))
      rsn_cd = ReasonCodeEnum.REASON_CODE7.getReasonCode
    else if(action.equalsIgnoreCase("click") && ams_flag_map("ams_fltr_roi_value").equals(1))
      rsn_cd = ReasonCodeEnum.REASON_CODE8.getReasonCode
    else if (progPubMapStatus == null || progPubMapStatus.equals(""))
      rsn_cd = ReasonCodeEnum.REASON_CODE2.getReasonCode
    else if (publisherStatus == null || !publisherStatus.equalsIgnoreCase("1"))
      rsn_cd = ReasonCodeEnum.REASON_CODE4.getReasonCode
    else if (progPubMapStatus != null && !progPubMapStatus.equalsIgnoreCase("1"))
      rsn_cd = ReasonCodeEnum.REASON_CODE5.getReasonCode
    else if (config_flag == 1 && filter_yn_ind == 1)
      rsn_cd = ReasonCodeEnum.REASON_CODE6.getReasonCode
    else
      rsn_cd = ReasonCodeEnum.REASON_CODE0.getReasonCode
    rsn_cd
  }

  def getPublisherStatus(publisherId: String): String = {
    var publisherInfo = new PublisherInfo
    if (publisherId != null && !publisherId.equals("")) {
      val map = CouchbaseClient.getBucket()._2.get("publisher_" + publisherId, classOf[JsonDocument])
      if (map == null)
        return ""
      publisherInfo = new Gson().fromJson(String.valueOf(map.content()), classOf[PublisherInfo])
    }
    publisherInfo.getApplication_status_enum
  }

  def getProgMapStatsu(publisherId: String, rotationId: String): String = {
    var progPubMapInfo = new ProgPubMapInfo
    val res = getPrgrmIdAdvrtsrIdFromAMSClick(rotationId)
    var programId = -1
    if (!res.isEmpty) {
      programId = res(0)
    }
    if (publisherId != null && !publisherId.equals("") && programId != -1) {
      val map = CouchbaseClient.getBucket()._2.get("ppm_" + publisherId + "_" + programId , classOf[JsonDocument])
      if (map == null || map.content().getNames.size() == 0)
        return ""
      progPubMapInfo = new Gson().fromJson(String.valueOf(map.content()), classOf[ProgPubMapInfo])
    }
    progPubMapInfo.getStatus_enum
  }

  def getcampaignStatus(campaignId: String): String = {
    var campaign_sts = new PublisherCampaignInfo
    if (campaignId != null && !campaignId.equals("")) {
      val map = CouchbaseClient.getBucket()._2.get("pubcmpn_" + campaignId, classOf[JsonDocument])
      if (map == null || map.content().getNames.size() == 0)
        return ""
      campaign_sts = new Gson().fromJson(String.valueOf(map.content().get("0")), classOf[PublisherCampaignInfo])
    }
    campaign_sts.getStatus_enum
  }


  object CouchbaseClient {

    @transient private lazy val properties = {
      val properties = new Properties
      properties.load(getClass.getClassLoader.getResourceAsStream("couchbase.properties"))
      properties
    }

    @transient private lazy val dataSource: String = properties.getProperty("chocolate.corp.couchbase.dataSource.epnnrt")

    @transient private lazy val factory =
      com.ebay.dukes.builder.GenericCacheFactoryBuilder.newBuilder()
        .cache(dataSource)
        .dbEnv(properties.getProperty("chocolate.corp.couchbase.dbEnv"))
        .deploymentSlot(properties.getProperty("chocolate.corp.couchbase.deploymentSlot"))
        .dnsRegion(properties.getProperty("chocolate.corp.couchbase.dnsRegion"))
        .pool(properties.getProperty("chocolate.corp.couchbase.pool"))
        .poolType(properties.getProperty("chocolate.corp.couchbase.poolType"))
        .appName(properties.getProperty("chocolate.corp.couchbase.appName"))
        .build()

    @transient var getBucketFunc: () => (Option[CacheClient], Bucket) = getBucket

    /**
      * get bucket
      */
    def getBucket(): (Option[CacheClient], Bucket) = {
      val cacheClient: CacheClient = factory.getClient(dataSource)
      val baseClient: BaseDelegatingCacheClient = cacheClient.asInstanceOf[BaseDelegatingCacheClient]
      val cbCacheClient: Couchbase2CacheClient = baseClient.getCacheClient.asInstanceOf[Couchbase2CacheClient]
      (Option(cacheClient), cbCacheClient.getCouchbaseClient)
    }

    /**
      * Close corp couchbase connection.
      */
    def close(): Unit = {
      try {
        factory.shutdown()
      } catch {
        case e: Exception => {
          logger.error("Corp Couchbase bucket close error.", e)
          throw e
        }
      }
    }

  }



}
