package com.ebay.traffic.chocolate.sparknrt.epnnrt

import java.net.{MalformedURLException, URL, URLDecoder}
import java.text.SimpleDateFormat
import java.util.Properties

import com.couchbase.client.java.document.{JsonArrayDocument, JsonDocument}
import com.ebay.app.raptor.chocolate.avro.ChannelType
import com.ebay.app.raptor.chocolate.common.ShortSnapshotId
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import com.google.gson.Gson
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.slf4j.LoggerFactory
import rx.Observable
import rx.functions.Func1

import scala.collection.immutable.HashMap
import scala.collection.mutable.ListBuffer

class EpnNrtCommon(params: Parameter, df: DataFrame) extends Serializable {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  val cbData = asyncCouchbaseGet(df)

  /**
    * The hadoop conf
    */
  @transient lazy val hadoopConf = {
    new Configuration()
  }

  /**
    * The file system
    */
  @transient lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  var properties: Properties = {
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

  lazy val ams_map: HashMap[Int, Array[String]] = {
    val value = Array(Array("2", "1"), Array("6", "1"), Array("4", "1"), Array("10", "1"), Array("16", "1"), Array("9", "1"), Array("5", "1"),
      Array("15", "1"), Array("3", "1"), Array("14", "1"), Array("17", "2"), Array("12", "1"), Array("11", "1"), Array("8", "1"), Array("13", "1"),
      Array("1", "1"), Array("7", "1"))
    val key = Array(5282, 4686, 705, 709, 1346, 3422, 1553, 710, 5221, 5222, 8971, 724, 707, 3423, 1185, 711, 706)
    var map = new HashMap[Int, Array[String]]

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

  /*var ams_flag_map : mutable.HashMap[String, Int] = {
    var map = new mutable.HashMap[String, Int]
    map += ("google_fltr_do_flag" -> 0)
    map += ("ams_fltr_roi_value" -> 0)
    map += ("traffic_source_code" -> 0)
    map
  }*/

  @transient lazy val config_flag_map : HashMap[Int, Int] = {
    var map = new HashMap[Int, Int]
    map += (1 -> 3)
    map += (2 -> 0)
    map
  }

 /* @transient lazy val drop_columns: Seq[String] = {
    val drop_list = Seq("snapshot_id", "timestamp", "publisher_id", "campaign_id", "request_headers",
    "uri", "response_headers", "rt_rule_flags", "nrt_rule_flags", "channel_action", "channel_type",
    "http_method", "snid", "is_tracked")
    drop_list
  }*/


  // all udf
  val snapshotIdUdf = udf((snapshotId: String) => {
    new ShortSnapshotId(snapshotId.toLong).getRepresentation.toString
  })

  // val getRoverChannelIdUdf = udf((uri: String) => getRoverUriInfo(uri, 4))
  val getRoverUriInfoUdf = udf((uri: String, index: Int) => getRoverUriInfo(uri, index))

  val getGUIDUdf = udf((requestHeader: String, responseHeader:String, guid: String) => getGUIDFromCookie(requestHeader, responseHeader, guid))
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
  val get_rule_flag_udf = udf((ruleFlag: Long, index: Int) => getRuleFlag(ruleFlag, index))
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
  val get_roi_rule_value_udf = udf((uri: String, publisherId: String, requestHeader: String, google_fltr_do_flag: Int, traffic_source_code: Int) => getRoiRuleValue(getRoverUriInfo(uri, 3), publisherId, getValueFromRequest(requestHeader, "Referer"), google_fltr_do_flag, traffic_source_code)._1)
  val get_roi_fltr_yn_ind_udf = udf((uri: String, publisherId: String, requestHeader: String, google_fltr_do_flag: Int, traffic_source_code: Int) => getRoiRuleValue(getRoverUriInfo(uri, 3), publisherId, getValueFromRequest(requestHeader, "Referer"), google_fltr_do_flag, traffic_source_code)._2)
  val get_ams_clk_fltr_type_id_udf = udf((publisherId: String) => getclickFilterTypeId(publisherId))
  val get_click_reason_code_udf = udf((uri: String, publisherId: String, campaignId: String, rt_rule_flag: Long, nrt_rule_flag: Long, ams_fltr_roi_value: Int) => getReasonCode("click", getRoverUriInfo(uri, 3), publisherId, campaignId, rt_rule_flag, nrt_rule_flag, ams_fltr_roi_value))
  val get_impression_reason_code_udf = udf((uri: String, publisherId: String, campaignId: String, rt_rule_flag: Long, nrt_rule_flag: Long, ams_fltr_roi_value: Int) => getReasonCode("impression", getRoverUriInfo(uri, 3), publisherId, campaignId, rt_rule_flag, nrt_rule_flag, ams_fltr_roi_value))
  val get_google_fltr_do_flag_udf = udf((requestHeader: String, publisherId: String) => getGoogleFltrDoFlag(getValueFromRequest(requestHeader, "Referer"), publisherId))


  def getDateTimeFromTimestamp(timestamp: Long): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    df.format(timestamp)
  }

  def getRefererHost(requestHeaders: String): String = {
    val referer = getValueFromRequest(requestHeaders, "Referer")
    if (referer != null && !referer.equals("")) {
      try {
        return new URL(referer).getHost()
      } catch {
        case e: MalformedURLException => {
          logger.error("Malformed URL for referer host: " + referer)
        }
      }
    }
    ""
  }

  def getFFValue(uri: String, index: String): String = {
    val key = "ff" + index
    getQueryParam(uri, key)
  }

  def getRoverUriInfo(uri: String, index: Int): String = {
    val path = new URL(uri).getPath()
    if (path != null && path != "" && index >= 0 && index <= 4) {
      val pathArray = path.split("/")
      if (pathArray.length == 5)
        return pathArray(index)
    }
    ""
  }

  def getGUIDFromCookie(requestHeader: String, response_headers: String, guid: String) : String = {
    var cookie = ""
    if (response_headers != null) {
      cookie = getValueFromRequest(response_headers, "Set-Cookie")
      if (cookie.equalsIgnoreCase(""))
        cookie = getValueFromRequest(response_headers, "Cookie")
    } else if(requestHeader != null) {
      cookie = getValueFromRequest(requestHeader, "Cookie")
      if (cookie.equalsIgnoreCase(""))
        cookie = getValueFromRequest(response_headers, "Set-Cookie")
    }
    try {
      if (cookie != null && !cookie.equals("")) {
        val index = cookie.indexOf(guid)
        if (index != -1)
          return cookie.substring(index + 6, index + 38)
      }
    } catch {
      case e: StringIndexOutOfBoundsException => {
        logger.error("Error in get GUID from cookie " + cookie + e)
        return ""
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
      for (i <- params.indices) {
        try {
          val array = params(i).split("=")
          val key = array(0)
          if (key.equalsIgnoreCase(param) && array.size == 2)
            return URLDecoder.decode(array(1), "UTF-8")
          else
            ""
        } catch {
          case e: ArrayIndexOutOfBoundsException => {
            logger.error("Error query parameters " + uri + e)
            return ""
          }
        }
      }

    }
    ""
  }

  def getPrgrmIdAdvrtsrIdFromAMSClick(rotationId: String): Array[String] = {
    val empty = Array("","")
    if (rotationId == null || rotationId.equals(""))
      return empty
    val parts = rotationId.split("-")
    try {
      if (parts.length == 4)
        return ams_map(parts(0).toInt)
    } catch {
      case e: NoSuchElementException => {
        logger.error("Key " + parts(0) +  " not found in the ams_map " + e)
        return empty
      }
      case e: NumberFormatException => {
        logger.error("RotationId " + rotationId +  " is not accepted " + e)
        return empty
      }
    }
    empty
  }

  def getRuleFlag(flag: Long, index: Int): Int = {
    if ((flag & 1L << index) == (1L << index))
      return 1
    0
  }

  def getCountryLocaleFromHeader(requestHeader: String): String = {
    var accept = getValueFromRequest(requestHeader, "accept-language")
    try {
      if (accept != null && !accept.equals(""))
        accept = accept.split(",")(0)
      if (accept != null && !accept.equals("") && accept.contains("-"))
        accept = accept.split("-")(1)
    } catch {
      case e: ArrayIndexOutOfBoundsException => {
        logger.error("Error accept-language " + accept + e)
        return ""
      }
    }
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
      val res = getRuleFlag(flag, 10)
      if (res == 0)
        return 0
      else {
       // ams_flag_map("traffic_source_code") = 2
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


  //only check filter rule, no check on flag rule
  def getFilter_Yn_Ind(rt_rule_flag: Long, nrt_rule_flag: Long, action: String): Int = {
    if (action.equalsIgnoreCase("impression")) {
      return getRuleFlag(rt_rule_flag, 11) | getRuleFlag(rt_rule_flag, 1) |
        getRuleFlag(rt_rule_flag, 10) | getRuleFlag(rt_rule_flag, 5)
    }
    if(action.equalsIgnoreCase("click")) {
      return getRuleFlag(rt_rule_flag, 11) | getRuleFlag(rt_rule_flag, 1) |
        getRuleFlag(rt_rule_flag, 10) | getRuleFlag(rt_rule_flag, 5) |
        getRuleFlag(nrt_rule_flag, 2) | getRuleFlag(nrt_rule_flag, 3) | getRuleFlag(nrt_rule_flag, 5) |
        getRuleFlag(nrt_rule_flag, 6) | getRuleFlag(nrt_rule_flag, 4)
    }
    0
  }

  def getPageIdByLandingPage(responseHeader: String): String = {
    val location = getValueFromRequest(responseHeader, "Location")
    if (location == null || location.equals(""))
      return ""
    val splits = location.trim.split("/")
    try {
      if (splits(2).startsWith("cgi6") && landing_page_pageId_map.contains("http://" + splits(2) + "/ws/eBayISAPI.dll?ViewStoreV4&name=")) {
        return landing_page_pageId_map("http://" + splits(2) + "/ws/eBayISAPI.dll?ViewStoreV4&name=")
      } else if (splits(2).startsWith("cgi") && landing_page_pageId_map.contains("http://" + splits(2) + "/ws/eBayISAPI.dll?ViewItem")) {
        return landing_page_pageId_map("http://" + splits(2) + "/ws/eBayISAPI.dll?ViewItem")
      }
      if (splits.length == 3) {
        if (landing_page_pageId_map.contains("http://" + splits(2) + "/")){
          return landing_page_pageId_map("http://" + splits(2) + "/")
        }
      } else if (splits.length > 3) {
        if (landing_page_pageId_map.contains("http://" + splits(2) + "/" + splits(3) + "/")) {
          return landing_page_pageId_map("http://" + splits(2) + "/" + splits(3) + "/")
        }
      }
    } catch {
      case e: ArrayIndexOutOfBoundsException => {
        logger.error("Error while getting PageId for location = " + location + " in the request", e)
        return ""
      }
    }
    ""
  }

  def getclickFilterTypeId(publisherId: String): String = {
    var clickFilterTypeId = "3"
    val list = getAdvClickFilterMap(publisherId)
    list.foreach(e => {
      if (e.getStatus_enum != null) {
        if (e.getStatus_enum.equals("1") || e.getStatus_enum.equals("2"))
          if (e.getAms_clk_fltr_type_id != "0")
            clickFilterTypeId = e.getAms_clk_fltr_type_id
      }
    })
    clickFilterTypeId
  }

  def getRoiRuleValue(rotationId: String, publisherId: String, referer_domain: String, google_fltr_do_flag: Int, traffic_source_code: Int): (Int, Int) = {
    var temp_roi_values = 0
    var roiRuleValues = 0
    var amsFilterRoiValue = 0
    var roi_fltr_yn_ind = 0

    if (isDefinedPublisher(publisherId) && isDefinedAdvertiserId(rotationId)) {
      if(callRoiRulesSwitch(publisherId, getPrgrmIdAdvrtsrIdFromAMSClick(rotationId)(1)).equals("2")) {
        val roiRuleList = lookupAdvClickFilterMapAndROI(publisherId, getPrgrmIdAdvrtsrIdFromAMSClick(rotationId)(1), traffic_source_code)
        roiRuleList(0).setRule_result(callRoiSdkRule(roiRuleList(0).getIs_rule_enable, roiRuleList(0).getIs_pblshr_advsr_enable_rule, 0))
        roiRuleList(1).setRule_result(callRoiEbayReferrerRule(roiRuleList(1).getIs_rule_enable, roiRuleList(1).getIs_pblshr_advsr_enable_rule, 0))
        roiRuleList(2).setRule_result(callRoiNqBlacklistRule(roiRuleList(2).getIs_rule_enable, roiRuleList(2).getIs_pblshr_advsr_enable_rule, 0))
        roiRuleList(3).setRule_result(callRoiNqWhitelistRule(publisherId, roiRuleList(2).getIs_rule_enable, roiRuleList(2).getIs_pblshr_advsr_enable_rule, referer_domain, traffic_source_code))
        roiRuleList(4).setRule_result(callRoiMissingReferrerUrlRule(roiRuleList(4).getIs_rule_enable, roiRuleList(4).getIs_pblshr_advsr_enable_rule, referer_domain))
        roiRuleList(5).setRule_result(callRoiNotRegisteredRule(publisherId, roiRuleList(5).getIs_rule_enable, roiRuleList(5).getIs_pblshr_advsr_enable_rule, referer_domain, traffic_source_code))

        for (i <- roiRuleList.indices) {
          temp_roi_values = temp_roi_values + (roiRuleList(i).getRule_result << i)
        }
      }
    }
 //   ams_flag_map("google_fltr_do_flag") = lookupRefererDomain(referer_domain, isDefinedPublisher(publisherId), publisherId)
 //   roiRuleValues = temp_roi_values + ams_flag_map("google_fltr_do_flag") << 6
    roiRuleValues = temp_roi_values + google_fltr_do_flag << 6
    if (roiRuleValues != 0) {
      amsFilterRoiValue = 1
      roi_fltr_yn_ind = 1
    }
   // ams_flag_map("ams_fltr_roi_value") = amsFilterRoiValue
    (roiRuleValues, roi_fltr_yn_ind)
  }

  def getGoogleFltrDoFlag(referer_domain: String, publisherId: String): Int = {
    lookupRefererDomain(referer_domain, isDefinedAdvertiserId(publisherId), publisherId)
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


  def callRoiNqWhitelistRule(publisherId: String, is_rule_enable: Int, is_pblshr_advsr_enable_rule: Int, referer_domain: String, traffic_source_code: Int): Int = {
    callRoiRuleCommon(publisherId, is_rule_enable, is_pblshr_advsr_enable_rule, referer_domain, "NETWORK_QUALITY_WHITELIST", traffic_source_code)
  }

  def callRoiNotRegisteredRule(publisherId: String, is_rule_enable: Int, is_pblshr_advsr_enable_rule: Int, referer_domain: String, traffic_source_code: Int): Int = {
    callRoiRuleCommon(publisherId, is_rule_enable, is_pblshr_advsr_enable_rule, referer_domain, "APP_REGISTERED", traffic_source_code)
  }

  def callRoiRuleCommon(publisherId: String, is_rule_enable: Int, is_pblshr_advsr_enable_rule: Int, referer_domain: String, dimensionLookupConstants: String, traffic_source_code: Int): Int = {
    var result = 0
    if (is_rule_enable == 1 && is_pblshr_advsr_enable_rule == 1) {
    //  if (ams_flag_map("traffic_source_code") == 0 || ams_flag_map("traffic_source_code") == 2) {
      if (traffic_source_code == 0 || traffic_source_code == 2) {
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
    var list = cbData._5.getOrElse(publisherId, ListBuffer.empty[PubDomainInfo])
    if (roi_rule.equals("NETWORK_QUALITY_WHITELIST"))
      list = list.filterNot(e => !e.getDomain_status_enum.equals("1") || !e.getWhitelist_status_enum.equals("1") && e.getIs_registered.equals("0"))
    list
  }

  def lookupAdvClickFilterMapAndROI(publisherId: String, advertiserId: String, traffic_source_code: Int): ListBuffer[RoiRule] = {
    val roiList = getRoiRuleList(traffic_source_code)
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

  def getRoiRuleList(traffic_source_code: Int): ListBuffer[RoiRule] = {
    var list: ListBuffer[RoiRule] = ListBuffer.empty[RoiRule]
    for (i <- 0 until 6) {
      var value = 0
      val rr = new RoiRule
      value = clickFilterTypeLookup(i + 1, traffic_source_code)
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
    try {
      if (rotationId != null && !rotationId.equals("")) {
        val parts = rotationId.split("-")
        if (parts.length == 4)
          return ams_map.contains(parts(0).toInt)
      }
    } catch {
      case e: NoSuchElementException => {
        logger.error("RotationId " + rotationId +  " is not accepted " + e)
        return false
      }
      case e: NumberFormatException => {
        logger.error("RotationId " + rotationId +  " is not accepted " + e)
        return false
      }
    }
    false
  }

  def isDefinedPublisher(publisherId: String): Boolean = {
    if (publisherId != null && publisherId.toLong != -1L)
      return true
    false
  }

  def getAdvClickFilterMap(publisherId: String): ListBuffer[PubAdvClickFilterMapInfo] = {
    cbData._4.getOrElse(publisherId, ListBuffer.empty[PubAdvClickFilterMapInfo])
  }

  def getReasonCode(action: String, rotationId: String, publisherId: String, campaignId: String, rt_rule_flag: Long, nrt_rule_flag: Long, ams_fltr_roi_value: Int) : String = {
    var rsn_cd = ReasonCodeEnum.REASON_CODE0.getReasonCode
    var config_flag = 0
    val campaign_sts = getcampaignStatus(campaignId)
    val progPubMapStatus = getProgMapStatus(publisherId, rotationId)
    val publisherStatus = getPublisherStatus(publisherId)
    val res = getPrgrmIdAdvrtsrIdFromAMSClick(rotationId)
    val filter_yn_ind = getFilter_Yn_Ind(rt_rule_flag, nrt_rule_flag, action)
    if (!res(1).equals("")) {
      config_flag = res(1).toInt & 1
    }
    if (action.equalsIgnoreCase("click") && ams_fltr_roi_value == 1)
      rsn_cd = ReasonCodeEnum.REASON_CODE10.getReasonCode
    else if (publisherId == null || publisherId.equalsIgnoreCase("") || publisherId.equalsIgnoreCase("999"))
      rsn_cd = ReasonCodeEnum.REASON_CODE3.getReasonCode
    else if (campaign_sts == null || campaign_sts.equalsIgnoreCase("2") || campaign_sts.equalsIgnoreCase(""))
      rsn_cd = ReasonCodeEnum.REASON_CODE7.getReasonCode
    else if(action.equalsIgnoreCase("click") && ams_fltr_roi_value == 1)
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
    cbData._1.getOrElse(publisherId, "")
  }

  def getProgMapStatus(publisherId: String, rotationId: String): String = {
    val res = getPrgrmIdAdvrtsrIdFromAMSClick(rotationId)
    var programId = ""
    if (!res(0).equals(""))
      programId = res(0)
    cbData._3.getOrElse(publisherId + "_" + programId, "")
  }

  def getcampaignStatus(campaignId: String): String = {
   cbData._2.getOrElse(campaignId, "")
  }

  // async couchbase get

  def asyncCouchbaseGet(df: DataFrame): (HashMap[String, String], HashMap[String, String],
    HashMap[String, String], HashMap[String, ListBuffer[PubAdvClickFilterMapInfo]],
    HashMap[String, ListBuffer[PubDomainInfo]]) = {
    logger.info("Set Broadcast couchbase data...")
    logger.info("DF columns " + df.count())
    val test = df.select("publisher_id", "campaign_id", "uri").collect()
    var publisher_list = new Array[String](test.length)
    var campaign_list = new Array[String](test.length)
    val rotation_list = new Array[String](test.length)
    var progmap_list = new Array[String](test.length)
    var publisher_map = new HashMap[String, String]
    var campaign_map = new HashMap[String, String]
    var prog_map = new HashMap[String, String]
    var clickFilter_map = new HashMap[String, ListBuffer[PubAdvClickFilterMapInfo]]
    var pubDomain_map = new HashMap[String, ListBuffer[PubDomainInfo]]


    for (i <- test.indices) {
      publisher_list(i) = String.valueOf(test(i).get(0))
      campaign_list(i) = String.valueOf(test(i).get(1))
      rotation_list(i) = getRoverUriInfo(String.valueOf(test(i).get(2)), 3)
    }


    for (i <- publisher_list.indices) {
      val res = getPrgrmIdAdvrtsrIdFromAMSClick(rotation_list(i))
      var programId = ""
      if (!res(0).equals(""))
        programId = res(0)
      progmap_list(i) = publisher_list(i) + "_" + programId
    }

    logger.info("publisher_list count is " + publisher_list.length)
    logger.info("campaign_list count is " + campaign_list.length)
    logger.info("progmap_list count is " + progmap_list.length)

    publisher_list = publisher_list.distinct
    campaign_list = campaign_list.distinct
    progmap_list = progmap_list.distinct

    logger.info("after publisher_list count is " + publisher_list.length)
    logger.info("after campaign_list count is " + campaign_list.length)
    logger.info("after progmap_list count is " + progmap_list.length)

    logger.info("Begin load data from couchbase")
    publisher_map = batchGetPublisherStatus(publisher_list)
    campaign_map = batchGetCampaignStatus(campaign_list)
    prog_map = batchGetProgMapStatus(progmap_list)
    clickFilter_map = batchGetAdvClickFilterMap(publisher_list)
    pubDomain_map = batchGetPubDomainMap(publisher_list)


    logger.info("publisher_map count is " + publisher_map.size)
    logger.info("campaign_map count is " + campaign_map.size)
    logger.info("prog_map count is " + prog_map.size)
    logger.info("clickFilter_map is " + clickFilter_map.size)
    logger.info("pubDomain_map count is " + pubDomain_map.size)
    (publisher_map, campaign_map, prog_map, clickFilter_map, pubDomain_map)
  }



  def batchGetPublisherStatus(list: Array[String]): HashMap[String, String] = {
    logger.info("loading publisher status...")
    var res = new HashMap[String, String]
    val (cacheClient, bucket) = CouchbaseClient.getBucketFunc()
    try {
      val jsonDocuments = Observable
        .from(list)
        .flatMap(new Func1[String, Observable[JsonDocument]]() {
          override def call(key: String): Observable[JsonDocument] = {
            bucket.async.get("EPN_publisher_" + key, classOf[JsonDocument])
          }
        }).toList.toBlocking.single
      logger.info("publisher status count " + jsonDocuments.size())
      for (i <- 0 until jsonDocuments.size()) {
        val publisherInfo = new Gson().fromJson(String.valueOf(jsonDocuments.get(i).content()), classOf[PublisherInfo])
        if (publisherInfo != null)
          res = res + (publisherInfo.getAms_publisher_id -> publisherInfo.getApplication_status_enum)
        else
          res = res + (publisherInfo.getAms_publisher_id -> "")
      }
      if (jsonDocuments.size() != list.length) {
        for (i <- list.indices) {
          if (!res.contains(list(i)))
            res = res + (list(i) -> "")
        }
      }
    } catch {
      case e: Exception => {
        logger.error("Corp Couchbase error while getting publisher status " +  e)
      }
    }
    CouchbaseClient.returnClient(cacheClient)
    res
  }

  def batchGetCampaignStatus(list: Array[String]): HashMap[String, String] = {
    logger.info("loading campaign status...")
    var res = new HashMap[String, String]
    val (cacheClient, bucket) = CouchbaseClient.getBucketFunc()
    try {
      val jsonDocuments = Observable
        .from(list)
        .flatMap(new Func1[String, Observable[JsonDocument]]() {
          override def call(key: String): Observable[JsonDocument] = {
            bucket.async.get("EPN_pubcmpn_" + key, classOf[JsonDocument])
          }
        }).toList.toBlocking.single()

      logger.info("campaign status count " + jsonDocuments.size())
      for (i <- 0 until jsonDocuments.size()) {
        val campaign_sts = new Gson().fromJson(String.valueOf(jsonDocuments.get(i).content()), classOf[PublisherCampaignInfo])
        if (campaign_sts != null)
          res = res + (campaign_sts.getAms_publisher_campaign_id -> campaign_sts.getStatus_enum)
        else
          res = res + (campaign_sts.getAms_publisher_campaign_id -> "")
        if (jsonDocuments.size() != list.length) {
          for (i <- list.indices) {
            if (!res.contains(list(i)))
              res = res + (list(i) -> "")
          }
        }
      }
    } catch {
      case e: Exception => {
        logger.error("Corp Couchbase error while getting campaign status " +  e)
      }
    }
    CouchbaseClient.returnClient(cacheClient)
    res
  }

  def batchGetProgMapStatus(list: Array[String]): HashMap[String, String] = {
    logger.info("loading progmap status...")
    var res = new HashMap[String, String]
    val (cacheClient, bucket) = CouchbaseClient.getBucketFunc()
    try {
      val jsonDocuments = Observable
        .from(list)
        .flatMap(new Func1[String, Observable[JsonDocument]]() {
          override def call(key: String): Observable[JsonDocument] = {
            bucket.async.get("EPN_ppm_" + key, classOf[JsonDocument])
          }
        }).toList.toBlocking.single()
      logger.info("progmap status count " + jsonDocuments.size())
      for (i <- 0 until jsonDocuments.size()) {
        val progPubMap = new Gson().fromJson(String.valueOf(jsonDocuments.get(i).content()), classOf[ProgPubMapInfo])
        if (progPubMap != null)
          res = res + ((progPubMap.getAms_publisher_id + "_" + progPubMap.getAms_program_id) -> progPubMap.getStatus_enum)
        else
          res = res + ((progPubMap.getAms_publisher_id + "_" + progPubMap.getAms_program_id) -> "")
        if (jsonDocuments.size() != list.length) {
          for (i <- list.indices) {
            if (!res.contains(list(i)))
              res = res + (list(i) -> "")
          }
        }
      }
    } catch {
      case e: Exception => {
        logger.error("Corp Couchbase error while getting progmap status " +  e)
      }
    }
    CouchbaseClient.returnClient(cacheClient)
    res
  }


  def batchGetAdvClickFilterMap(list: Array[String]): HashMap[String, ListBuffer[PubAdvClickFilterMapInfo]] = {
    logger.info("loading click filter map...")
    var res = new HashMap[String, ListBuffer[PubAdvClickFilterMapInfo]]
    val (cacheClient, bucket) = CouchbaseClient.getBucketFunc()
    try {
      val jsonArrayDocuments = Observable
        .from(list)
        .flatMap(new Func1[String, Observable[JsonArrayDocument]]() {
          override def call(key: String): Observable[JsonArrayDocument] = {
            bucket.async.get("EPN_amspubfilter_" + key, classOf[JsonArrayDocument])
          }
        }).toList.toBlocking.single()
      logger.info("click filter count " + jsonArrayDocuments.size())
      for (i <- 0 until jsonArrayDocuments.size()) {
        var objectList: ListBuffer[PubAdvClickFilterMapInfo] = ListBuffer.empty[PubAdvClickFilterMapInfo]
        for (j <-0 until jsonArrayDocuments.get(i).content().size()) {
          objectList += new Gson().fromJson(String.valueOf(jsonArrayDocuments.get(i).content().get(j)), classOf[PubAdvClickFilterMapInfo])
        }
        if (objectList.nonEmpty)
          res = res + (objectList.head.getAms_publisher_id -> objectList)
      }
      if (jsonArrayDocuments.size() != list.length) {
        for (i <- list.indices) {
          if (!res.contains(list(i)))
            res = res + (list(i) -> ListBuffer.empty[PubAdvClickFilterMapInfo])
        }
      }
    } catch {
      case e: Exception => {
        logger.error("Corp Couchbase error while getting advClickFilterMap" +  e)
      }
    }
    CouchbaseClient.returnClient(cacheClient)
    res
  }

  def batchGetPubDomainMap(list: Array[String]): HashMap[String, ListBuffer[PubDomainInfo]] = {
    logger.info("loading pubdomain map...")
    var res = new HashMap[String, ListBuffer[PubDomainInfo]]
    val (cacheClient, bucket) = CouchbaseClient.getBucketFunc()
    try {
      val jsonArrayDocuments = Observable
        .from(list)
        .flatMap(new Func1[String, Observable[JsonArrayDocument]]() {
          override def call(key: String): Observable[JsonArrayDocument] = {
            bucket.async.get("EPN_amspubdomain_" + key, classOf[JsonArrayDocument])
          }
        }).toList.toBlocking.single()
      logger.info("pubdomain count " + jsonArrayDocuments.size())

      for (i <- 0 until jsonArrayDocuments.size()) {
        var objectList: ListBuffer[PubDomainInfo] = ListBuffer.empty[PubDomainInfo]
        for (j <-0 until jsonArrayDocuments.get(i).content().size()) {
          objectList += new Gson().fromJson(String.valueOf(jsonArrayDocuments.get(i).content().get(j)), classOf[PubDomainInfo])
        }
        if (objectList.nonEmpty)
          res = res + (objectList.head.getAms_publisher_id -> objectList)
      }
      if (jsonArrayDocuments.size() != list.length) {
        for (i <- list.indices) {
          if (!res.contains(list(i)))
            res = res + (list(i) -> ListBuffer.empty[PubDomainInfo])
        }
      }
    } catch {
      case e: Exception => {
        logger.error("Corp Couchbase error while getting pubDomainMap " +  e)
      }
    }
    CouchbaseClient.returnClient(cacheClient)
    res
  }
}
