package com.ebay.traffic.chocolate.sparknrt.epnnrt

import java.net.{MalformedURLException, URL, URLDecoder}
import java.text.SimpleDateFormat
import java.util.Properties

import com.couchbase.client.java.document.{JsonArrayDocument, JsonDocument}
import com.ebay.app.raptor.chocolate.avro.ChannelType
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
import scala.collection.mutable
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

  //
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

  lazy val referer_domain_map: HashMap[String, ListBuffer[String]] = {
    var map = new HashMap[String, ListBuffer[String]]
    val stream = fs.open(new Path(params.resourceDir + "/" + properties.getProperty("epnnrt.refng.pblsh")))
    def readLine = Stream.cons(stream.readLine(), Stream.continually(stream.readLine))
    readLine.takeWhile(_ != null).foreach(line => {
      val parts = line.split("\t")
      if (map.contains(parts(2))) {
        var list = map(parts(2))
        list += parts(1)
        map = map + (parts(2) -> list)
      }
      else {
        var list: mutable.ListBuffer[String] = mutable.ListBuffer[String]()
        list += parts(1)
        map = map + (parts(2) -> list)
      }
    })
    map
  }

  @transient lazy val config_flag_map : HashMap[Int, Int] = {
    var map = new HashMap[Int, Int]
    map += (1 -> 3)
    map += (2 -> 0)
    map
  }

  // val getRoverChannelIdUdf = udf((uri: String) => getRoverUriInfo(uri, 4))
  val getRoverUriInfoUdf = udf((uri: String, index: Int) => getRoverUriInfo(uri, index))

  //val getGUIDUdf = udf((requestHeader: String, responseHeader:String, guid: String) => getGUIDFromCookie(requestHeader, responseHeader, guid))
  val getValueFromRequestUdf = udf((requestHeader: String, key: String) => getValueFromRequest(requestHeader, key))
  val getUserQueryTextUdf = udf((url: String, action: String) => getUserQueryTxt(url, action))
  val getToolIdUdf = udf((url: String) => getQueryParam(url, "toolid"))
  val getCustomIdUdf = udf((url: String) => getQueryParam(url, "customid"))
  val getFFValueUdf = udf((url: String, index: String) => getFFValue(url, index))
  val getFFValueNotEmptyUdf = udf((url: String, index: String) => getFFValueNotEmpty(url, index))
  val getCtxUdf = udf((url: String) => getQueryParam(url, "ctx"))
  val getCtxCalledUdf = udf((url: String) => getQueryParam(url, "ctx_called"))
  val getcbkwUdf = udf((url: String) => getQueryParam(url, "cb_kw"))
  val getRefererHostUdf = udf((requestHeaders: String) => getRefererURLAndDomain(requestHeaders, true))
  val getDateTimeUdf = udf((timestamp: Long) => getDateTimeFromTimestamp(timestamp, "yyyy-MM-dd HH:mm:ss.SSS"))
  val getcbcatUdf = udf((url: String) => getQueryParam(url, "cb_cat"))
  val get_ams_prgrm_id_Udf = udf((uri: String) => getPrgrmIdAdvrtsrIdFromAMSClick(getRoverUriInfo(uri, 3)))
  val get_cb_ex_kw_Udf = udf((url: String) => getQueryParam(url, "cb_ex_kw"))
  val get_cb_ex_cat_Udf = udf((url: String) => getQueryParam(url, "cb_ex_cat"))
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
  val get_page_id_udf = udf((landingPage: String) => getPageIdByLandingPage(landingPage))
  val get_roi_rule_value_udf = udf((uri: String, publisherId: String, requestHeader: String, google_fltr_do_flag: Int, traffic_source_code: Int, rt_rule_flags: Int) => getRoiRuleValue(getRoverUriInfo(uri, 3), publisherId, getRefererURLAndDomain(requestHeader, true), google_fltr_do_flag, traffic_source_code, getRuleFlag(rt_rule_flags, 13))._1)
  val get_roi_fltr_yn_ind_udf = udf((uri: String, publisherId: String, requestHeader: String, google_fltr_do_flag: Int, traffic_source_code: Int, rt_rule_flags: Int) => getRoiRuleValue(getRoverUriInfo(uri, 3), publisherId, getRefererURLAndDomain(requestHeader, true), google_fltr_do_flag, traffic_source_code, getRuleFlag(rt_rule_flags, 13))._2)
  val get_ams_clk_fltr_type_id_udf = udf((publisherId: String, uri: String) => getclickFilterTypeId(publisherId, getRoverUriInfo(uri, 3)))
  val get_click_reason_code_udf = udf((uri: String, publisherId: String, campaignId: String, rt_rule_flag: Long, nrt_rule_flag: Long, ams_fltr_roi_value: Int, google_fltr_do_flag: Int) => getReasonCode("click", getRoverUriInfo(uri, 3), publisherId, campaignId, rt_rule_flag, nrt_rule_flag, ams_fltr_roi_value, google_fltr_do_flag))
  val get_impression_reason_code_udf = udf((uri: String, publisherId: String, campaignId: String, rt_rule_flag: Long, nrt_rule_flag: Long, ams_fltr_roi_value: Int, google_fltr_do_flag: Int) => getReasonCode("impression", getRoverUriInfo(uri, 3), publisherId, campaignId, rt_rule_flag, nrt_rule_flag, ams_fltr_roi_value, google_fltr_do_flag))
  val get_google_fltr_do_flag_udf = udf((requestHeader: String, publisherId: String) => getGoogleFltrDoFlag(getRefererURLAndDomain(requestHeader, true), publisherId))
  val get_lnd_page_url_name_udf = udf((responseHeader: String) => getLndPageUrlName(responseHeader))
  val get_IcepFlexFld_udf = udf((uri:String, key:String) => getIcepFlexFld(uri, key))
  val get_Geo_Trgtd_Ind_udf = udf((uri:String) => getValueFromQueryURL(uri, "isgeo"))
  val get_Pblshr_Acptd_Prgrm_Ind_udf = udf((uri:String) => getValueFromQueryURL(uri, "isprogAccepted"))
  val get_Prgrm_Excptn_List_udf = udf((uri:String) => getValueFromQueryURL(uri, "in_exp_list"))


  def getValueFromQueryURL(uri: String, key: String): String = {
    val value = getQueryParam(uri, key)
    if (value.trim.equalsIgnoreCase("1"))
      return "1"
    "0"
  }

  def getIcepFlexFld(uri: String, key: String): String = {
    val value = getQueryParam(uri, "icep_" + key)
    if (value.equalsIgnoreCase(""))
      return "0"
    "1"
  }


  def getDateTimeFromTimestamp(timestamp: Long, format: String): String = {
    val df = new SimpleDateFormat(format)
    df.format(timestamp)
  }

  def getLndPageUrlName(responseHeader: String): String = {
    val location = getValueFromRequest(responseHeader, "Location")
    if (location.equalsIgnoreCase(""))
      return ""
    val url = new URL(location)
    if (url.getHost.contains("rover.ebay.com") || url.getHost.contains("r.ebay.com"))
      return location
    else {
   //   if (url.getHost.contains(".com"))
   //     return location
    //  else {
        var res = getQueryParam(location, "mpre")
        if (res.equalsIgnoreCase(""))
          res = getQueryParam(location, "loc")
        if (res.equalsIgnoreCase(""))
          res = getQueryParam(location, "url")
        return res
      //}
    }
    ""
  }

  def getRefererURLAndDomain(requestHeaders: String, domain: Boolean): String = {
    val referer = getValueFromRequest(requestHeaders, "Referer")
    if (referer != null && !referer.equals("")) {
      try {
        val url = new URL(referer)
        if (domain) {
          return url.getHost
        } else {
          return url.getProtocol + "://" + url.getHost
        }

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

  def getFFValueNotEmpty(uri: String, index: String): String = {
    val icep_key = "icep_ff" + index
    val value = getQueryParam(uri, icep_key)
    if (!value.equalsIgnoreCase(""))
      return value
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

  def getUserQueryTxt(uri: String, action: String): String = {
    var value = getQueryParam(uri, "item")
    if (value != null && !value.equals(""))
      return value
    value = getQueryParam(uri, "uq")
    if (value != null && !value.equals(""))
      return value
    if (action.equalsIgnoreCase("impression"))
      return ""
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
      try {
        val params = new URL(uri).getQuery().split("&")
        for (i <- params.indices) {
          val array = params(i).split("=")
          val key = array(0)
          if (key.equalsIgnoreCase(param) && array.size == 2)
            return URLDecoder.decode(array(1), "UTF-8")
          else
            ""
        }
      } catch {
        case e: ArrayIndexOutOfBoundsException => {
          logger.error("Error query parameters " + uri + e)
          return ""
        }
        case e: NullPointerException => {
          logger.error("Error query parameters " + uri + e)
          return ""
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

  def getToolLvlOptn(uri: String): String = {
    val value = getQueryParam(uri, "lgeo")
    if (value != null && !value.equals(""))
      if (value.equals("1") || value.equals("0"))
        return value
    "1"
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
    if (userAgentStr == null | userAgentStr.equals(""))
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
        getRuleFlag(rt_rule_flag, 10) | getRuleFlag(rt_rule_flag, 5) | getRuleFlag(rt_rule_flag, 3)
    }
    if(action.equalsIgnoreCase("click")) {
      return getRuleFlag(rt_rule_flag, 11) | getRuleFlag(rt_rule_flag, 1) |
        getRuleFlag(rt_rule_flag, 10) | getRuleFlag(rt_rule_flag, 5) | getRuleFlag(rt_rule_flag, 3) |
        getRuleFlag(nrt_rule_flag, 1) | getRuleFlag(nrt_rule_flag, 2) | getRuleFlag(nrt_rule_flag, 4) |
        getRuleFlag(nrt_rule_flag, 5) | getRuleFlag(nrt_rule_flag, 3) | getRuleFlag(nrt_rule_flag, 6)
    }
    0
  }

  def getPageIdByLandingPage(landingPage: String): String = {
    if (landingPage == null || landingPage.equals(""))
      return ""
    val splits = landingPage.trim.split("/")
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
        logger.error("Error while getting PageId for location = " + landingPage + " in the request", e)
        return ""
      }
    }
    ""
  }

  def getclickFilterTypeId(publisherId: String, rotationId: String): String = {
    var clickFilterTypeId = "3"
    val advrtsrId = getPrgrmIdAdvrtsrIdFromAMSClick(rotationId)(1)
    var list = getAdvClickFilterMap(publisherId)
    list = list.filter(e => e.getAms_advertiser_id.equalsIgnoreCase(advrtsrId))
    list.foreach(e => {
      if (e.getStatus_enum != null) {
        if (e.getStatus_enum.equals("1") || e.getStatus_enum.equals("2"))
          if (e.getAms_clk_fltr_type_id != "0")
            clickFilterTypeId = e.getAms_clk_fltr_type_id
      }
    })
    clickFilterTypeId
  }

  def getRoiRuleValue(rotationId: String, publisherId: String, referer_domain: String, google_fltr_do_flag: Int, traffic_source_code: Int, rt_rule_9: Int): (Int, Int) = {
    var temp_roi_values = 0
    var roiRuleValues = 0
  //  var amsFilterRoiValue = 0
    var roi_fltr_yn_ind = 0

    if (isDefinedPublisher(publisherId) && isDefinedAdvertiserId(rotationId)) {
      if(callRoiRulesSwitch(publisherId, getPrgrmIdAdvrtsrIdFromAMSClick(rotationId)(1)).equals("2")) {
        val roiRuleList = lookupAdvClickFilterMapAndROI(publisherId, getPrgrmIdAdvrtsrIdFromAMSClick(rotationId)(1), traffic_source_code)
        roiRuleList(0).setRule_result(callRoiSdkRule(roiRuleList(0).getIs_rule_enable, roiRuleList(0).getIs_pblshr_advsr_enable_rule, 0))
        roiRuleList(1).setRule_result(callRoiEbayReferrerRule(roiRuleList(1).getIs_rule_enable, roiRuleList(1).getIs_pblshr_advsr_enable_rule, rt_rule_9))
        roiRuleList(2).setRule_result(callRoiNqBlacklistRule(roiRuleList(2).getIs_rule_enable, roiRuleList(2).getIs_pblshr_advsr_enable_rule, 0))
        roiRuleList(3).setRule_result(callRoiNqWhitelistRule(publisherId, roiRuleList(3).getIs_rule_enable, roiRuleList(3).getIs_pblshr_advsr_enable_rule, referer_domain, traffic_source_code))
        roiRuleList(4).setRule_result(callRoiMissingReferrerUrlRule(roiRuleList(4).getIs_rule_enable, roiRuleList(4).getIs_pblshr_advsr_enable_rule, referer_domain))
        roiRuleList(5).setRule_result(callRoiNotRegisteredRule(publisherId, roiRuleList(5).getIs_rule_enable, roiRuleList(5).getIs_pblshr_advsr_enable_rule, referer_domain, traffic_source_code))

        for (i <- roiRuleList.indices) {
          temp_roi_values = temp_roi_values + (roiRuleList(i).getRule_result << i)
        }
      }
    }
    roiRuleValues = temp_roi_values + (google_fltr_do_flag << 6)
    if (roiRuleValues != 0) {
      roi_fltr_yn_ind = 1
    }
    (roiRuleValues, roi_fltr_yn_ind)
  }

  def getGoogleFltrDoFlag(referer_domain: String, publisherId: String): Int = {
    lookupRefererDomain(referer_domain, isDefinedPublisher(publisherId), publisherId)
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
    if (roi_rule.equals("NETWORK_QUALITY_WHITELIST") && list.nonEmpty)
      list = list.filterNot(e => (e.getDomain_status_enum != null && !e.getDomain_status_enum.equals("1")) ||
        (e.getWhitelist_status_enum != null && !e.getWhitelist_status_enum.equals("1")) && (e.getIs_registered != null && e.getIs_registered.equals("0")))
    list
  }

  def lookupAdvClickFilterMapAndROI(publisherId: String, advertiserId: String, traffic_source_code: Int): ListBuffer[RoiRule] = {
    val roiList = getRoiRuleList(traffic_source_code)
    var clickFilterMapList = getAdvClickFilterMap(publisherId)
    clickFilterMapList = clickFilterMapList.filter(e => e.getAms_advertiser_id.equalsIgnoreCase(advertiserId))
    clickFilterMapList.foreach(e => {
      var loop = true
      roiList.foreach(k => {
        if (k.getAms_clk_fltr_type_id == e.getAms_clk_fltr_type_id.toInt && loop) {
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
        rr.setAms_clk_fltr_type_id(value)
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
    var list = getAdvClickFilterMap(publisherId)
    list = list.filter(e => e.getAms_advertiser_id.equalsIgnoreCase(advertiserId))
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

  def getReasonCode(action: String, rotationId: String, publisherId: String, campaignId: String, rt_rule_flag: Long, nrt_rule_flag: Long, ams_fltr_roi_value: Int, google_fltr_do_flag: Int) : String = {
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
    if (action.equalsIgnoreCase("click") && google_fltr_do_flag == 1)
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


    publisher_list = publisher_list.distinct
    campaign_list = campaign_list.distinct
    progmap_list = progmap_list.distinct


    publisher_map = batchGetPublisherStatus(publisher_list)
    campaign_map = batchGetCampaignStatus(campaign_list)
    prog_map = batchGetProgMapStatus(progmap_list)
    clickFilter_map = batchGetAdvClickFilterMap(publisher_list)
    pubDomain_map = batchGetPubDomainMap(publisher_list)

    (publisher_map, campaign_map, prog_map, clickFilter_map, pubDomain_map)
  }



  def batchGetPublisherStatus(list: Array[String]): HashMap[String, String] = {
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
