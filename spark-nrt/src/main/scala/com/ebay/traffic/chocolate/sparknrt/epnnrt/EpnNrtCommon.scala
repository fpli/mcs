package com.ebay.traffic.chocolate.sparknrt.epnnrt

import java.net.{MalformedURLException, URL}
import java.text.SimpleDateFormat
import java.util.Properties

import com.couchbase.client.java.document.{JsonArrayDocument, JsonDocument}
import com.ebay.app.raptor.chocolate.avro.ChannelType
import com.ebay.app.raptor.chocolate.common.ShortSnapshotId
import com.ebay.traffic.chocolate.sparknrt.couchbase.CorpCouchbaseClient
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import com.google.gson.Gson
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.udf
import org.slf4j.LoggerFactory

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class EpnNrtCommon(params: Parameter) extends Serializable {

  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

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

  @transient lazy val drop_columns: Seq[String] = {
    val drop_list = Seq("snapshot_id", "timestamp", "publisher_id", "campaign_id", "request_headers",
    "uri", "response_headers", "rt_rule_flags", "nrt_rule_flags", "channel_action", "channel_type",
    "http_method", "snid", "is_tracked")
    drop_list
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
      if (pathArray.length == 4)
        return pathArray(index)
    }
    ""
  }

  def getGUIDFromCookie(requestHeader: String, guid: String) : String = {
    if (requestHeader != null) {
      val cookie = getValueFromRequest(requestHeader, "Cookie")
      if (cookie != null && !cookie.equals("")) {
        val index = cookie.indexOf(guid)
        if (index != -1)
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
        val array = i.split("=")
        val key = i.split("=")(0)
        if (key.equalsIgnoreCase(param) && array.size == 2)
          return i.split("=")(1)
        else
          ""
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
    if (accept != null && !accept.equals(""))
      accept = accept.split(",")(0)
    if (accept != null && !accept.equals("") && accept.contains("-"))
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
      try {
        logger.debug("Get publisher domain for publisherId = " + publisherId + " from corp couchbase")
        val (cacheClient, bucket) = CorpCouchbaseClient.getBucketFunc()
        if (bucket.exists("EPN_amspubdomain_" + publisherId)) {
          val array = bucket.get("EPN_amspubdomain_" + publisherId, classOf[JsonArrayDocument])
          for (i <- 0 until array.content().size()) {
            list += new Gson().fromJson(String.valueOf(array.content().get(i)), classOf[PubDomainInfo])
          }
          CorpCouchbaseClient.returnClient(cacheClient)
        }
      } catch {
        case e: Exception => {
          logger.error("Corp Couchbase error while getting publisher domain for publisherId = " + publisherId, e)
          throw e
        }
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
      try {
        logger.debug("Get advClickFilterMap for publisherId = " + publisherId + " from corp couchbase")
        val (cacheClient, bucket) = CorpCouchbaseClient.getBucketFunc()
        if (bucket.exists("EPN_amspubfilter_" + publisherId)) {
          val array = bucket.get("EPN_amspubfilter_" + publisherId, classOf[JsonArrayDocument])
          for (i <- 0 until array.content().size()) {
            list += new Gson().fromJson(String.valueOf(array.content().get(i)), classOf[PubAdvClickFilterMapInfo])
          }
        }
        CorpCouchbaseClient.returnClient(cacheClient)
      } catch {
        case e: Exception => {
          logger.error("Corp Couchbase error while getting advClickFilterMap for publisherId = " + publisherId, e)
        }
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
      try {
        logger.debug("Get publisher domain for publisherId = " + publisherId + " from corp couchbase")
        val (cacheClient, bucket) = CorpCouchbaseClient.getBucketFunc()
        if (bucket.exists("EPN_publisher_" + publisherId)) {
          val map = bucket.get("EPN_publisher_" + publisherId, classOf[JsonDocument])
          publisherInfo = new Gson().fromJson(String.valueOf(map.content()), classOf[PublisherInfo])
        }
        CorpCouchbaseClient.returnClient(cacheClient)
      } catch {
        case e: Exception => {
          logger.error("Corp Couchbase error while getting publisher status for publisherId = " + publisherId, e)
          throw e
        }
      }
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
      try {
        logger.debug("Get progMap status for publisherId = " + publisherId + " from corp couchbase")
        val (cacheClient, bucket) = CorpCouchbaseClient.getBucketFunc()
        if (bucket.exists("EPN_ppm_" + publisherId + "_" + programId)) {
          val map = bucket.get("EPN_ppm_" + publisherId + "_" + programId , classOf[JsonDocument])
          progPubMapInfo = new Gson().fromJson(String.valueOf(map.content()), classOf[ProgPubMapInfo])
        }
        CorpCouchbaseClient.returnClient(cacheClient)
      } catch {
        case e: Exception => {
          logger.error("Corp Couchbase error while getting progMap status for publisherId = " + publisherId, e)
          throw e
        }
      }
    }
    progPubMapInfo.getStatus_enum
  }

  def getcampaignStatus(campaignId: String): String = {
    var campaign_sts = new PublisherCampaignInfo
    if (campaignId != null && !campaignId.equals("")) {
      try {
        logger.debug("Get campaign status for campaignId = " + campaignId + " from corp couchbase")
        val (cacheClient, bucket) = CorpCouchbaseClient.getBucketFunc()
        if (bucket.exists("EPN_pubcmpn_" + campaignId)) {
          val map = bucket.get("EPN_pubcmpn_" + campaignId, classOf[JsonDocument])
          campaign_sts = new Gson().fromJson(String.valueOf(map.content().get("0")), classOf[PublisherCampaignInfo])
        }
        CorpCouchbaseClient.returnClient(cacheClient)
      } catch {
        case e: Exception => {
          logger.error("Corp Couchbase error while getting campaign status for campaignId = " + campaignId, e)
          throw e
        }
      }
    }
    campaign_sts.getStatus_enum
  }

  def getDate(date: String): String = {
    val splitted = date.split("=")
    if (splitted != null && splitted.nonEmpty) splitted(1)
    else throw new Exception("Invalid date field in metafile.")
  }
}
