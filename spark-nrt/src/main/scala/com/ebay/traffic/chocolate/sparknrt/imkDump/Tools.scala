package com.ebay.traffic.chocolate.sparknrt.imkDump

import java.net.{URL, URLDecoder}
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date
import java.util.regex.Pattern

import com.ebay.traffic.monitoring.{ESMetrics, Metrics}
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by ganghuang on 12/3/18.
  * some tools of dump job
  */

class Tools(metricsPrefix: String, elasticsearchUrl: String) extends Serializable{
  @transient lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  @transient lazy val metrics: Metrics = {
    if (StringUtils.isNotEmpty(metricsPrefix) && StringUtils.isNotEmpty(elasticsearchUrl)) {
      ESMetrics.init(metricsPrefix, elasticsearchUrl)
      ESMetrics.getInstance()
    } else null
  }

  lazy val METRIC_IMK_DUMP_MALFORMED = "imk.dump.malformed"

  lazy val keywordParams: Array[String] = Array("_nkw", "keyword", "kw")

  lazy val userQueryParamsOfReferrer: Array[String] = Array("q")

  lazy val userQueryParamsOfLandingUrl: Array[String] = Array("uq", "satitle", "keyword", "item", "store")

  // do not filter /ulk XC-1541
  lazy val ebaySites: Pattern = Pattern.compile("^(http[s]?:\\/\\/)?([\\w-.]+\\.)?(ebay(objects|motors|promotion|development|static|express|liveauctions|rtm)?)\\.[\\w-.]+($|\\/(?!ulk\\/).*)", Pattern.CASE_INSENSITIVE)

  lazy val user_agent_map: Map[String, Int] = Map(
    "msie" -> 2,
    "firefox" -> 5,
    "chrome" -> 11,
    "safari" -> 4,
    "opera" -> 7,
    "netscape" -> 1,
    "navigator" -> 1,
    "aol" -> 3,
    "mac" -> 8,
    "msntv" -> 9,
    "webtv" -> 6,
    "trident" -> 2,
    "bingbot" -> 12,
    "adsbot-google" -> 19,
    "ucweb" -> 25,
    "facebookexternalhit" -> 20,
    "dvlvik" -> 26,
    "ahc" -> 13,
    "tubidy" -> 14,
    "roku" -> 15,
    "ymobile" -> 16,
    "pycurl" -> 17,
    "dailyme" -> 18,
    "ebayandroid" -> 21,
    "ebayiphone" -> 22,
    "ebayipad" -> 23,
    "ebaywinphocore" -> 24,
    "NULL_USERAGENT" -> 10,
    "UNKNOWN_USERAGENT" -> -99)


  @transient lazy val search_keyword_map: Map[String, String] = {
    val mapData = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("search_host_keyword_map.txt")).getLines
    mapData.map(line => line.split(",")(0) -> line.split(",")(1)).toMap
  }


  def getDateTimeFromTimestamp(timestamp: Long): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    df.format(timestamp)
  }

  /**
    * get query string from url string
    * @param uri url string
    * @return query string
    */
  def getQueryString(uri: String): String = {
    var query = ""
    if (StringUtils.isNotEmpty(uri)) {
      try {
        query = new URL(uri).getQuery
        if (StringUtils.isEmpty(query)) {
          query = ""
        }
      }catch {
        case e: Exception => {
          if(metrics != null) {
            metrics.meter(METRIC_IMK_DUMP_MALFORMED, 1)
          }
          logger.warn("MalformedUrl", e)
        }
      }
    }
    query
  }

  /**
    * get perf_track_name_value from query string
    * @param query query
    * @return perf_track_name_value
    */
  def getPerfTrackNameValue(query: String): String = {
    val buf = StringBuilder.newBuilder
    try {
      if (StringUtils.isNotEmpty(query)) {
        query.split("&").foreach(paramMapString => {
          val paramStringArray = paramMapString.split("=")
          if (paramStringArray.length == 2) {
            buf.append("^" + paramMapString)
          }
        })
      }
    } catch {
      case e: Exception => {
        if(metrics != null) {
          metrics.meter("imk.dump.error.getPerfTrackNameValue", 1)
        }
        logger.warn("MalformedUrl", e)
      }
    }
    buf.toString()
  }

  def getDateFromTimestamp(timestamp: Long): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    df.format(timestamp)
  }

  def getOutPutFileDate: String = {
    val df = new SimpleDateFormat("yyyyMMdd_HHmmss")
    df.format(new Date())
  }

  /**
    * get one param from the url query string
    * @param query url query string
    * @param key param name
    * @return param value
    */
  def getParamValueFromQuery(query: String, key: String): String = {
    var result = ""
    try {
      if (StringUtils.isNotEmpty(query)) {
        query.split("&").foreach(paramMapString => {
          val paramStringArray = paramMapString.split("=")
          if (paramStringArray.nonEmpty && paramStringArray(0).trim.equalsIgnoreCase(key) && paramStringArray.length == 2) {
            result = paramStringArray(1).trim
          }
        })
      }
    } catch {
      case e: Exception => {
        if(metrics != null) {
          metrics.meter("imk.dump.error.getParamValueFromQuery", 1)
        }
        logger.warn("MalformedUrl", e)
      }
    }
    return result
  }

  /**
    * get number param value from url, default value is null string
    * @param query query
    * @param key param key
    * @return
    */
  def getDefaultNullNumParamValueFromQuery(query: String, key: String): String = {
    var result = ""
    try {
      if (StringUtils.isNotEmpty(query)) {
        query.split("&").foreach(paramMapString => {
          val paramStringArray = paramMapString.split("=")
          if (paramStringArray(0).trim.equalsIgnoreCase(key) && paramStringArray.length == 2) {
            if (StringUtils.isNumeric(paramStringArray(1).trim)) {
              result = paramStringArray(1).trim
            }
          }
        })
      }
    }catch {
      case e: Exception => {
        if(metrics != null) {
          metrics.meter("imk.dump.parsemtid.error", 1)
        }
        logger.warn("ParseMtidError", e)
        logger.warn("ParseMtidError query: ", query)
      }
    }
    result
  }

  /**
    * client id is the first part of rotation id
    * @param rotationId rotation id
    * @return client id
    */
  def getClientIdFromRotationId(rotationId: String): String = {
    var result = ""
    try {
      if (StringUtils.isNotEmpty(rotationId)
        && rotationId.length <= 25
        && StringUtils.isNumeric(rotationId.replace("-", ""))
        && rotationId.contains("-")) {
        result = rotationId.substring(0, rotationId.indexOf("-"))
      } else {
        result = ""
      }
    }
    result
  }

  /**
    * get item id from url, only support itm and i page now for marketing channels and mpuid in roi events
    * @param uri url string
    * @return item id
    */
  def getItemIdFromUri(uri: String): String = {
    var path = ""
    try {
      path = new URL(uri).getPath
      if (StringUtils.isNotEmpty(path) && (path.startsWith("/itm/") || path.startsWith("/i/"))) {
        val itemId = path.substring(path.lastIndexOf("/") + 1)
        if (StringUtils.isNumeric(itemId)) {
          return itemId
        }
      }
    } catch {
      case e: Exception => {
        if (metrics != null) {
          metrics.meter(METRIC_IMK_DUMP_MALFORMED, 1)
        }
        logger.warn("MalformedUrl", e)
      }
    }
    ""
  }

  /**
    * get browser type by user agent
    * @param userAgent user agent
    * @return browser type
    */
  def getBrowserType(userAgent: String): Int = {
    if (StringUtils.isNotEmpty(userAgent)) {
      val agentStr = userAgent.toLowerCase()
      for ((k, v) <- user_agent_map) {
        if (agentStr.contains(k))
          return v
      }
    }
    user_agent_map("UNKNOWN_USERAGENT")
  }

  /**
    * get one value from a list of param names
    * @param query url query string
    * @param keys param name list
    * @return param value
    */
  def getParamFromQuery(query: String, keys: Array[String]): String = {
    try {
      if (StringUtils.isNotEmpty(query)) {
        query.split("&").foreach(paramMapString => {
          val paramMapStringArray = paramMapString.split("=")
          val param = paramMapStringArray(0)
          for (key <- keys) {
            if (key.equalsIgnoreCase(param) && paramMapStringArray.size == 2) {
              return paramMapStringArray(1)
            }
          }
        })
      }
    }catch {
      case e: Exception => {
        if (metrics != null) {
          metrics.meter("imk.dump.error.getParamFromQuery", 1)
        }
        logger.warn("MalformedUrl", e)
      }
    }
    ""
  }

  /**
    * get command type
    * @param commandType command
    * @return
    */
  def getCommandType(commandType: String): String = {
    commandType match {
      case "IMPRESSION" => "4"
      case "ROI" => "2"
      case "SERVE" => "4"
      case _ => "1"
    }
  }

  /**
    * get channel id from channel type
    * @param channelType channel type
    * @return channel id
    */
  def getChannelType(channelType: String): String = {
    channelType match {
      case "EPN" => "1"
      case "DISPLAY" => "4"
      case "PAID_SEARCH" => "2"
      case "SOCIAL_MEDIA" => "16"
      case "PAID_SOCIAL" => "20"
      case "ROI" => "0"
      case "NATURAL_SEARCH" => "3"
      case "SEARCH_ENGINE_FREE_LISTINGS" => "28"
      case _ => "0"
    }
  }

  def getUserMapInd(userId: String): String = {
    if(StringUtils.isEmpty(userId) || userId == "0") {
      "0"
    } else{
      "1"
    }
  }

  /**
    * get batch id
    * @return batch id byte array
    */
  def getBatchId: String = {
    val date = new Date()
    val formatter = new DecimalFormat("00")
    formatter.format(date.getHours) + formatter.format(date.getMinutes) + formatter.format(date.getSeconds)
  }

  /**
    * get domain of one link
    * @param link link
    * @return domain
    */
  def getDomain(link: String): String = {
    var result = ""
    if (StringUtils.isNotEmpty(link)) {
      try {
        result = new URL(link).getHost
      } catch {
        case e: Exception => {
          if(metrics != null) {
            metrics.meter(METRIC_IMK_DUMP_MALFORMED, 1)
          }
          logger.warn("MalformedUrl", e)
        }
      }
    }
    result
  }

  /**
    * get user query from referrer or uri
    * @param referrer referrer
    * @param query uri
    * @return user query
    */
  def getUserQuery(referrer: String, query: String): String = {
    var result = ""
    try {
      if (StringUtils.isNotEmpty(referrer)) {
        val userQueryFromReferrer = getParamFromQuery(getQueryString(referrer.toLowerCase), userQueryParamsOfReferrer)
        if (StringUtils.isNotEmpty(userQueryFromReferrer)) {
          result = userQueryFromReferrer
        } else {
          result = getParamFromQuery(query.toLowerCase, userQueryParamsOfLandingUrl)
        }
      } else {
        result = ""
      }
    } catch {
      case e: Exception => {
        if(metrics != null) {
          metrics.meter("imk.dump.errorGetQuery", 1)
        }
        logger.warn("ErrorGetQuery", e)
      }
    }
    result
  }

  /**
    * judge traffic is from ebay sites
    * @param referrer referrer
    * @return is or not
    */
  def judgeNotEbaySites(referrer: String): Boolean = {
    val matcher = ebaySites.matcher(referrer)
    if (matcher.find()) {
      if(metrics != null)
        metrics.meter("imk.dump.internalReferer")
      false
    } else {
      true
    }
  }

  /**
    * Get ROI related fields from mpuid
    * @param index the index of field
    * @param query input uri query
    * @return roi field
    */
  def getRoiIdFromUrlQuery(index: Int, query: String): String = {
    val mupid = getParamValueFromQuery(query,"mpuid")
    val ids = mupid.split(";")
    if(ids.length > index)
      return ids(index)
    else
      return "0"
  }

  /**
    * Get client_id from rover url
    * @param uri rover uri
    * @return client_id
    */
  def getClientIdFromRoverUrl(uri: String): String = {
    "0"
  }



  /**
    * Get query map from  url
    * @param uri uri
    * @return query_map
    */
  def getQueryMapFromUrl(uri: String): mutable.Map[String, String] = {
    val result: mutable.Map[String, String] = mutable.Map[String, String]()
    if (StringUtils.isEmpty(uri)) {
      return result
    }
    try {
      val query = new URL(uri).getQuery
      if (StringUtils.isNotEmpty(query)) {
        query.split("&").foreach(paramMapString => {
          if (StringUtils.isNotEmpty(paramMapString)) {
            val paramStringArray = paramMapString.split("=")
            if (paramStringArray.nonEmpty && paramStringArray.length == 2) {
              result += (paramStringArray(0).trim -> paramStringArray(1).trim)
            }
          }
        })
      }
    } catch {
      case e: Exception => {
        if (metrics != null) {
          metrics.meter("imk.dump.errorGetQueryMap", 1)
        }
        logger.warn("ErrorGetQueryMap", e)
      }
    }
    result
  }


  /**
    * get user query from applicationPayload.ref, only for natural-search channel
    * @param ref applicationPayload.ref
    * @return user query
    */
  def getUserQueryFromRef(ref: String): String = {
    val host = getDomain(ref.toLowerCase)
    val queryMap = getQueryMapFromUrl(ref.toLowerCase)
    if (StringUtils.isNotEmpty(host) && queryMap.nonEmpty) {
      for ((k, v) <- search_keyword_map) {
        if (host.contains(k.trim)) {
          v.split("\\|").foreach(keyword => {
            if (StringUtils.isNotEmpty(keyword) && !queryMap.get(keyword.trim).isEmpty) {
              return queryMap.get(keyword.trim).get
            }
          })
        }
      }
    }
    return ""
  }



  /**
    * get landing page url from rvr_url or client_data.referrer
    * if rvr_url contains 'mpcl', then extract from rvr_url,
    * else return client_data.referrer
    * @param mpcl: extract from rvr_url, referer: client_data.referrer
    * @return landing page url
    */
  def getLandingPageUrlFromUriOrRfrr(mpcl: String, referer: String): String = {
    if (StringUtils.isEmpty(mpcl)) {
      return referer
    }
    mpcl
  }


  /**
    * get one decode param url value from the url query string
    * @param query url query string
    * @param key param name
    * @return param value
    */
  def getDecodeParamUrlValueFromQuery(query: String, key: String): String = {
    var result = ""
    try {
      if (StringUtils.isNotEmpty(query)) {
        query.split("&").foreach(paramMapString => {
          val paramStringArray = paramMapString.split("=")
          if (paramStringArray.nonEmpty && paramStringArray(0).trim.equalsIgnoreCase(key) && paramStringArray.length == 2) {
            result = URLDecoder.decode(paramStringArray(1).trim, "UTF-8")
          }
        })
      }
    } catch {
      case e: Exception => {
        if(metrics != null) {
          metrics.meter("imk.dump.error.getDecodeParamUrlValueFromQuery", 1)
        }
        logger.warn("MalformedUrl", e)
      }
    }
    return result
  }

  /**
    * get decode perf_track_name_value from query string
    * @param query query
    * @return perf_track_name_value
    */
  def getDecodePerfTrackNameValue(query: String): String = {
    val buf = StringBuilder.newBuilder
    try {
      if (StringUtils.isNotEmpty(query)) {
        query.split("&").foreach(paramMapString => {
          val paramStringArray = paramMapString.split("=")
          if (paramStringArray.length == 2) {
            buf.append("^" + URLDecoder.decode(paramMapString, "UTF-8"))
          }
        })
      }
    } catch {
      case e: Exception => {
        if(metrics != null) {
          metrics.meter("imk.dump.error.getDecodePerfTrackNameValue", 1)
        }
        logger.warn("MalformedUrl", e)
      }
    }
    buf.toString()
  }

}
