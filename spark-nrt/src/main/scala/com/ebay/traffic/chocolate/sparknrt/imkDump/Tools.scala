package com.ebay.traffic.chocolate.sparknrt.imkDump

import java.net.URL
import java.text.SimpleDateFormat

import org.apache.commons.lang3.StringUtils

import scala.collection.immutable.HashMap

object Tools {

  //refer to https://github.corp.ebay.com/chocolate/lp_url_rewrite/blob/CC-2.0.0/src/main/java/com/ebay/traffic/lprewrite/common/constant/DefaultUrlEnum.java
  lazy val rotationIdPrefixSiteIdMap: HashMap[String, Int] = HashMap(
    "711-" -> 1,
    "1065-" -> 1,
    "8971-" -> 1,
    "705-" -> 15,
    "5221-" -> 16,
    "13031-" -> 16,
    "706-" -> 2,
    "1242-" -> 2,
    "4080-" -> 223,
    "709-" -> 71,
    "1833-" -> 71,
    "707-" -> 77,
    "1066-" -> 77,
    "3422-" -> 201,
    "4686-" -> 203,
    "3416-" -> 203,
    "5282-" -> 205,
    "5284-" -> 205,
    "13032-" -> 205,
    "724-" -> 101,
    "1561-" -> 101,
    "4825-" -> 207,
    "1346-" -> 146,
    "1841-" -> 146,
    "4824-" -> 211,
    "4908-" -> 212,
    "3423-" -> 216,
    "1185-" -> 186,
    "1721-" -> 186,
    "13029-" -> 186,
    "3424-" -> 218,
    "5222-" -> 193,
    "1631-" -> 223,
    "710-" -> 3,
    "1116-" -> 3,
    "1553-" -> 23,
    "1842-" -> 23,
    "13030-" -> 23
  )

  lazy val keywordParams: Array[String] = Array("_nkw")

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

  def getValueFromRequestHeader(request: String, key: String): String = {
    if (StringUtils.isNotEmpty(request)) {
      request.split("\\|").foreach(paramMapString => {
        val paramMapStringArray = paramMapString.split(":")
        val param = paramMapStringArray(0)
        if(param.trim.equalsIgnoreCase(key) && paramMapStringArray.size >= 2) {
          return paramMapString.substring(paramMapString.indexOf(":") + 1).trim
        }
      })
    }
    ""
  }

  def getDateTimeFromTimestamp(timestamp: Long): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    df.format(timestamp)
  }

  def getQueryString(uri: String): String = {
    // do we need catch malformed url? illegal url are filtered in event-listener
    val query = new URL(uri).getQuery
    if(StringUtils.isEmpty(query)) {
      ""
    } else {
      query
    }
  }

  def getDateFromTimestamp(timestamp: Long): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    df.format(timestamp)
  }

  def getParamValueFromUrl(uri: String, key: String): String = {
    val query = new URL(uri).getQuery
    if (StringUtils.isNotEmpty(query)) {
      query.split("&").foreach(paramMapString => {
        val paramStringArray = paramMapString.split("=")
        if (paramStringArray(0).trim.equalsIgnoreCase(key) && paramStringArray.length == 2) {
          return paramStringArray(1).trim
        }
      })
    }
    ""
  }

  def getUserMapInd(header: String): String = {
    val userId = getValueFromRequestHeader(header, "userid")
    if(StringUtils.isEmpty(userId) || userId == "0") {
      "0"
    } else {
      "1"
    }
  }

  def getSiteIdFromRotationId(rotationId: String): String = {
    // sample data from imk table, siteid for ps is always null, need this function?
    if (rotationId.contains("-")) {
      rotationIdPrefixSiteIdMap(rotationId.substring(0, rotationId.indexOf("-") + 1)).toString
    } else {
      ""
    }
  }

  def getItemIdFromUri(uri: String): String = {
    val path = new URL(uri).getPath
    if (StringUtils.isNotEmpty(path) && (path.startsWith("/itm/") || path.startsWith("/i/"))){
      val itemId = path.substring(path.lastIndexOf("/") + 1)
      if(itemId forall Character.isDigit){
        return itemId
      }
    }
    ""
  }

  def getBrowserType(requestHeader: String): Int = {
    val userAgentStr = getValueFromRequestHeader(requestHeader, "User-Agent")
    if (userAgentStr == null)
      return user_agent_map("NULL_USERAGENT")
    val agentStr = userAgentStr.toLowerCase()
    for ((k, v) <- user_agent_map) {
      if (agentStr.contains(k))
        return v
    }
    user_agent_map("UNKNOWN_USERAGENT")
  }

  def getCGuidFromCookie(request_header: String, cguid: String): String = {
    val cookie = getValueFromRequestHeader(request_header, "Cookie")
    if (StringUtils.isNotEmpty(cookie)) {
      val index = cookie.indexOf(cguid)
      if (index != -1)
        return cookie.substring(index + 6, index + 38)
    }
    ""
  }

  def getParamFromQuery(uri: String, keys: Array[String]): String = {
    val query = new URL(uri).getQuery
    if(StringUtils.isNotEmpty(query)) {
      query.split("&").foreach(paramMapString => {
        val paramMapStringArray = paramMapString.split("=")
        val param = paramMapStringArray(0)
        for (key <- keys) {
          if(key.equalsIgnoreCase(param) && paramMapStringArray.size == 2) {
            return paramMapStringArray(1)
          }
        }
      })
    }
    ""
  }
}
