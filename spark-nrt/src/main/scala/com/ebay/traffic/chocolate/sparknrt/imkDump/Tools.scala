package com.ebay.traffic.chocolate.sparknrt.imkDump

import java.net.URL
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date

import org.apache.commons.lang3.StringUtils

import scala.collection.immutable.HashMap

/**
  * Created by ganghuang on 12/3/18.
  * some utilities of dump job
  */

object Tools extends Serializable{

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

  def getOutPutFileDate: String = {
    val df = new SimpleDateFormat("yyyyMMdd_HHmmss")
    df.format(new Date())
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

  def getClientIdFromRotationId(rotationId: String): String = {
    if (StringUtils.isNotEmpty(rotationId)
      && rotationId.length <= 25
      && StringUtils.isNumeric(rotationId.replace("-", ""))
      && rotationId.contains("-")) {
      rotationId.substring(0, rotationId.indexOf("-"))
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

  /**
    * get extrnl_cookie from svid in request header cookie
    * @param request_header header of request
    * @return Svid
    */
  def getSvidFromCookie(request_header: String): String = {
    val cookie = getValueFromRequestHeader(request_header, "Cookie")
    if (StringUtils.isNotEmpty(cookie)) {
      val index = cookie.indexOf("svid")
      if (index != -1) {
        return cookie.substring(index + 7, index + 25)
      }
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

  def getCommandType(commandType: String): String = {
    commandType match {
      case "IMPRESSION" => "4"
      case _ => "1"
    }
  }

  def getUserIdFromHeader(request_header: String): String = {
    if(StringUtils.isNotEmpty(request_header)) {
      val value = getValueFromRequestHeader(request_header, "X-EBAY-C-ENDUSERCTX")
      if(StringUtils.isNotEmpty(value)) {
        value.split(",").foreach(keyValueMap => {
          val keyValueArray = keyValueMap.split("=")
          if(keyValueArray(0).trim.equalsIgnoreCase("userid")
            && keyValueArray.length == 2 && StringUtils.isNotEmpty(keyValueArray(1).trim)) {
            return keyValueArray(1).trim
          }
        })
      }
    }
    ""
  }

  def getUserMapInd(userId: String): String = {
    if(StringUtils.isEmpty(userId) || userId == "0") {
      "0"
    } else{
      "1"
    }
  }

  /**
    * replace '-' in ram rotationid and check the rotation id can be convert to long
    * @param rotationId raw rotation id
    * @return
    */
  def convertRotationId(rotationId: String): String = {
    if (StringUtils.isNotEmpty(rotationId)
      && rotationId.length <= 25
      && StringUtils.isNumeric(rotationId.replace("-", ""))) {
      rotationId.replace("-", "")
    } else {
      ""
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

}
