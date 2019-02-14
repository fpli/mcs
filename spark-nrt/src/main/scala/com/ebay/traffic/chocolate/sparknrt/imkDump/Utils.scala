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

object Utils extends Serializable{

  lazy val keywordParams: Array[String] = Array("_nkw")

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

  /**
    * get one param from the url string
    * @param uri url string
    * @param key param name
    * @return param value
    */
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

  def getDefaultNullNumParamValueFromUrl(uri: String, key: String): String = {
    val query = new URL(uri).getQuery
    if (StringUtils.isNotEmpty(query)) {
      query.split("&").foreach(paramMapString => {
        val paramStringArray = paramMapString.split("=")
        if (paramStringArray(0).trim.equalsIgnoreCase(key) && paramStringArray.length == 2) {
          if (StringUtils.isNumeric(paramStringArray(1).trim)){
            return paramStringArray(1).trim
          }
        }
      })
    }
    ""
  }

  /**
    * client id is the first part of rotation id
    * @param rotationId rotation id
    * @return client id
    */
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

  /**
    * get item id from url, only support itm and i page now
    * @param uri url string
    * @return item id
    */
  def getItemIdFromUri(uri: String): String = {
    val path = new URL(uri).getPath
    if (StringUtils.isNotEmpty(path) && (path.startsWith("/itm/") || path.startsWith("/i/"))){
      val itemId = path.substring(path.lastIndexOf("/") + 1)
      if (StringUtils.isNumeric(itemId)) {
        return itemId
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
    * @param uri url string
    * @param keys param name list
    * @return param value
    */
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

  /**
    * get command type
    * @param commandType command
    * @return
    */
  def getCommandType(commandType: String): String = {
    commandType match {
      case "IMPRESSION" => "4"
      case _ => "1"
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
    if (StringUtils.isNotEmpty(link)) {
      new URL(link).getHost
    } else {
      ""
    }
  }

}
