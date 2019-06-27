package com.ebay.traffic.chocolate.sparknrt.epnnrt

import java.sql.Timestamp
import java.util.Properties

import com.ebay.traffic.monitoring.{ESMetrics, Metrics}
import com.google.gson.JsonParser
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpResponse}
import spray.json._

object BullseyeUtils {
  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  @transient lazy val properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("epnnrt.properties"))
    properties
  }

  @transient lazy val metrics: Metrics = {
    val url = properties.getProperty("epnnrt.elasticsearchUrl")
    if (url != null && url.nonEmpty) {
      ESMetrics.init("chocolate-metrics-", url)
      ESMetrics.getInstance()
    } else null
  }

  //TODO try catch metrics  bullseye response time   renew token  retry 2 times
  def generateToken: JsValue = Http(properties.getProperty("epnnrt.oauthUrl")).method("GET")
    .param("client_id", properties.getProperty("epnnrt.clientId"))
    .param("client_secret", properties.getProperty("epnnrt.clientsecret"))
    .param("grant_type", "client_credentials")
    .param("scope", "https://api.ebay.com/oauth/scope/@public")
    .asString
    .body.parseJson

  var token: JsValue = generateToken


  //may be retry here
  def getData(cguid: String, modelId: String, count: String, bullseyeUrl: String): Option[HttpResponse[String]] = {
    try {
      logger.debug(s"Bullseye sending, cguid=$cguid")
      val response = Http(bullseyeUrl).method("GET")
        .header("Authorization", s"Bearer $token")
        .param("cguid", cguid)
        .param("modelid", modelId)
        .param("count", count)
        .asString

      if (response.isNotError)
        Some(response)
      else {
        logger.error(s"bullseye response for cguid $cguid with error: $response")
        token = generateToken
        logger.warn(s"get new token: $token")
        metrics.meter("BullsEyeError", 1)
        None
      }
    } catch {
      case e: Exception => {
        logger.error("error when parse last view item : CGUID:" + cguid + " response: " + e)
        metrics.meter("BullsEyeError", 1)
        None
      }
    }
  }

  def getLastViewItem(cguid: String, timestamp: String, modelId: String, count: String, bullseyeUrl: String): (String, String) = {
    val start = System.currentTimeMillis
    val result = getData(cguid, modelId, count, bullseyeUrl)
    metrics.mean("BullsEyeLatency", System.currentTimeMillis - start)

    try {
      val responseBody = result.get.body
      responseBody match {
        case null | "" => ("", "")
        case _ =>
          val result_list = new JsonParser().parse(responseBody).getAsJsonArray
          if (result_list.size() == 1) {
            //normally there is one result
            val list = new JsonParser().parse(responseBody).getAsJsonArray.get(0).getAsJsonObject.get("results").
              getAsJsonObject.get("response").getAsJsonObject.get("view_item_list").getAsJsonArray
            if (list.size() > 0) {
              for (i <- 0 until list.size()) {
                var item_id = list.get(i).getAsJsonObject.get("item_id").toString
                if (!item_id.equalsIgnoreCase("null") && list.get(i).getAsJsonObject.get("timestamp").toString.toLong <= timestamp.toLong) {
                  item_id = item_id.replace("\"", "")
                  val date = new Timestamp(list.get(i).getAsJsonObject.get("timestamp").toString.toLong).toString
                  metrics.meter("SuccessfulGet", 1)
                  return (item_id, date)
                }
              }
            }
          } else {
            //for multiple response results
            var maxLastViwTime = Long.MinValue
            var itemId = ""
            for (i <- 0 until result_list.size()) {
              val list = new JsonParser().parse(responseBody).getAsJsonArray.get(i).getAsJsonObject.get("results").
                getAsJsonObject.get("response").getAsJsonObject.get("view_item_list").getAsJsonArray
              if (list.size() > 0) {
                import util.control.Breaks._
                breakable {
                  for (i <- 0 until list.size()) {
                    val item_Id = list.get(i).getAsJsonObject.get("item_id").toString
                    val lastViewTime = list.get(i).getAsJsonObject.get("timestamp").toString.toLong
                    if (!item_Id.equalsIgnoreCase("null") && lastViewTime <= timestamp.toLong && lastViewTime > maxLastViwTime) {
                      itemId = item_Id.replace("\"", "")
                      maxLastViwTime = lastViewTime
                      break()
                    }
                  }
                }
              }
            }
            if (maxLastViwTime > 0) {
              metrics.meter("SuccessfulGet", 1)
              return (itemId, new Timestamp(maxLastViwTime).toString)
            }
          }
          metrics.meter("BullsEyeHit", 1)
          ("", "")
      }
    } catch {
      case _: Exception =>
        logger.error("error when parse last view item : CGUID:" + cguid + " response: " + result.toString)
        ("", "")
    }
  }


  // for unit test
  def getLastViewItemByResponse(timestamp: String, result: HttpResponse[String]): (String, String) = {
    try {
      val responseBody = Some(result).get.body
      responseBody match {
        case null | "" => ("", "")
        case _ =>
          val result_list = new JsonParser().parse(responseBody).getAsJsonArray
          if (result_list.size() == 1) {
            val list = new JsonParser().parse(responseBody).getAsJsonArray.get(0).getAsJsonObject.get("results").
              getAsJsonObject.get("response").getAsJsonObject.get("view_item_list").getAsJsonArray
            if (list.size() > 0) {
              for (i <- 0 until list.size()) {
                var item_id = list.get(i).getAsJsonObject.get("item_id").toString
                if (!item_id.equalsIgnoreCase("null") && list.get(i).getAsJsonObject.get("timestamp").toString.toLong <= timestamp.toLong) {
                  item_id = item_id.replace("\"", "")
                  val date = new Timestamp(list.get(i).getAsJsonObject.get("timestamp").toString.toLong).toString
                  return (item_id, date)
                }
              }
            }
          } else {
            var maxLastViwTime = Long.MinValue
            var itemId = ""
            for (i <- 0 until result_list.size()) {
              val list = new JsonParser().parse(responseBody).getAsJsonArray.get(i).getAsJsonObject.get("results").
                getAsJsonObject.get("response").getAsJsonObject.get("view_item_list").getAsJsonArray
              if (list.size() > 0) {
                import util.control.Breaks._
                breakable {
                  for (i <- 0 until list.size()) {
                    val item_Id = list.get(i).getAsJsonObject.get("item_id").toString
                    val lastViewTime = list.get(i).getAsJsonObject.get("timestamp").toString.toLong
                    if (!item_Id.equalsIgnoreCase("null") && lastViewTime <= timestamp.toLong && lastViewTime > maxLastViwTime) {
                      itemId = item_Id.replace("\"", "")
                      maxLastViwTime = lastViewTime
                      break()
                    }
                  }
                }
              }
            }
            if (maxLastViwTime > 0) {
              return (itemId, new Timestamp(maxLastViwTime).toString)
            }
          }
          ("", "")
      }
    } catch {
      case _: Exception =>
        ("", "")
    }
  }
}