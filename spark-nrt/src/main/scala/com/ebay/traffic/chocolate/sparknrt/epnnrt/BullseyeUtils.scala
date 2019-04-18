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
    val url  = properties.getProperty("epnnrt.elasticsearchUrl")
    if (url != null && url.nonEmpty) {
      ESMetrics.init("chocolate-metrics-", url)
      ESMetrics.getInstance()
    } else null
  }

  //TODO try catch metrics  bullseye response time   renew token  retry 2 times
  def generateToken: JsValue = Http(properties.getProperty("epnnrt.oauthUrl")).method("GET")
    .param("client_id", properties.getProperty("epnnrt.clientId"))
    .param("client_secret", properties.getProperty("epnnrt.clientsecret"))
    .param("grant_type","client_credentials")
    .param("scope", "https://api.ebay.com/oauth/scope/@public")
    .asString
    .body.parseJson

  var token: JsValue = generateToken


  //may be retry here
  def getData(cguid:String):Option[HttpResponse[String]] = {
    try {
      logger.debug(s"Bullseye sending, cguid=$cguid")
      val response = Http(properties.getProperty("epnnrt.bullseyeUrl")).method("GET")
        .header("Authorization",s"Bearer $token")
        .param("cguid", cguid)
        .param("modelid", properties.getProperty("epnnrt.modelId"))
        .param("count", properties.getProperty("epnnrt.lastviewitemnum"))
        .param("linkvtou", false.toString)
        .asString

      if(response.isNotError)
        Some(response)
       else {
        logger.error(s"bullseye response for cguid $cguid with error: $response")
        token = generateToken
        logger.warn(s"get new token: $token")
        metrics.meter("BullsEyeError", 1)
        None
      }
    } catch {
      case e : Exception => {
        logger.error("error when parse last view item : CGUID:" + cguid + " response: " + e)
        metrics.meter("BullsEyeError", 1)
        None
      }
    }
  }

/*  def getLastViewItem(cguid: String, timestamp: String): (String, String)= {
    retry(2) {
      val start = System.currentTimeMillis
      val result = getData(cguid)
      metrics.mean("BullsEyeLatency", System.currentTimeMillis - start)
      try {
        val responseBody = result.get.body
        responseBody match {
          case null | "" => ("", "")
          case _ =>
            val list = new JsonParser().parse(responseBody).getAsJsonArray.get(0).getAsJsonObject.get("results").
              getAsJsonObject.get("response").getAsJsonObject.get("view_item_list").getAsJsonArray
            if (list.size() > 0) {
              for (i <- 0 until list.size()) {
                if(list.get(i).getAsJsonObject.get("timestamp").toString.toLong <= timestamp.toLong) {
                  var item_id = list.get(i).getAsJsonObject.get("item_id").toString
                  if (item_id.equalsIgnoreCase("null"))
                    item_id = ""
                  else
                    item_id = item_id.replace("\"","")
                  val date = new Timestamp(list.get(i).getAsJsonObject.get("timestamp").toString.toLong).toString
                  metrics.meter("SuccessfulGet", 1)
                  return (item_id, date)
                }
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
  }*/

  def getLastViewItem(cguid: String, timestamp: String): (String, String)= {
    val start = System.currentTimeMillis
    val result = getData(cguid)
    metrics.mean("BullsEyeLatency", System.currentTimeMillis - start)

    try {
      val responseBody = result.get.body
      responseBody match {
        case null | "" => ("", "")
        case _ =>
          val list = new JsonParser().parse(responseBody).getAsJsonArray.get(0).getAsJsonObject.get("results").
            getAsJsonObject.get("response").getAsJsonObject.get("view_item_list").getAsJsonArray
          if (list.size() > 0) {
            for (i <- 0 until list.size()) {
              var item_id = list.get(i).getAsJsonObject.get("item_id").toString
              if(!item_id.equalsIgnoreCase("null") && list.get(i).getAsJsonObject.get("timestamp").toString.toLong <= timestamp.toLong){
                  item_id = item_id.replace("\"","")
                  val date = new Timestamp(list.get(i).getAsJsonObject.get("timestamp").toString.toLong).toString
                  metrics.meter("SuccessfulGet", 1)
                  return (item_id, date)
              }
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
}
