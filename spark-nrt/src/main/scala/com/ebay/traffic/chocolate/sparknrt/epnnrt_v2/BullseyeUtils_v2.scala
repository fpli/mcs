package com.ebay.traffic.chocolate.sparknrt.epnnrt_v2

import java.sql.Timestamp
import java.util.{Base64, Properties}

import com.ebay.traffic.monitoring.{ESMetrics, Metrics}
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics
import com.google.gson.JsonParser
import jodd.io.FileUtil.params
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpResponse}
import spray.json._

object BullseyeUtils_v2 {
  @transient lazy val logger = LoggerFactory.getLogger(this.getClass)

  lazy val bullseyeTokenFile = properties.getProperty("epnnrt.bullseye.token")

  @transient lazy val properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("epnnrt_v2.properties"))
    properties.load(getClass.getClassLoader.getResourceAsStream("sherlockio.properties"))
    properties
  }

  @transient lazy val metrics: SherlockioMetrics = {
    SherlockioMetrics.init(properties.getProperty("sherlockio.namespace"),properties.getProperty("sherlockio.endpoint"),properties.getProperty("sherlockio.user"))
    val sherlockioMetrics = SherlockioMetrics.getInstance()
    sherlockioMetrics.setJobName("bullseyeUtil")
    sherlockioMetrics
  }

  // use new oAuth POST endpoint to get token
  def generateToken2: String = try {
    Http(properties.getProperty("epnnrt.oauthUrl")).method("POST")
      .header("Authorization", "Basic " + getOauthAuthorization())
      .header("Content-Type", properties.getProperty("epnnrt.contenttype"))
      .postData(properties.getProperty("epnnrt.oauthbody"))
      .asString
      .body.parseJson.convertTo[TokenResponse].access_token
  } catch {
    case e: Exception => {
      logger.error("Error when generate Bullseye token from bullseye, get token from HDFS file" + e)
      metrics.meterByGauge("BullseyeTokenError_tess", 1)
      null
    }
  }

  var token: String = generateToken2

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
        token = generateToken2
        logger.warn(s"get new token: $token")
        metrics.meterByGauge("BullsEyeError_tess", 1)
        None
      }
    } catch {
      case e: Exception => {
        logger.error("error when parse last view item : CGUID:" + cguid + " response: " + e)
        // metrics.meterByGauge("BullsEyeError", 1)
        None
      }
    }
  }

  def getDataV3(cguid: String, guid: String, modelId: String, count: String, bullseyeUrl: String): Option[HttpResponse[String]] = {
    try {
      logger.debug(s"Bullseye sending, cguid=$cguid, guid=$guid")
      val response = Http(bullseyeUrl).method("GET")
        .header("Authorization", s"Bearer $token")
        .param("uuid", "cguid:" + cguid)
        .param("uuid", "guid:" + guid)
        .param("modelid", modelId)
        .param("count", count)
        .asString

      if (response.isNotError)
        Some(response)
      else {
        logger.error(s"bullseye response for cguid $cguid, guid $guid with error: $response")
        token = generateToken2
        logger.warn(s"get new token: $token")
        metrics.meterByGauge("BullsEyeError_tess", 1)
        None
      }
    } catch {
      case e: Exception => {
        logger.error("error when parse last view item : CGUID:" + cguid +  " , GUID:" + guid + " response: " + e)
        // metrics.meterByGauge("BullsEyeError", 1)
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
            (0 until list.size())
              .foreach(i => {
                var item_id = list.get(i).getAsJsonObject.get("item_id").toString
                val lastViewTime = list.get(i).getAsJsonObject.get("timestamp").toString.toLong
                if (isItemIdValid(timestamp, item_id, lastViewTime)) {
                  item_id = item_id.replace("\"", "")
                  val date = new Timestamp(lastViewTime).toString
                  metrics.meterByGauge("SuccessfulGet_tess", 1)
                  return (item_id, date)
                }
              })
          } else {
            //for multiple response results
            var maxLastViwTime = Long.MinValue
            var itemId = ""
            (0 until result_list.size())
              .foreach(i => {
                val list = new JsonParser().parse(responseBody).getAsJsonArray.get(i).getAsJsonObject.get("results").
                  getAsJsonObject.get("response").getAsJsonObject.get("view_item_list").getAsJsonArray
                import util.control.Breaks._
                breakable {
                  (0 until list.size())
                    .foreach(i => {
                      val item_Id = list.get(i).getAsJsonObject.get("item_id").toString
                      val lastViewTime = list.get(i).getAsJsonObject.get("timestamp").toString.toLong
                      if (isItemIdValid(timestamp, maxLastViwTime, item_Id, lastViewTime)) {
                        itemId = item_Id.replace("\"", "")
                        maxLastViwTime = lastViewTime
                        break()
                      }
                    })
                }
              })
            if (maxLastViwTime > 0) {
              metrics.meterByGauge("SuccessfulGet_tess", 1)
              return (itemId, new Timestamp(maxLastViwTime).toString)
            }
          }
          metrics.meterByGauge("BullsEyeHit_tess", 1)
          ("", "")
      }
    } catch {
      case _: Exception =>
        logger.error("error when parse last view item : CGUID:" + cguid + " response: " + result.toString)
        ("", "")
    }
  }

  def getLastViewItemV3(cguid: String, guid: String, timestamp: String, modelId: String, count: String, bullseyeUrl: String): (String, String) = {
    val start = System.currentTimeMillis
    val result = getDataV3(cguid, guid, modelId, count, bullseyeUrl)
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
            (0 until list.size())
              .foreach(i => {
                var item_id = list.get(i).getAsJsonObject.get("item_id").toString
                val lastViewTime = list.get(i).getAsJsonObject.get("timestamp").toString.toLong
                if (isItemIdValid(timestamp, item_id, lastViewTime)) {
                  item_id = item_id.replace("\"", "")
                  val date = new Timestamp(lastViewTime).toString
                  metrics.meterByGauge("SuccessfulGet_tess", 1)
                  return (item_id, date)
                }
              })
          } else {
            //for multiple response results
            var maxLastViwTime = Long.MinValue
            var itemId = ""
            (0 until result_list.size())
              .foreach(i => {
                val list = new JsonParser().parse(responseBody).getAsJsonArray.get(i).getAsJsonObject.get("results").
                  getAsJsonObject.get("response").getAsJsonObject.get("view_item_list").getAsJsonArray
                import util.control.Breaks._
                breakable {
                  (0 until list.size())
                    .foreach(i => {
                      val item_Id = list.get(i).getAsJsonObject.get("item_id").toString
                      val lastViewTime = list.get(i).getAsJsonObject.get("timestamp").toString.toLong
                      if (isItemIdValid(timestamp, maxLastViwTime, item_Id, lastViewTime)) {
                        itemId = item_Id.replace("\"", "")
                        maxLastViwTime = lastViewTime
                        break()
                      }
                    })
                }
              })
            if (maxLastViwTime > 0) {
              metrics.meterByGauge("SuccessfulGet_tess", 1)
              return (itemId, new Timestamp(maxLastViwTime).toString)
            }
          }
          metrics.meterByGauge("BullsEyeHit_tess", 1)
          ("", "")
      }
    } catch {
      case _: Exception =>
        logger.error("error when parse last view item : CGUID:" + cguid +  " , GUID:" + guid + " response: " + result.toString)
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
            (0 until list.size())
              .foreach(i => {
                var item_id = list.get(i).getAsJsonObject.get("item_id").toString
                val lastViewTime = list.get(i).getAsJsonObject.get("timestamp").toString.toLong
                if (isItemIdValid(timestamp, item_id, lastViewTime)) {
                  item_id = item_id.replace("\"", "")
                  val date = new Timestamp(lastViewTime).toString
                  return (item_id, date)
                }
              })
          } else {
            var maxLastViwTime = Long.MinValue
            var itemId = ""
            (0 until result_list.size())
              .foreach(i => {
                val list = new JsonParser().parse(responseBody).getAsJsonArray.get(i).getAsJsonObject.get("results").
                  getAsJsonObject.get("response").getAsJsonObject.get("view_item_list").getAsJsonArray
                import util.control.Breaks._
                breakable {
                  (0 until list.size())
                    .foreach(i => {
                      val item_Id = list.get(i).getAsJsonObject.get("item_id").toString
                      val lastViewTime = list.get(i).getAsJsonObject.get("timestamp").toString.toLong
                      if (isItemIdValid(timestamp, maxLastViwTime, item_Id, lastViewTime)) {
                        itemId = item_Id.replace("\"", "")
                        maxLastViwTime = lastViewTime
                        break()
                      }
                    })
                }
              })
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

  private def isItemIdValid(timestamp: String, maxLastViwTime: Long, item_Id: String, lastViewTime: Long) = {
    !item_Id.equalsIgnoreCase("null") && lastViewTime <= timestamp.toLong && lastViewTime > maxLastViwTime
  }

  private def isItemIdValid(timestamp: String, item_id: String, lastViewTime: Long) = {
    !item_id.equalsIgnoreCase("null") && lastViewTime <= timestamp.toLong
  }

  // get oauth Authorization
  def getOauthAuthorization(): String = {
    var authorization = ""
    try {
      val consumerIdAndSecret = properties.getProperty("epnnrt.clientId") + ":" + getSecretByClientId(properties.getProperty("epnnrt.clientId"))
      authorization = Base64.getEncoder().encodeToString(consumerIdAndSecret.getBytes("UTF-8"))
    } catch {
      case e: Exception => {
        logger.error("Error when encode consumerId:consumerSecret to String" + e)
        metrics.meterByGauge("ErrorEncodeConsumerIdAndSecret_tess", 1)
      }
    }

    authorization
  }

  case class TokenResponse(
                            access_token: String,
                            token_type: String,
                            expires_in: Long
                          )

  object TokenResponse extends DefaultJsonProtocol {
    implicit val _format: RootJsonFormat[TokenResponse] = jsonFormat3(apply)
  }

  def getSecretByClientId(clientId: String): String = {
    var secret = ""
    val secretEndPoint = properties.getProperty("epnnrt.fetchclientsecret.endpoint") + clientId
    try {
      val response = Http(secretEndPoint).method("GET")
        .asString
        .body.parseJson
      if (response != null) {
        secret = response.convertTo[SecretResponse].clientSecret
      }
    } catch {
      case e: Exception =>
        metrics.meterByGauge("getSecretByClientIdError_tess",1)
        logger.error("get client secret failed " + e)
    }
    if (secret == null) {
      secret = ""
      metrics.meterByGauge("getClientSecretNull_tess",1)
    }
    secret
  }

  case class SecretResponse(
                             clientId: String,
                             clientSecret: String,
                             expiration: Long
                           )

  object SecretResponse extends DefaultJsonProtocol {
    implicit val _format: RootJsonFormat[SecretResponse] = jsonFormat3(apply)
  }

}
