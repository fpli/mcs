package com.ebay.traffic.chocolate.sparknrt.epnnrt

import java.util.Properties

import com.ebay.kernel.util.StringUtils
import scalaj.http.Http
import spray.json._

/**
 * @Auther YangYang
 */
object SecretClient {
  @transient lazy val properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("epnnrt.properties"))
    properties
  }

  def getSecretByClientId(clientId: String): String = {
    var secret = ""
    val secretEndPoint = properties.getProperty("epnnrt.fetchclientsecret.endpoint") + clientId
    val response = Http(secretEndPoint).method("GET")
      .asString
      .body.parseJson

    if (response != null) {
      secret = response.convertTo[SecretResponse].clientSecret
    }
    if (StringUtils.isBlank(secret)) {
      throw new Exception("fetch client secret failed.")
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
