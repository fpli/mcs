package com.ebay.traffic.chocolate.sparknrt.epnnrt

import java.util.Properties

import org.scalatest.{FlatSpec, Matchers}
import scalaj.http.HttpResponse

class TestBullseyeUtils extends FlatSpec with Matchers{

  val properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("epnnrt.properties"))
    properties
  }

  "can parse response correctly" must "work" in {
    val response = "[{\"results\":{\"response\": { \"view_item_list\":[" +
      "{\"item_id\": \"100\",\"timestamp\": 1555292269030}," +
      "{\"item_id\": \"101\",\"timestamp\": 1555292249110}," +
      "{\"item_id\": \"102\",\"timestamp\": 1555292191250}," +
      "{\"item_id\": \"103\",\"timestamp\": 1555292153680}," +
      "{\"item_id\": \"104\",\"timestamp\": 1555291959220}," +
      "{\"item_id\": null,\"timestamp\": 1555291900220}," +
      "{\"item_id\": null,\"timestamp\": 1555291769030}]}," +
      " \"accountType\": \"CGUID\", \"userLinkCreationTime\": 0,\"userLinkLastModifiedDay\": 0,\"maskUserInfo\": \"d2ad\",\"userLink\": false},\"error\": null}]"
    val mockResponse = HttpResponse(response, 200, Map())
    val results = BullseyeUtils.getLastViewItemByResponse("1555292153683", mockResponse)
    assert(results._1 == "103")
    assert(results._2 == "2019-04-15 09:35:53.68")
  }

  "can parse response correctly to filter null result" must "work" in {
    val response = "[{\"results\":{\"response\": { \"view_item_list\":[" +
      "{\"item_id\": null,\"timestamp\": 1555292269030}," +
      "{\"item_id\": null,\"timestamp\": 1555292249110}," +
      "{\"item_id\": \"102\",\"timestamp\": 1555292191250}," +
      "{\"item_id\": \"103\",\"timestamp\": 1555292153680}," +
      "{\"item_id\": \"104\",\"timestamp\": 1555291959220}," +
      "{\"item_id\": null,\"timestamp\": 1555291900220}," +
      "{\"item_id\": null,\"timestamp\": 1555291769030}]}," +
      " \"accountType\": \"CGUID\", \"userLinkCreationTime\": 0,\"userLinkLastModifiedDay\": 0," +
      "\"maskUserInfo\": \"d2ad\",\"userLink\": false},\"error\": null}]"
    val mockResponse = HttpResponse(response, 200, Map())
    val results = BullseyeUtils.getLastViewItemByResponse("1555292249113", mockResponse)
    assert(results._1 == "102")
    assert(results._2 == "2019-04-15 09:36:31.25")
  }

  "can parse multiple response correctly" must "work" in {
    val response = "[{\"results\":{\"response\": { \"view_item_list\":[" +
      "{\"item_id\": null,\"timestamp\": 1555292269030}," +
      "{\"item_id\": \"102\",\"timestamp\": 1555292249110}," +
      "{\"item_id\": \"103\",\"timestamp\": 1555292191250}," +
      "{\"item_id\": null,\"timestamp\": 1555292153680}]}," +
      " \"accountType\": \"CGUID\", \"userLinkCreationTime\": 0," +
      "\"userLinkLastModifiedDay\": 0,\"maskUserInfo\": \"d2ad\",\"userLink\": false},\"error\": null}," +
      "{\"results\":{\"response\": { \"view_item_list\":[" +
        "{\"item_id\": null,\"timestamp\": 1555291959220}," +
      "{\"item_id\": \"104\",\"timestamp\": 1555290059220}," +
      "{\"item_id\": null,\"timestamp\": 1555281900220}," +
        "{\"item_id\": null,\"timestamp\": 1555271769030}]}," +
        " \"accountType\": \"CGUID\", \"userLinkCreationTime\": 0," +
        "\"userLinkLastModifiedDay\": 0,\"maskUserInfo\": \"d2ad\",\"userLink\": false},\"error\": null}]"

    val mockResponse = HttpResponse(response, 200, Map())
    val results = BullseyeUtils.getLastViewItemByResponse("1555290059224", mockResponse)
    assert(results._1 == "104")
    assert(results._2 == "2019-04-15 09:00:59.22")
  }

  //cguid
  "BullseyeUtilBasedCguid" must "work" in {
    val result = BullseyeUtils.getData("b3f1325515e0a93fd3d7ac27fffdd2ad", "910" , "200", properties.getProperty("epnnrt.bullseyeUrl"))
    println(result)
  }

  it must "can parse response correctly with cguid" in {
    val result = BullseyeUtils.getLastViewItem("b3f1325515e0a93fd3d7ac27fffdd2ad", "1555292249112","910" , "200", properties.getProperty("epnnrt.bullseyeUrl"))
    assert(result._1 == "350011047735")
    assert(result._2 == "2019-04-15 09:37:29.11")
  }


}
