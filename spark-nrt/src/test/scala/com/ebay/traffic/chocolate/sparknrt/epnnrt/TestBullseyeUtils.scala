package com.ebay.traffic.chocolate.sparknrt.epnnrt

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.scalatest.{FlatSpec, Matchers}
import scalaj.http.HttpResponse

class TestBullseyeUtils extends FlatSpec with Matchers{

  val properties: Properties = {
    val properties = new Properties()
    properties.load(getClass.getClassLoader.getResourceAsStream("epnnrt.properties"))
    properties
  }

  @transient private lazy val hadoopConf = {
    new Configuration()
  }

  private lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
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
      "{\"item_id\": \"103\",\"timestamp\": 1555290059210}," +
      "{\"item_id\": null,\"timestamp\": 1555280059208}]}," +
      " \"accountType\": \"CGUID\", \"userLinkCreationTime\": 0," +
      "\"userLinkLastModifiedDay\": 0,\"maskUserInfo\": \"d2ad\",\"userLink\": false},\"error\": null}," +
      "{\"results\":{\"response\": { \"view_item_list\":[" +
        "{\"item_id\": null,\"timestamp\": 1555292269032}," +
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
    val result = BullseyeUtils.getData(fs,"6018ed8b1720a9cc5e628468f7d256a5", "910" , "200", properties.getProperty("epnnrt.bullseyeUrl"))
    assert(result != None)
  }

//  it must "can parse response correctly with cguid" in {
//    val result = BullseyeUtils.getLastViewItem(fs,"6018ed8b1720a9cc5e628468f7d256a5", "1591169377921","910" , "200", properties.getProperty("epnnrt.bullseyeUrl"))
//    assert(result._1 == "250012780462")
//    assert(result._2 == "2020-06-03 15:29:37.92")
//  }

  "FetchClientSecret" must "work" in  {
    assert(BullseyeUtils.getSecretByClientId(properties.getProperty("epnnrt.clientId")) == "e884b0bd-8f38-4d1d-a161-f85038c3d0f3")
  }
}
