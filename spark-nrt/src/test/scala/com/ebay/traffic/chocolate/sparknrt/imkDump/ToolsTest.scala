package com.ebay.traffic.chocolate.sparknrt.imkDump

import org.scalatest.FunSuite

/**
  * Created by ganghuang on 12/3/18.
  */
class ToolsTest extends FunSuite {

  test("test getDateTimeFromTimestamp") {
    assert(Tools.getDateFromTimestamp(1531377313068L) == "2018-07-12")
    assert(Tools.getDateTimeFromTimestamp(1531377313068L) == "2018-07-12 14:35:13.068")
  }

  test("test getQueryString") {
    assert(Tools.getQueryString("http://www.ebay.com?a=test") == "a=test")
    assert(Tools.getQueryString("http://www.ebay.com") == "")
    assert(Tools.getQueryString("http://www.ebay.com?") == "")
    assert(Tools.getQueryString("http://www.ebay.com?a=test&b=test2") == "a=test&b=test2")
  }

  test("test getParamValueFromUrl") {
    assert(Tools.getParamValueFromUrl("http://www.ebay.com?a=test", "a") == "test")
    assert(Tools.getParamValueFromUrl("http://www.ebay.com?a=test&b=test2", "a") == "test")
    assert(Tools.getParamValueFromUrl("http://www.ebay.com", "a") == "")
    assert(Tools.getParamValueFromUrl("http://www.ebay.com?a=test=test2", "a") == "")
    assert(Tools.getParamValueFromUrl("http://www.ebay.com?A=test", "a") == "test")
  }

  test("test getDefaultNullNumParamValueFromUrl") {
    assert(Tools.getDefaultNullNumParamValueFromUrl("http://www.ebay.com?a=123", "a") == "123")
    assert(Tools.getDefaultNullNumParamValueFromUrl("http://www.ebay.com?a=", "a") == "")
    assert(Tools.getDefaultNullNumParamValueFromUrl("http://www.ebay.com?a=123a", "a") == "")
    assert(Tools.getDefaultNullNumParamValueFromUrl("http://www.ebay.com", "a") == "")
  }

  test("test getUserMapInd") {
    assert(Tools.getUserMapInd("gang") == "1")
    assert(Tools.getUserMapInd("") == "0")
    assert(Tools.getUserMapInd("0") == "0")
  }

  test("test getClientIdFromRotationId") {
    assert(Tools.getClientIdFromRotationId("123") == "")
    assert(Tools.getClientIdFromRotationId("711-123-223") == "711")
    assert(Tools.getClientIdFromRotationId("") == "")
  }

  test("test getItemIdFromUri") {
    assert(Tools.getItemIdFromUri("http://www.ebay.com/itm/aaa/123") == "123")
    assert(Tools.getItemIdFromUri("http://www.ebay.com/itm/123") == "123")
    assert(Tools.getItemIdFromUri("http://www.ebay.com/itm/aaa/123a") == "")
    assert(Tools.getItemIdFromUri("http://www.ebay.com/i/aaa/123") == "123")
    assert(Tools.getItemIdFromUri("http://www.ebay.com/item/aaa/123") == "")
  }

  test("test getBrowserType") {
    assert(Tools.getBrowserType("Referer:http://www.google.com")
    == -99)
    assert(Tools.getBrowserType("User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")
    == 8)
  }

  test("test getParamFromQuery") {
    val keywordParams: Array[String] = Array("_nkw")
    assert(Tools.getParamFromQuery("http://www.ebay.com?_nkw=apple", keywordParams) == "apple")
    assert(Tools.getParamFromQuery("http://www.ebay.com?nkw=apple", keywordParams) == "")
    assert(Tools.getParamFromQuery("http://www.ebay.com?_Nkw=apple", keywordParams) == "apple")
  }

  test("test getCommandType") {
    assert(Tools.getCommandType("IMPRESSION") == "4")
    assert(Tools.getCommandType("CLICK") == "1")
    assert(Tools.getCommandType("") == "1")
  }

  test("test getOutPutFileDate") {
    assert(Tools.getOutPutFileDate.length == 15)
  }

  test("test getBatchId") {
    println(Tools.getBatchId)
  }

  test("test getDomain") {
    assert(Tools.getDomain("http://www.ebay.com") == "www.ebay.com")
    assert(Tools.getDomain("") == "")
  }


}
