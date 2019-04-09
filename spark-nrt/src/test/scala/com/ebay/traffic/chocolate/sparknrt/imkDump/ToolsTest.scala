package com.ebay.traffic.chocolate.sparknrt.imkDump

import org.scalatest.FunSuite

/**
  * Created by ganghuang on 12/3/18.
  */
class ToolsTest extends FunSuite {
  val tools = new Tools(null, null)
  
  test("test getDateTimeFromTimestamp") {
    assert(tools.getDateFromTimestamp(1531377313068L) == "2018-07-12")
    assert(tools.getDateTimeFromTimestamp(1531377313068L) == "2018-07-12 14:35:13.068")
  }

  test("test getQueryString") {
    assert(tools.getQueryString("http://www.ebay.com?a=test") == "a=test")
    assert(tools.getQueryString("http://www.ebay.com") == "")
    assert(tools.getQueryString("http://www.ebay.com?") == "")
    assert(tools.getQueryString("http://www.ebay.com?a=test&b=test2") == "a=test&b=test2")
  }

  test("test getParamValueFromUrl") {
    assert(tools.getParamValueFromQuery("a=test", "a") == "test")
    assert(tools.getParamValueFromQuery("a=test&b=test2", "a") == "test")
    assert(tools.getParamValueFromQuery("", "a") == "")
    assert(tools.getParamValueFromQuery("a=test=test2", "a") == "")
    assert(tools.getParamValueFromQuery("A=test", "a") == "test")
  }

  test("test getDefaultNullNumParamValueFromUrl") {
    assert(tools.getDefaultNullNumParamValueFromQuery("a=123", "a") == "123")
    assert(tools.getDefaultNullNumParamValueFromQuery("a=", "a") == "")
    assert(tools.getDefaultNullNumParamValueFromQuery("a=123a", "a") == "")
    assert(tools.getDefaultNullNumParamValueFromQuery("", "a") == "")
  }

  test("test getUserMapInd") {
    assert(tools.getUserMapInd("gang") == "1")
    assert(tools.getUserMapInd("") == "0")
    assert(tools.getUserMapInd("0") == "0")
  }

  test("test getClientIdFromRotationId") {
    assert(tools.getClientIdFromRotationId("123") == "")
    assert(tools.getClientIdFromRotationId("711-123-223") == "711")
    assert(tools.getClientIdFromRotationId("") == "")
  }

  test("test getItemIdFromUri") {
    assert(tools.getItemIdFromUri("http://www.ebay.com/itm/aaa/123") == "123")
    assert(tools.getItemIdFromUri("http://www.ebay.com/itm/123") == "123")
    assert(tools.getItemIdFromUri("http://www.ebay.com/itm/aaa/123a") == "")
    assert(tools.getItemIdFromUri("http://www.ebay.com/i/aaa/123") == "123")
    assert(tools.getItemIdFromUri("http://www.ebay.com/item/aaa/123") == "")
  }

  test("test getBrowserType") {
    assert(tools.getBrowserType("Referer:http://www.google.com")
    == -99)
    assert(tools.getBrowserType("User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")
    == 8)
  }

  test("test getParamFromQuery") {
    val keywordParams: Array[String] = Array("_nkw")
    assert(tools.getParamFromQuery("_nkw=apple", keywordParams) == "apple")
    assert(tools.getParamFromQuery("nkw=apple", keywordParams) == "")
    assert(tools.getParamFromQuery("_Nkw=apple", keywordParams) == "apple")
  }

  test("test getCommandType") {
    assert(tools.getCommandType("IMPRESSION") == "4")
    assert(tools.getCommandType("CLICK") == "1")
    assert(tools.getCommandType("") == "1")
  }

  test("test getOutPutFileDate") {
    assert(tools.getOutPutFileDate.length == 15)
  }

  test("test getBatchId") {
    println(tools.getBatchId)
  }

  test("test getDomain") {
    assert(tools.getDomain("http://www.ebay.com") == "www.ebay.com")
    assert(tools.getDomain("") == "")
  }

  test("test judgeNotEbaySites") {
    assert(!tools.judgeNotEbaySites("http://www.ebay.com"))
    assert(!tools.judgeNotEbaySites("www.ebay.com"))
    assert(tools.judgeNotEbaySites("http://www.google.com"))
  }

  test("test getChannelType") {
    assert(tools.getChannelType("EPN") == "1")
    assert(tools.getChannelType("OTHER") == "0")
  }

  test("test getPerfTrackNameValue") {
    assert(tools.getPerfTrackNameValue("a=123") == "^a=123")
    assert(tools.getPerfTrackNameValue("") == "")
    assert(tools.getPerfTrackNameValue("a=123&b=123") == "^a=123^b=123")
  }

}
