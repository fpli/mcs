package com.ebay.traffic.chocolate.sparknrt.imkDump

import org.scalatest.FunSuite

/**
  * Created by ganghuang on 12/3/18.
  */
class UtilsTest extends FunSuite {

  test("test getDateTimeFromTimestamp") {
    assert(Utils.getDateFromTimestamp(1531377313068L) == "2018-07-12")
    assert(Utils.getDateTimeFromTimestamp(1531377313068L) == "2018-07-12 14:35:13.068")
  }

  test("test getQueryString") {
    assert(Utils.getQueryString("http://www.ebay.com?a=test") == "a=test")
    assert(Utils.getQueryString("http://www.ebay.com") == "")
    assert(Utils.getQueryString("http://www.ebay.com?") == "")
    assert(Utils.getQueryString("http://www.ebay.com?a=test&b=test2") == "a=test&b=test2")
  }

  test("test getParamValueFromUrl") {
    assert(Utils.getParamValueFromUrl("http://www.ebay.com?a=test", "a") == "test")
    assert(Utils.getParamValueFromUrl("http://www.ebay.com?a=test&b=test2", "a") == "test")
    assert(Utils.getParamValueFromUrl("http://www.ebay.com", "a") == "")
    assert(Utils.getParamValueFromUrl("http://www.ebay.com?a=test=test2", "a") == "")
    assert(Utils.getParamValueFromUrl("http://www.ebay.com?A=test", "a") == "test")
  }

  test("test getDefaultNullNumParamValueFromUrl") {
    assert(Utils.getDefaultNullNumParamValueFromUrl("http://www.ebay.com?a=123", "a") == "123")
    assert(Utils.getDefaultNullNumParamValueFromUrl("http://www.ebay.com?a=", "a") == "")
    assert(Utils.getDefaultNullNumParamValueFromUrl("http://www.ebay.com?a=123a", "a") == "")
    assert(Utils.getDefaultNullNumParamValueFromUrl("http://www.ebay.com", "a") == "")
  }

  test("test getUserMapInd") {
    assert(Utils.getUserMapInd("gang") == "1")
    assert(Utils.getUserMapInd("") == "0")
    assert(Utils.getUserMapInd("0") == "0")
  }

  test("test getClientIdFromRotationId") {
    assert(Utils.getClientIdFromRotationId("123") == "")
    assert(Utils.getClientIdFromRotationId("711-123-223") == "711")
    assert(Utils.getClientIdFromRotationId("") == "")
  }

  test("test getItemIdFromUri") {
    assert(Utils.getItemIdFromUri("http://www.ebay.com/itm/aaa/123") == "123")
    assert(Utils.getItemIdFromUri("http://www.ebay.com/itm/123") == "123")
    assert(Utils.getItemIdFromUri("http://www.ebay.com/itm/aaa/123a") == "")
    assert(Utils.getItemIdFromUri("http://www.ebay.com/i/aaa/123") == "123")
    assert(Utils.getItemIdFromUri("http://www.ebay.com/item/aaa/123") == "")
  }

  test("test getBrowserType") {
    assert(Utils.getBrowserType("Referer:http://www.google.com")
    == -99)
    assert(Utils.getBrowserType("User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")
    == 8)
  }

  test("test getParamFromQuery") {
    val keywordParams: Array[String] = Array("_nkw")
    assert(Utils.getParamFromQuery("http://www.ebay.com?_nkw=apple", keywordParams) == "apple")
    assert(Utils.getParamFromQuery("http://www.ebay.com?nkw=apple", keywordParams) == "")
    assert(Utils.getParamFromQuery("http://www.ebay.com?_Nkw=apple", keywordParams) == "apple")
  }

  test("test getCommandType") {
    assert(Utils.getCommandType("IMPRESSION") == "4")
    assert(Utils.getCommandType("CLICK") == "1")
    assert(Utils.getCommandType("") == "1")
  }

  test("test getOutPutFileDate") {
    assert(Utils.getOutPutFileDate.length == 15)
  }

  test("test getBatchId") {
    println(Utils.getBatchId)
  }

  test("test getDomain") {
    assert(Utils.getDomain("http://www.ebay.com") == "www.ebay.com")
    assert(Utils.getDomain("") == "")
  }

  test("test judgeNotEbaySites") {
    assert(!Utils.judgeNotEbaySites("http://www.ebay.com"))
    assert(!Utils.judgeNotEbaySites("www.ebay.com"))
    assert(Utils.judgeNotEbaySites("http://www.google.com"))
  }
}
