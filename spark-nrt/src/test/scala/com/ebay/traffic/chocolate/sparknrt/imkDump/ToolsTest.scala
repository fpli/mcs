package com.ebay.traffic.chocolate.sparknrt.imkDump

import org.scalatest.FunSuite

class ToolsTest extends FunSuite {

  test("test getDateTimeFromTimestamp") {
    assert(Tools.getDateFromTimestamp(1531377313068L) == "2018-07-12")
    assert(Tools.getDateTimeFromTimestamp(1531377313068L) == "2018-07-12 14:35:13.068")
  }

  test("test getValueFromRequestHeader") {
    assert(Tools.getValueFromRequestHeader("Referer:http://www.google.com|User-Agent:Shuang-UP.Browser|Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8|Accept-Encoding:gzip, deflate, sdch|Accept-Language:en-US,en;q=0.8|Cookie:ebay=%5Esbf%3D%23%5E; nonsession=CgADLAAFY825/NQDKACBiWWj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDjrjVIf; dp1=bbl/USen-US5cb5ce77^; s=CgAD4ACBY9Lj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDhRBcIc; npii=btguid/76cbd9ea15b0a93d12831833fff1c1085ad49dd7^trm/svid%3D1136038334911271815ad49dd7^cguid/76cbd9ea15b0a93d12831833fff1c1065ad49dd7^|Proxy-Connection:keep-alive|Upgrade-Insecure-Requests:1|X-EBAY-CLIENT-IP:157.55.39.67", "Referer")
      == "http://www.google.com")
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

  test("test getUserIdFromHeader") {
    assert(Tools.getUserIdFromHeader("X-EBAY-C-ENDUSERCTX:userid=gang") == "gang")
    assert(Tools.getUserIdFromHeader("X-EBAY-C-ENDUSERCTX:loc=ch, userid=gang") == "gang")
    assert(Tools.getUserIdFromHeader("X-EBAY-C-ENDUSERCTX:userid=0") == "0")
    assert(Tools.getUserIdFromHeader("X-EBAY-C-ENDUSERCTX:loc=ch") == "")
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

  test("test getCGuidFromCookie") {
    assert(Tools.getCGuidFromCookie("Cookie:ebay=%5Esbf%3D%23%5E; nonsession=CgADLAAFY825/NQDKACBiWWj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDjrjVIf; dp1=bbl/USen-US5cb5ce77^; s=CgAD4ACBY9Lj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDhRBcIc; npii=btguid/76cbd9ea15b0a93d12831833fff1c1085ad49dd7^trm/svid%3D1136038334911271815ad49dd7^cguid/76cbd9ea15b0a93d12831833fff1c1065ad49dd7^", "cguid")
    == "76cbd9ea15b0a93d12831833fff1c106")
    assert(Tools.getCGuidFromCookie("Cookie:ebay=%5Esbf%3D%23%5E; nonsession=CgADLAAFY825/NQDKACBiWWj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDjrjVIf; dp1=bbl/USen-US5cb5ce77^; s=CgAD4ACBY9Lj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDhRBcIc; npii=btguid/76cbd9ea15b0a93d12831833fff1c1085ad49dd7^trm/svid%3D1136038334911271815ad49dd7^cguid/76cbd9ea15b0a93d12831833fff1c1065ad49dd7^", "guid")
    == "6cbd9ea15b0a93d12831833fff1c1085")
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

  test("test getSvidFromCookie") {
    assert(Tools.getSvidFromCookie("Cookie: npii=btguid/2fe65cc415c0ab1ca77184baf77400565d8c95b4^trm/mojo1%3Ds%2F005121%2F20%3B+svid%3D1108043186490388215d8c95b4^cguid/2fe65cc415c0ab1ca77184baf77400545d8c95b4^|X-eBay-Web-Tier-IP: 10.164.225.21")
      == "110804318649038821")
  }

  test("test convertRotationId") {
    assert(Tools.convertRotationId("711-15380-6615-0630") == "7111538066150630")
    assert(Tools.convertRotationId("711-15380-6615-06303333333") == "")
    assert(Tools.convertRotationId("711-15380-6615-063a0") == "")
  }

}
