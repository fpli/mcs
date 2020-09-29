package com.ebay.traffic.chocolate.sparknrt.imkDump

import java.net.URL

import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum
import org.scalatest.FunSuite

/**
  * Created by ganghuang on 12/3/18.
  */
class ToolsTest extends FunSuite {
  val tools = new Tools("ut", "http://10.148.181.34:9200")
  
  test("test getDateTimeFromTimestamp") {
    assert(tools.getDateFromTimestamp(1531377313068L) == "2018-07-12")
    assert(tools.getDateTimeFromTimestamp(1531377313068L) == "2018-07-12 06:35:13.068")
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
    assert(tools.getCommandType("SERVE") == "4")
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
    // /ulk/ should be treated as external
    assert(tools.judgeNotEbaySites("www.ebay.com/ulk/sch/?_nkw=iphone+cases&mkevt=1&mkrid=123&mkcid=2&keyword=testkeyword&crlp=123&MT_ID=1geo_id=123&rlsatarget=123&adpos=1&device=m&loc=1&poi=1&abcId=1&cmpgn=123&sitelnk=123&test=XiangMobile"))
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

  test("test getClientIdFromRoverUrl") {
    assert(tools.getClientIdFromRoverUrl("https://rover.ebay.com/roverroi/1/711-518-1801-10?mpuid=187937644;223525488837;2288208264012;&siteId=0&BIN-Store=1&ff1=ss&ff2=CHECKOUT") == "0")
  }

  test("test get ROI ids") {
    val query = new URL("https://rover.ebay.com/roverroi/1/711-518-1801-10?mpuid=187937644;223525488837;2288208264012;&siteId=0&BIN-Store=1&ff1=ss&ff2=CHECKOUT").getQuery
    assert(tools.getRoiIdFromUrlQuery(0, query) == "187937644")
    assert(tools.getRoiIdFromUrlQuery(1, query) == "223525488837")
    assert(tools.getRoiIdFromUrlQuery(2, query) == "2288208264012")

  }

  test("test getUserQueryFromRef") {
    val testQuery = tools.getQueryString("https://rover.ebay.com/roverns/1/710-16388-7832-0?mpt=1572370403910&mpcl=https%3A%2F%2Fwww.ebay.co.uk%2F&mpvl=https%3A%2F%2Fwww.bing.com%2Fsearch%3FQ%3DeBay%26form%3DEDNTHT%26mkt%3Den-gb%26httpsmsn%3D1%26msnews%3D1%26plvar%3D0%26refig%3Dde0359d2287f4fc6f0d43c815214d153%26sp%3D1%26qs%3DAS%26pq%3Deb%26sk%3DPRES1%26sc%3D8-2%26cvid%3Dde0359d2287f4fc6f0d43c815214d153%26cc%3DGB%26setlang%3Den-GB")
    val ref = tools.getDecodeParamUrlValueFromQuery(testQuery, "mpvl")
    val userQuery = tools.getUserQueryFromRef(ref)
    val refererDomainName = tools.getDomain(ref)

    assert(ref == "https://www.bing.com/search?Q=eBay&form=EDNTHT&mkt=en-gb&httpsmsn=1&msnews=1&plvar=0&refig=de0359d2287f4fc6f0d43c815214d153&sp=1&qs=AS&pq=eb&sk=PRES1&sc=8-2&cvid=de0359d2287f4fc6f0d43c815214d153&cc=GB&setlang=en-GB")
    assert(userQuery == "ebay")
    assert(refererDomainName == "www.bing.com")
  }

  test("test getLandingPageUrlFromUriOrRfrr") {
    val testQuery = tools.getQueryString("https://rover.ebay.com/roverns/1/710-16388-7832-0?mpt=1572370403910&mpcl=https%3A%2F%2Fwww.ebay.co.uk%2F&mpvl=https%3A%2F%2Fwww.bing.com%2Fsearch%3Fq%3Debay%26form%3DEDNTHT%26mkt%3Den-gb%26httpsmsn%3D1%26msnews%3D1%26plvar%3D0%26refig%3Dde0359d2287f4fc6f0d43c815214d153%26sp%3D1%26qs%3DAS%26pq%3Deb%26sk%3DPRES1%26sc%3D8-2%26cvid%3Dde0359d2287f4fc6f0d43c815214d153%26cc%3DGB%26setlang%3Den-GB")
    val pageOne = tools.getDecodeParamUrlValueFromQuery(testQuery, "mpcl")
    val pageTwo = tools.getDecodeParamUrlValueFromQuery("https://rover.ebay.com/roverns/1/710-16388-7832-0?mpt=1572370403910", "mpcl")
    val referer = "https://www.ebay.co.uk/"

    assert(tools.getLandingPageUrlFromUriOrRfrr(pageOne, referer) == "https://www.ebay.co.uk/")
    assert(tools.getLandingPageUrlFromUriOrRfrr(pageTwo, referer) == referer)
  }

  test ("test getDecodePerfTrackNameValue") {
    val testQuery = tools.getQueryString("https://rover.ebay.com/roverns/1/710-16388-7832-0?mpt=1572370403910&mpcl=https%3A%2F%2Fwww.ebay.co.uk%2F&mpvl=https%3A%2F%2Fwww.bing.com%2Fsearch%3Fq%3Debay%26form%3DEDNTHT%26mkt%3Den-gb%26httpsmsn%3D1%26msnews%3D1%26plvar%3D0%26refig%3Dde0359d2287f4fc6f0d43c815214d153%26sp%3D1%26qs%3DAS%26pq%3Deb%26sk%3DPRES1%26sc%3D8-2%26cvid%3Dde0359d2287f4fc6f0d43c815214d153%26cc%3DGB%26setlang%3Den-GB")
    assert(tools.getDecodePerfTrackNameValue(testQuery) == "^mpt=1572370403910^mpcl=https://www.ebay.co.uk/^mpvl=https://www.bing.com/search?q=ebay&form=EDNTHT&mkt=en-gb&httpsmsn=1&msnews=1&plvar=0&refig=de0359d2287f4fc6f0d43c815214d153&sp=1&qs=AS&pq=eb&sk=PRES1&sc=8-2&cvid=de0359d2287f4fc6f0d43c815214d153&cc=GB&setlang=en-GB")
  }

}
