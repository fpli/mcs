package com.ebay.traffic.chocolate.sparknrt.epnnrt

import java.io.PrintWriter

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.couchbase.{CorpCouchbaseClient, CouchbaseClientMock}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class TestEpnNrtCommon extends BaseFunSuite{

  private val tmpPath = createTempPath()
  private val workDir = tmpPath + "/workDir/"
  private val resourceDir = tmpPath

  val schema = StructType(
    Seq(
      StructField("snapshot_id", LongType, nullable = true),
      StructField("timestamp", LongType, nullable = true),
      StructField("publisher_id", LongType, nullable = true),
      StructField("campaign_id", LongType, nullable = true),
      StructField("request_headers", StringType, nullable = true),
      StructField("uri", StringType, nullable = true),
      StructField("cguid", StringType, nullable = true),
      StructField("response_headers", StringType, nullable = true),
      StructField("rt_rule_flags", LongType, nullable = true),
      StructField("nrt_rule_flags", LongType, nullable = true),
      StructField("channel_action", StringType, nullable = true),
      StructField("channel_type", StringType, nullable = true),
      StructField("http_method", StringType, nullable = true),
      StructField("snid", StringType, nullable = true),
      StructField("is_tracked", BooleanType, nullable = true)
    )
  )

  val args = Array(
    "--mode", "local[8]",
    "--workDir", workDir,
    "--resourceDir", resourceDir,
    "--filterTime", "0"
  )
  val params = Parameter(args)

  var df: DataFrame = _
  var epnNrtCommon : EpnNrtCommon = _

  @transient lazy val spark = {
    val builder = SparkSession.builder().appName("Unit Test")

    builder.master("local[8]")
      .appName("SparkUnitTesting")
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir"))

    builder.getOrCreate()
  }

  /**
    * The spark context
    */
  @transient lazy val sc = {
    spark.sparkContext
  }

  /**
    * The sql context
    */
  @transient lazy val sqlsc = {
    spark.sqlContext
  }


  override def beforeAll(): Unit = {
    CouchbaseClientMock.startCouchbaseMock()
    CorpCouchbaseClient.getBucketFunc = () => {
      (None, CouchbaseClientMock.connect().openBucket("default"))
    }
    df = createTestChocolateData()
    epnNrtCommon = new EpnNrtCommon(params, df)
  }



  def createTestChocolateData(): DataFrame = {
    val rdd = sc.parallelize(
      Seq(
        Row(
          1L, 1L, 1L, 1L, "X-eBay-Client-IP: 1|cguid/1",
          "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338107049&item=132355040461&vectorid=229466&lgeo=1&dashenId=6432328199681789952&dashenCnt=0",
          "123", "",
          0L,
          0L,
          "CLICK", "EPN", "", "", false
        ),
        Row(
          1L, 1L, 1L, 1L, "X-eBay-Client-IP: 1|cguid/1",
          "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338107049&item=132355040461&vectorid=229466&lgeo=1&dashenId=6432328199681789952&dashenCnt=0",
          "123", "",
          0L,
          0L,
          "CLICK", "EPN", "", "", false
        ),
        Row(
          1L, 1L, 1L, 1L, "X-eBay-Client-IP: 1|cguid/1",
          "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5337666873&customid=&lgeo=1&vectorid=229466&item=222853652218&dashenId=6432328199681789952&dashenCnt=0",
          "123", "",
          0L,
          0L,
          "CLICK", "EPN", "", "", false
        ),
        Row(
          1L, 1L, 1L, 1L, "X-eBay-Client-IP: 1|cguid/1",
          "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5337666873&customid=&lgeo=1&vectorid=229466&item=222853652218&dashenId=6432328199681789952&dashenCnt=0",
          "123", "",
          0L,
          0L,
          "CLICK", "EPN", "", "", false
        )
      )
    )

    sqlsc.createDataFrame(rdd, schema)

  }

  test("Test get value from query URL(0 or 1)") {
    val value = epnNrtCommon.getValueFromQueryURL("http://www.ebay.com/1?isgeo=1&foo=bar", "isgeo")
    assert(value.equals("1"))
  }

  test("Test get ICEP Flex field(ffv 0 or 1)") {
    val value = epnNrtCommon.getIcepFlexFld("http://www.ebay.com/1?isgeo=1&icep_ffv=test", "2")
    assert(value.equals("0"))
  }

  test("Test get ICEP Flex field()") {
    val value = epnNrtCommon.getIcepFlexFld1("http://www.ebay.com/1?isgeo=1&icep_ff1=test", "ff1")
    assert(value.equals("test"))
  }

  test("Test get date time from timestamp") {
    val value = epnNrtCommon.getDateTimeFromTimestamp(1552328971000L, "yyyy-MM-dd")
    assert(value.equals("2019-03-12"))
  }

  test("Test get landing page url name") {
    val responseHeader = "Referer:http://translate.google.com.mx|X-Purpose:preview|Location:http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&dashenId=10044|Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8|Accept-Encoding:gzip, deflate, sdch|Accept-Language:en-US,en;q=0.8|Cookie:ebay=%5Esbf%3D%23%5E; nonsession=CgADLAAFY825/NQDKACBiWWj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDjrjVIf; dp1=bbl/USen-US5cb5ce77^; s=CgAD4ACBY9Lj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDhRBcIc; npii=btguid/92d9dfe51670a93d12831833fff1c1085ad49dd7^trm/svid%3D1136038334911271815ad49dd7^cguid/47a11c671620a93c91006917fffa2a915d116016^|Proxy-Connection:keep-alive|Upgrade-Insecure-Requests:1|X-EBAY-CLIENT-IP:10.108.159.177|User-Agent:Shuang-UP.Browser-baiduspider-ebaywinphocore"
    val res = epnNrtCommon.getLndPageUrlName(responseHeader)
    assert(res.equals("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2"))
  }

  test("Test get value from request") {
    val requestHeader = "Referer:http://translate.google.com.mx|X-Purpose:preview|Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8|Accept-Encoding:gzip, deflate, sdch|Accept-Language:en-US,en;q=0.8|Cookie:ebay=%5Esbf%3D%23%5E; nonsession=CgADLAAFY825/NQDKACBiWWj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDjrjVIf; dp1=bbl/USen-US5cb5ce77^; s=CgAD4ACBY9Lj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDhRBcIc; npii=btguid/92d9dfe51670a93d12831833fff1c1085ad49dd7^trm/svid%3D1136038334911271815ad49dd7^cguid/47a11c671620a93c91006917fffa2a915d116016^|Proxy-Connection:keep-alive|Upgrade-Insecure-Requests:1|X-EBAY-CLIENT-IP:10.108.159.177|User-Agent:Shuang-UP.Browser-baiduspider-ebaywinphocore"
    val res = epnNrtCommon.getValueFromRequest(requestHeader, "accept-language")
    assert(res.equals("en-US,en;q=0.8"))
  }

  test("Test remove params") {
    val location = "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&dashenId=10044&dashenCnt=2&xxx=4&pub=2"
    val res = epnNrtCommon.removeParams(location)
    assert(res.equals("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&xxx=4"))
  }

  test("Test get FFx value") {
    val uri = "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&dashenId=10044&dashenCnt=2&xxx=4&pub=2"
    val res = epnNrtCommon.getFFValue(uri, "3")
    assert(res.equals("2"))
  }

  test("Test get FFValue Not Empty") {
    val uri = "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&icep_ff2=10044&dashenCnt=2&xxx=4&pub=2"
    val res = epnNrtCommon.getFFValueNotEmpty(uri, "2")
    assert(res.equals("10044"))
  }

  test("test get Rover URI info") {
    val uri = "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&icep_ff2=10044&dashenCnt=2&xxx=4&pub=2"
    val rotation = epnNrtCommon.getRoverUriInfo(uri, 3)
    assert(rotation.equals("711-53200-19255-0"))
  }

  test("test get value from request") {
    val responseHeader = "Referer:http://translate.google.com.mx|X-Purpose:preview|Location:http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&dashenId=10044&mpre=http://www.amazon.com?xx=http://www.amazon.com|Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8|Accept-Encoding:gzip, deflate, sdch|Accept-Language:en-US,en;q=0.8|Cookie:ebay=%5Esbf%3D%23%5E; nonsession=CgADLAAFY825/NQDKACBiWWj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDjrjVIf; dp1=bbl/USen-US5cb5ce77^; s=CgAD4ACBY9Lj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDhRBcIc; npii=btguid/92d9dfe51670a93d12831833fff1c1085ad49dd7^trm/svid%3D1136038334911271815ad49dd7^cguid/47a11c671620a93c91006917fffa2a915d116016^|Proxy-Connection:keep-alive|Upgrade-Insecure-Requests:1|X-EBAY-CLIENT-IP:10.108.159.177|User-Agent:Shuang-UP.Browser-baiduspider-ebaywinphocore"
    val value = epnNrtCommon.getValueFromRequest(responseHeader, "location")
    assert(value.equals("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&dashenId=10044&mpre=http://www.amazon.com?xx=http://www.amazon.com"))
  }

  test("test get user query text") {
    val uri = "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&icep_ff2=10044&uq=2&xxx=4&pub=2"
    val res = epnNrtCommon.getUserQueryTxt(uri, "uq")
    assert(res.equals("2"))
  }

  test("test get error query param") {
    val uri = "http://www.ebay.com/itm/2323"
    val res = epnNrtCommon.getQueryParam(uri, "udid")
    assert(res.equals(""))
  }

  test("test get programId advertisedId from ams click") {
    val rotation = "5282-53200-19255-0"
    val res = epnNrtCommon.getPrgrmIdAdvrtsrIdFromAMSClick(rotation)
    assert(res(0).equals("2"))
    assert(res(1).equals("1"))
  }

  test("test get rule flag") {
    val rule = 9
    val index1 = epnNrtCommon.getRuleFlag(rule, 0)
    val index4 = epnNrtCommon.getRuleFlag(rule, 3)
    assert(index1 == 1)
    assert(index4 == 1)
  }

  test("test get country locale from header") {
    val requestHeader = "Referer:http://translate.google.com.mx|X-Purpose:preview|Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8|Accept-Encoding:gzip, deflate, sdch|Accept-Language:en-US,en;q=0.8|Cookie:ebay=%5Esbf%3D%23%5E; nonsession=CgADLAAFY825/NQDKACBiWWj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDjrjVIf; dp1=bbl/USen-US5cb5ce77^; s=CgAD4ACBY9Lj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDhRBcIc; npii=btguid/92d9dfe51670a93d12831833fff1c1085ad49dd7^trm/svid%3D1136038334911271815ad49dd7^cguid/47a11c671620a93c91006917fffa2a915d116016^|Proxy-Connection:keep-alive|Upgrade-Insecure-Requests:1|X-EBAY-CLIENT-IP:10.108.159.177|User-Agent:Shuang-UP.Browser-baiduspider-ebaywinphocore"
    val res = epnNrtCommon.getCountryLocaleFromHeader(requestHeader, "")
    assert(res.equals("US"))
  }

  test("test tool lvoptn") {
    val uri = "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&lego=1&uq=2&xxx=4&pub=2"
    val res = epnNrtCommon.getToolLvlOptn(uri)
    assert(res.equals("1"))
  }

  test("test get Item Id") {
    val uri = "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&icep_item_id=111&uq=2&xxx=4&pub=2"
    val res = epnNrtCommon.getItemId(uri)
    assert(res.equals("111"))
  }

  test("test get Item Id while invalid item Id") {
    val uri = "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&icep_item_id=QW112/&uq=2&xxx=4&pub=2"
    val res = epnNrtCommon.getItemId(uri)
    assert(res.equals("112"))
  }

  test("test get tool Id while invalid tool Id") {
    val uri = "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&icep_item_id=112/&toolid=20008/index.php/product/yongnuo-yn24ex-ttl-macro-ring-flash-led-macro-flash-speedlite-with-2-pcs-flash-head-and-4-pcs-adapter-rings-for-canon/&xxx=4&pub=2"
    val res = epnNrtCommon.getAms_tool_id(uri)
    assert(res.equals("20008"))
  }

  test("test get tool Id while invalid prefix tool Id") {
    val uri = "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&icep_item_id=112/&toolid=Q10001&xxx=4&pub=2"
    val res = epnNrtCommon.getAms_tool_id(uri)
    assert(res.equals("10001"))
  }

  test("test get ams program id while invalid rotation id") {
    val uri = "http://rover.ebay.com/rover/1/null/1?ff3=2&icep_item_id=112/&toolid=20008/index.php/product/yongnuo-yn24ex-ttl-macro-ring-flash-led-macro-flash-speedlite-with-2-pcs-flash-head-and-4-pcs-adapter-rings-for-canon/&xxx=4&pub=2"
    val res = epnNrtCommon.getAMSProgramId(uri)
    assert(res.equals(0))
  }

  test("test get valid param") {
    val test = "F1001"
    val res = epnNrtCommon.getValidParam(test)
    assert(res.equals("1001"))
  }

  test("test get valid param2") {
    val test = "1002ABC"
    val res = epnNrtCommon.getValidParam(test)
    assert(res.equals("1002"))
  }

  test("test get valid param3") {
    val test = "10A01"
    val res = epnNrtCommon.getValidParam(test)
    assert(res.equals("10"))
  }

  test("test get valid param4") {
    val test = "%1001"
    val res = epnNrtCommon.getValidParam(test)
    assert(res.equals("1001"))
  }

  test("test get valid param5") {
    val test = "ACDE"
    val res = epnNrtCommon.getValidParam(test)
    assert(res.equals(""))
  }


  test("test get traffic source code") {
    val browser = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36"
    val res = epnNrtCommon.get_TRFC_SRC_CD(browser, "click")
    assert(res == 0)
  }

  test("test get browser type") {
    val browser = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36"
    val res = epnNrtCommon.getBrowserType(browser)
    assert(res == 8)
  }

  test("test filter YN ind") {
    val rt_rule = 1024
    val nrt_rule = 129
    val res = epnNrtCommon.getFilter_Yn_Ind(rt_rule, nrt_rule, "click")
    assert(res == 1)
  }

  test("test get page map Id by landing page") {

    //create ams_landing_page_type_lookup.csv file
    import java.io.PrintWriter

    val printWriter = new PrintWriter(resourceDir + "/" + "ams_landing_page_type_lookup.csv")
    printWriter.println("Search Results\t362000\t2\t11\thttp://reise.shop.ebay.de/\t3077331\t1\t2/25/10\tSKHADER")
    printWriter.println("Search Results\t658500\t2\t15\thttp://motors.shop.ebay.co.uk/\t3003200\t1\t2/25/10\tSKHADER")
    printWriter.println("Item Page\t23500\t4\t4\thttp://cgi.ebay.com.au/\t4015015\t1\t2/25/10\tSKHADER")
    printWriter.println("Seller/Store Results\t794500\t3\t12\thttp://cgi6.ebay.it/ws/eBayISAPI.dll?ViewStoreV4&name=\t2101020\t1\t2/25/10\tSKHADER")
    printWriter.close()
    val url = "http://reise.shop.ebay.de/1?dw=3&ded=4"
    val rotation = "707-53200-19255-0"
    val res = epnNrtCommon.getPageIdByLandingPage(url, rotation)
    assert(res.equals("362000"))
  }

  test("test lookup referer domain") {
    val df = createTestChocolateData()
    val epnNrtCommon = new EpnNrtCommon(params, df)

    val printWriter = new PrintWriter(resourceDir + "/" + "ams_rfrng_dmn_pblshr_map.csv")
    printWriter.println("877\twww.google.al\t5574633013\t1\t2/18/13\tYAJI_DBA")
    printWriter.println("4692\twww.google.fi\t5574737088\t1\t3/26/14\tSHIDLEKAR_DBA")
    printWriter.close()

    val url = "www.google.fi"
    val res = epnNrtCommon.lookupRefererDomain(url, true, "5574737088")
    assert(res == 1)
  }

  test("test call roi ebay referrer rule") {
    val res = epnNrtCommon.callRoiEbayReferrerRule(1, 1, 0)
    assert(res == 0)
  }

  test("test call roi Nq blacklist rule") {
    val res = epnNrtCommon.callRoiNqBlacklistRule(1, 1, 1)
    assert(res == 1)
  }

  test("test call roi missing referrer url rule") {
    val res = epnNrtCommon.callRoiMissingReferrerUrlRule(1, 1, "")
    assert(res == 1)
  }

  test("test is Defined AdvertiserId") {
    val res = epnNrtCommon.isDefinedAdvertiserId("711-121-121-121")
    assert(res)
  }

  test("test is defined publisher") {
    val res = epnNrtCommon.isDefinedPublisher("5574737088")
    assert(res)
  }

  test("test call roi sdk rule") {
    val res = epnNrtCommon.callRoiSdkRule(1, 1, 0)
    assert(res == 1)
  }

}
