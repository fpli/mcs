package com.ebay.traffic.chocolate.sparknrt.epnnrt

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.couchbase.{CorpCouchbaseClient, CouchbaseClientMock}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

class TestUtils extends BaseFunSuite{
  private val tmpPath = createTempPath()
  private val workDir = tmpPath + "/workDir/"
  private val resourceDir = tmpPath

  val args = Array(
    "--mode", "local[8]",
    "--workDir", workDir,
    "--resourceDir", resourceDir,
    "--filterTime", "1552382488000"
  )
  val params = Parameter(args)

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

  override def beforeAll(): Unit = {
    /* df = createTestChocolateData()
     epnNrtCommon = new EpnNrtCommon(params, df)*/
    CouchbaseClientMock.startCouchbaseMock()
    CorpCouchbaseClient.getBucketFunc = () => {
      (None, CouchbaseClientMock.connect().openBucket("default"))
    }
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


  test("test call roi ebay referrer rule") {
    val df = createTestChocolateData()
    val epnNrtCommon = new EpnNrtCommon(params, df)
    val res = epnNrtCommon.callRoiEbayReferrerRule(1, 1, 0)
    assert(res == 0)
  }

  test("test call roi Nq blacklist rule") {
    val df = createTestChocolateData()
    val epnNrtCommon = new EpnNrtCommon(params, df)
    val res = epnNrtCommon.callRoiNqBlacklistRule(1, 1, 1)
    assert(res == 1)
  }

  test("test call roi missing referrer url rule") {
    val df = createTestChocolateData()
    val epnNrtCommon = new EpnNrtCommon(params, df)
    val res = epnNrtCommon.callRoiMissingReferrerUrlRule(1, 1, "")
    assert(res == 1)
  }

  test("test is Defined AdvertiserId") {
    val df = createTestChocolateData()
    val epnNrtCommon = new EpnNrtCommon(params, df)
    val res = epnNrtCommon.isDefinedAdvertiserId("711-121-121-121")
    assert(res)
  }

  test("test is defined publisher") {
    val df = createTestChocolateData()
    val epnNrtCommon = new EpnNrtCommon(params, df)
    val res = epnNrtCommon.isDefinedPublisher("5574737088")
    assert(res)
  }

}
