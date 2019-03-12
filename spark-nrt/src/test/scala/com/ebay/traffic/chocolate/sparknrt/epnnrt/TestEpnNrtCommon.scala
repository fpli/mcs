package com.ebay.traffic.chocolate.sparknrt.epnnrt

import java.text.SimpleDateFormat

import com.ebay.app.raptor.chocolate.avro.{ChannelAction, ChannelType, FilterMessage}
import com.ebay.traffic.chocolate.common.TestHelper
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class TestEpnNrtCommon extends BaseFunSuite{

  private val tmpPath = createTempPath()
  private val inputDir = tmpPath + "/inputDir/"
  private val workDir = tmpPath + "/workDir/"
  private val resourceDir = tmpPath

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

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
  //  createTestDataForEPN()
  /*  val args = Array(
      "--mode", "local[8]",
      "--workDir", workDir,
      "--resourceDir", resourceDir
    )
    val params = Parameter(args)
    val df = createTestChocolateData()*/

  }

  def createTestDataForEPN(): Unit = {
  /*  val metadata = Metadata(workDir, "EPN", MetadataEnum.capping)
    val dateFiles1 = DateFiles("date=2018-05-01", Array("file://" + inputDir + "/date=2018-05-01/part-00000.snappy.parquet",
      "file://" + inputDir + "/date=2018-05-01/part-00001.snappy.parquet"))
    val dateFiles2 = DateFiles("date=2018-05-02", Array("file://" + inputDir + "/date=2018-05-02/part-00000.snappy.parquet"))

    val meta: MetaFiles = MetaFiles(Array(dateFiles1,dateFiles2))
    metadata.writeDedupeOutputMeta(meta, Array(".epnnrt"))

    // prepare data file
    val writer1 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-05-01/part-00000.snappy.parquet"))
      .withSchema(FilterMessage.getClassSchema)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val writer2 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-05-01/part-00001.snappy.parquet"))
      .withSchema(FilterMessage.getClassSchema)
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val writer3 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-05-02/part-00000.snappy.parquet"))
      .withSchema(FilterMessage.getClassSchema)
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val timestamp = getTimestamp("2018-05-01")

    // Desktop
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 6457493984045429247L, 56826256L, 9000081120L, "76cbd9ea15b0a93d12831833fff1c1065ad49dd7^", timestamp - 12, writer1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 1109090984045429247L, 7000001727L, 9000028992L, "12cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", timestamp - 12, writer1)

    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 7817281212121239247L, 7000001564L, -1L, "34cbd9iqoiwjddws09ydwa33fff1c1065ad49dd7^", timestamp - 8, writer1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 2902129817128329247L, 7000000007L, -1L, "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", timestamp - 7,  writer1)
    writer1.close()

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 6457493984045429247L, 7000001711L, -1L, "76cbd9ea15b0a93d12831833fff1c1065ad49dd7^", timestamp - 12, writer2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.SERVE, 1109090984045429247L, 7000001262L, -1L, "12cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", timestamp - 12, writer2)

    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 7817281212121239247L, 7000001556L, -1L, "34cbd9iqoiwjddws09ydwa33fff1c1065ad49dd7^", timestamp - 8, writer2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.VIEWABLE, 2902129817128329247L, 7000001538L, -1L, "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", timestamp - 7,  writer2)
    writer2.close()

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 6457493984045429247L, 7000001262L, -1L, "76cbd9ea15b0a93d12831833fff1c1065ad49dd7^", timestamp - 12, writer3)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 1109090984045429247L, 7000001531L, -1L, "12cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", timestamp - 12, writer3)

    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 7817281212121239247L, 7000001285L, -1L, "34cbd9iqoiwjddws09ydwa33fff1c1065ad49dd7^", timestamp - 8, writer3)
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 2902129817128329247L, 7000001727L, 9000052575L, "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", timestamp - 7,  writer3)
    writer3.close()



    //create ams_landing_page_type_lookup.csv file
    import java.io.PrintWriter
    var printWriter = new PrintWriter(resourceDir + "/" + "ams_rfrng_dmn_pblshr_map.csv")
    printWriter.println("471\ttranslate.google.com.mx\t5574665384\t1\t2/18/13\tYAJI_DBA")
    printWriter.println("248\tmaps.google.de\t5574737746\t1\t2/18/13\tYAJI_DBA ")
    printWriter.println("6038\twww.google.si\t5574643520\t1\t3/26/14\tSHIDLEKAR_DBA ")
    printWriter.println("3691\twww.google.com.ng\t5574630834\t1\t3/26/14\tSHIDLEKAR_DBA")
    printWriter.println("2957\twww.google.kz\t5574674899\t1\t2/18/13\tYAJI_DBA")
    printWriter.println("5243\twww.google.jo\t5575042380\t1\t3/26/14\tSHIDLEKAR_DBA")
    printWriter.println("1594\twww.google.com.bh\t5574636783\t1\t2/18/13\tYAJI_DBA")
    printWriter.close()

    printWriter = new PrintWriter(resourceDir + "/" + "ams_landing_page_type_lookup.csv")
    printWriter.println("Search Results\t539000\t2\t2\thttp://local-services.shop.ebay.ie/items/\t3099180\t1\t2/25/10\tSKHADER")
    printWriter.println("Search Results\t658500\t2\t15\thttp://motors.shop.ebay.co.uk/\t3003200\t1\t2/25/10\tSKHADER")
    printWriter.println("Item Page\t23500\t4\t4\thttp://cgi.ebay.com.au/\t4015015\t1\t2/25/10\tSKHADER")
    printWriter.println("Seller/Store Results\t794500\t3\t12\thttp://cgi6.ebay.it/ws/eBayISAPI.dll?ViewStoreV4&name=\t2101020\t1\t2/25/10\tSKHADER")
    printWriter.close()*/
  }

  def writeFilterMessage(channelType: ChannelType,
                         channelAction: ChannelAction,
                         snapshotId: Long,
                         publisherId: Long,
                         campaignId: Long,
                         cguid: String,
                         timestamp: Long,
                         writer: ParquetWriter[GenericRecord]): FilterMessage = {
    val message = TestHelper.newFilterMessage(channelType,
      channelAction,
      snapshotId,
      publisherId,
      campaignId,
      cguid,
      timestamp)
    writer.write(message)
    message
  }

  def getTimestamp(date: String): Long = {
    sdf.parse(date).getTime
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
    val df = createTestChocolateData()
    val epnNrtCommon = new EpnNrtCommon(params, df)
    val value = epnNrtCommon.getValueFromQueryURL("http://www.ebay.com/1?isgeo=1&foo=bar", "isgeo")
    assert(value.equalsIgnoreCase("1"))
  }

  test("Test get ICEP Flex field(ffv 0 or 1)") {
    val df = createTestChocolateData()
    val epnNrtCommon = new EpnNrtCommon(params, df)
    val value = epnNrtCommon.getIcepFlexFld("http://www.ebay.com/1?isgeo=1&icep_ffv=test", "2")
    assert(value.equalsIgnoreCase("0"))
  }

  test("Test get ICEP Flex field()") {
    val df = createTestChocolateData()
    val epnNrtCommon = new EpnNrtCommon(params, df)
    val value = epnNrtCommon.getIcepFlexFld1("http://www.ebay.com/1?isgeo=1&icep_ff1=test", "ff1")
    assert(value.equalsIgnoreCase("test"))
  }

  test("Test get date time from timestamp") {
    val df = createTestChocolateData()
    val epnNrtCommon = new EpnNrtCommon(params, df)
    val value = epnNrtCommon.getDateTimeFromTimestamp(1552328971000L, "yyyy-MM-dd")
    assert(value.equalsIgnoreCase("2019-03-12"))
  }

  test("Test get landing page url name") {
    val df = createTestChocolateData()
    val epnNrtCommon = new EpnNrtCommon(params, df)
  }

  test("Test filter By Timestamp") {
    val df = createTestChocolateData()
    val epnNrtCommon = new EpnNrtCommon(params, df)
    val res = epnNrtCommon.filterByTimestamp("1548137796000")
    assert(res.equals(false))
  }


}
