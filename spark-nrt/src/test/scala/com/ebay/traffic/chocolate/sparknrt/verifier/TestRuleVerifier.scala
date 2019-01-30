package com.ebay.traffic.chocolate.sparknrt.verifier

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

class TestRuleVerifier extends BaseFunSuite {

  private val tmpPath = createTempPath()
  private val srcPath = tmpPath + "/inputDir1/"
  private val targetPath = tmpPath + "/inputDir2/"
  private val outputPath = tmpPath + "/outputDir/result.txt"

  private val args = Array(
    "--mode", "local[8]",
    "--srcPath", srcPath,
    "--targetPath", targetPath,
    "--outputPath", outputPath
  )

  private val params = Parameter(args)
  private val job = new RuleVerifier(params)

  override def beforeAll(): Unit = {
    val df1 = createMockChocolateData()
    val df2 = createMockAmsClickData()

    df1.show(false)
    df2.show(false)

    job.saveDFToFiles(df1, srcPath)
    job.saveDFToFiles(df2, targetPath, outputFormat = "csv", delimiter = "bel", compressFormat = "uncompressed")
  }

  test("Test RuleVerifier") {
    job.run()
    job.stop()
  }

  /*
  def binaryToDec(src: String): Long = {
    var rt_rule_bitmap = 0L
    for (c <- src) {
      rt_rule_bitmap = rt_rule_bitmap * 2 + c.toLong - '0'.toLong
    }
    rt_rule_bitmap
  }
  */

  def generateRtRuleFlags(rt_rule_flag2: String,
                          rt_rule_flag3: String,
                          rt_rule_flag4: String,
                          rt_rule_flag7: String,
                          rt_rule_flag8: String,
                          rt_rule_flag1: String,
                          rt_rule_flag10: String,
                          rt_rule_flag15: String,
                          rt_rule_flag6: String,
                          rt_rule_flag5: String): Long = {
    var rt_rule_bitmap = 0L

    if (StringUtils.isNotEmpty(rt_rule_flag2) && rt_rule_flag2.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 1
    }
    if ((StringUtils.isNotEmpty(rt_rule_flag3) && rt_rule_flag3.equals("1")) ||
      StringUtils.isNotEmpty(rt_rule_flag4) && rt_rule_flag4.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 3
    }
    if (StringUtils.isNotEmpty(rt_rule_flag7) && rt_rule_flag7.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 2
    }
    if (StringUtils.isNotEmpty(rt_rule_flag8) && rt_rule_flag8.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 12
    }
    if (StringUtils.isNotEmpty(rt_rule_flag1) && rt_rule_flag1.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 11
    }
    if (StringUtils.isNotEmpty(rt_rule_flag10) && rt_rule_flag10.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 6
    }
    if (StringUtils.isNotEmpty(rt_rule_flag15) && rt_rule_flag15.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 4
    }
    if (StringUtils.isNotEmpty(rt_rule_flag6) && rt_rule_flag6.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 5
    }
    if (StringUtils.isNotEmpty(rt_rule_flag5) && rt_rule_flag5.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 10
    }

    rt_rule_bitmap
  }

  def generateNrtRuleFlags(nrt_rule_flag39: String,
                           nrt_rule_flag43: String,
                           nrt_rule_flag51: String,
                           nrt_rule_flag53: String,
                           nrt_rule_flag54: String,
                           nrt_rule_flag56: String,
                           rt_rule_flag12: String,
                           rt_rule_flag13: String): Long = {
    var ams_rule_bitmap = 0L

    if (StringUtils.isNotEmpty(nrt_rule_flag39) && nrt_rule_flag39.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 1
    }
    if (StringUtils.isNotEmpty(nrt_rule_flag43) && nrt_rule_flag43.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 2
    }
    if (StringUtils.isNotEmpty(nrt_rule_flag51) && nrt_rule_flag51.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 5
    }
    if (StringUtils.isNotEmpty(nrt_rule_flag53) && nrt_rule_flag53.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 6
    }
    if (StringUtils.isNotEmpty(nrt_rule_flag54) && nrt_rule_flag54.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 3
    }
    if (StringUtils.isNotEmpty(nrt_rule_flag56) && nrt_rule_flag56.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 4
    }
    if (StringUtils.isNotEmpty(rt_rule_flag12) && rt_rule_flag12.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 7
    }
    if (StringUtils.isNotEmpty(rt_rule_flag13) && rt_rule_flag13.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 8
    }

    ams_rule_bitmap
  }

  def createMockChocolateData(): DataFrame = {
    val rdd = job.sc.parallelize(
      Seq(
        Row(
          1L, 1L, 1L, 1L, "X-eBay-Client-IP: 1|cguid/1",
          "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338107049&item=132355040461&vectorid=229466&lgeo=1&dashenId=6432328199681789952&dashenCnt=0",
          "123", "",
          generateRtRuleFlags("1", "1", "1", "1", "1", "1", "1", "1", "1", "1"),
          generateNrtRuleFlags("1", "1", "1", "1", "1", "1", "1", "1"),
          "CLICK", "EPN", "", "", false
        ),
        Row(
          1L, 1L, 1L, 1L, "X-eBay-Client-IP: 1|cguid/1",
          "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338107049&item=132355040461&vectorid=229466&lgeo=1&dashenId=6432328199681789952&dashenCnt=0",
          "123", "",
          generateRtRuleFlags("1", "1", "1", "1", "1", "1", "1", "1", "1", "1"),
          generateNrtRuleFlags("1", "1", "1", "1", "1", "1", "1", "1"),
          "CLICK", "EPN", "", "", false
        ),
        Row(
          1L, 1L, 1L, 1L, "X-eBay-Client-IP: 1|cguid/1",
          "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5337666873&customid=&lgeo=1&vectorid=229466&item=222853652218&dashenId=6432328199681789952&dashenCnt=0",
          "123", "",
          generateRtRuleFlags("1", "0", "1", "0", "1", "0", "1", "0", "1", "0"),
          generateNrtRuleFlags("1", "0", "1", "0", "1", "0", "1", "0"),
          "CLICK", "EPN", "", "", false
        ),
        Row(
          1L, 1L, 1L, 1L, "X-eBay-Client-IP: 1|cguid/1",
          "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5337666873&customid=&lgeo=1&vectorid=229466&item=222853652218&dashenId=6432328199681789952&dashenCnt=0",
          "123", "",
          generateRtRuleFlags("1", "0", "1", "0", "1", "0", "1", "0", "1", "0"),
          generateNrtRuleFlags("1", "0", "1", "0", "1", "0", "1", "0"),
          "CLICK", "EPN", "", "", false
        )
      )
    )

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

    job.sqlsc.createDataFrame(rdd, schema)
  }

  def createMockAmsClickData(): DataFrame = {
    // We extract data from ams_click into csv format, so everything appears as string.
    val rdd = job.sc.parallelize(
      Seq(
        Row(
          "", "1", "1", "1", "1", "1", "1",
          "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338107049&item=132355040461&vectorid=229466&lgeo=1&raptor=1",
          "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1"
        ),
        Row(
          "", "1", "1", "1", "1", "1", "1",
          "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338107049&item=132355040461&vectorid=229466&lgeo=1&raptor=1",
          "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1", "1"
        ),
        Row( // mismatch
          "", "1", "1", "1", "1", "1", "1",
          "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5337666873&customid=&lgeo=1&vectorid=229466&item=222853652218&raptor=1",
          "0", "1", "0", "1", "0", "1", "0", "1", "1", "1", "1", "0", "1", "0", "1", "0", "1", "0"
        ),
        Row( // mismatch
          "", "1", "1", "1", "1", "1", "1",
          "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5337666873&customid=&lgeo=1&vectorid=229466&item=222853652218&raptor=1",
          "0", "1", "0", "1", "0", "1", "0", "1", "1", "1", "1", "0", "1", "0", "1", "0", "1", "0"
        ),
        Row( // dummy one, won't appear in result of inner join
          "", "1", "1", "1", "1", "1", "1",
          "http://rover.ebay.com",
          "1", "0", "1", "0", "0", "0", "0", "0", "1", "0", "1", "0", "0", "0", "0", "0", "0", "0"
        )
      ))

    job.sqlsc.createDataFrame(rdd, job.amsClickSchema)
  }
}
