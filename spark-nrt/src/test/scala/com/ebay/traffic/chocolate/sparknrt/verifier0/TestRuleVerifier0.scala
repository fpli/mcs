package com.ebay.traffic.chocolate.sparknrt.verifier0

import com.ebay.traffic.chocolate.spark.BaseFunSuite
import org.apache.spark.sql.types._

/**
  * Created by jialili1 on 11/23/18.
  */
class TestRuleVerifier0 extends BaseFunSuite {

  val tmpPath = createTempPath()
  val workDir = tmpPath + "/workDir/"
  val outputPath = tmpPath + "/outputDir/result"

  val args = Array(
    "--mode", "local[4]",
    "--workPath", workDir,
    "--chocoTodayPath", getTestResourcePath("verifier0/choco_data/part-0001.csv"),
    "--chocoYesterdayPath", getTestResourcePath("verifier0/choco_data/part-0002.csv"),
    "--chocoInputFormat", "csv",
    "--chocoInputDelimiter", "bel",
    "--epnTodayPath", "",
    "--epnYesterdayPath", "",
    "--outputPath", outputPath
  )

  val schema = StructType(
    Seq(
      StructField("snapshot_id", LongType, nullable = true),
      StructField("short_snapshot_id", LongType, nullable = true),
      StructField("timestamp", LongType, nullable = true),
      StructField("user_id", LongType, nullable = true),
      StructField("guid", StringType, nullable = true),
      StructField("cguid", StringType, nullable = true),
      StructField("remote_ip", StringType, nullable = true),
      StructField("lang_cd", StringType, nullable = true),
      StructField("user_agent", StringType, nullable = true),
      StructField("geo_id", LongType, nullable = true),
      StructField("udid", StringType, nullable = true),
      StructField("referer", StringType, nullable = true),
      StructField("publisher_id", LongType, nullable = true),
      StructField("campaign_id", LongType, nullable = true),
      StructField("site_id", LongType, nullable = true),
      StructField("landing_page_url", StringType, nullable = true),
      StructField("src_rotation_id", LongType, nullable = true),
      StructField("dst_rotation_id", LongType, nullable = true),
      StructField("request_headers", StringType, nullable = true),
      StructField("uri", StringType, nullable = true),
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

  val params = Parameter(args)

  val job = new RuleVerifier0(params)

  override def beforeAll() = {
    job.setChocoSchema(schema)
  }

  override def afterAll() = {
    job.stop()
  }

  test("Test RuleVerifier0") {
    job.run()
  }

}
