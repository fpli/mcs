package com.ebay.traffic.chocolate.sparknrt.verifier

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object RuleVerifier {
  def main(args: Array[String]): Unit = {
    val params = Parameter(args)
    val job = new RuleVerifier(params)
    job.run()
    job.stop()
  }
}

class RuleVerifier(params: Parameter) extends BaseSparkNrtJob(params.appName, params.mode) {

  // Here lists all fields in order in ams_click that are required for verification.
  val amsClickSchema: StructType = StructType(
    Seq(
      StructField("rover_url_txt", StringType, nullable = true),
      StructField("rt_rule_flag1", StringType, nullable = true),
      StructField("rt_rule_flag2", StringType, nullable = true),
      StructField("rt_rule_flag3", StringType, nullable = true),
      StructField("rt_rule_flag4", StringType, nullable = true),
      StructField("rt_rule_flag5", StringType, nullable = true),
      StructField("rt_rule_flag6", StringType, nullable = true),
      StructField("rt_rule_flag7", StringType, nullable = true),
      StructField("rt_rule_flag8", StringType, nullable = true),
      StructField("rt_rule_flag10", StringType, nullable = true),
      StructField("rt_rule_flag12", StringType, nullable = true),
      StructField("rt_rule_flag13", StringType, nullable = true),
      StructField("rt_rule_flag15", StringType, nullable = true),
      StructField("nrt_rule_flag39", StringType, nullable = true),
      StructField("nrt_rule_flag43", StringType, nullable = true),
      StructField("nrt_rule_flag51", StringType, nullable = true),
      StructField("nrt_rule_flag53", StringType, nullable = true),
      StructField("nrt_rule_flag54", StringType, nullable = true),
      StructField("nrt_rule_flag56", StringType, nullable = true)
    )
  )

  import spark.implicits._

  override def run(): Unit = {
    // 1. Load chocolate data
    logger.info("load data for inputpath1...")

    val files1 = fs.listStatus(new Path(params.inputPath1)).map(status => status.getPath.toString)
    val df1 = readFilesAsDFEx(files1)
      .where($"channel_action" === "CLICK" and $"channel_type" === "EPN")
      .distinct()
      .select($"uri", $"rt_rule_flags", $"nrt_rule_flags")

    println("number of records in df1: " + df1.count())

    // 2. Load EPN data fetched from ams_click
    logger.info("load data for inputpath2...")

    val normalizeUrlUdf = udf((roverUrl: String) => normalizeUrl(roverUrl))
    val files2 = fs.listStatus(new Path(params.inputPath2))
      .map(status => status.getPath.toString)
      .filter(f => !f.contains("_SUCCESS"))
    // assume df2 only has columns that we want!
    val df2 = readFilesAsDFEx(files2, inputFormat = "csv", schema = amsClickSchema, delimiter = "comma")
      .distinct()
      .withColumn("rover_url", normalizeUrlUdf(col("rover_url_txt")))
      .drop("rover_url_txt")

    println("number of records in df2: " + df2.count())

    // 3. Aggregation - join
    logger.info("start aggregation...")

    // define udf for RT rule verification
    val verifyByBitUdf1 = udf(verifyByBit(_: Int, _: Long, _: String))
    val verifyByBitUdf2 = udf(verifyByBit(_: Int, _: Long, _: String, _: String))

    val df = df1.join(df2, $"uri" === $"rover_url", "inner")
      .withColumn("IPPubS", verifyByBitUdf1(lit(1), $"nrt_rule_flags", $"nrt_rule_flag39"))
      .withColumn("IPPubL", verifyByBitUdf1(lit(2), $"nrt_rule_flags", $"nrt_rule_flag43"))
      .withColumn("CGuidS", verifyByBitUdf1(lit(5), $"nrt_rule_flags", $"nrt_rule_flag51"))
      .withColumn("CGuidL", verifyByBitUdf1(lit(6), $"nrt_rule_flags", $"nrt_rule_flag53"))
      .withColumn("CGuidPubS", verifyByBitUdf1(lit(3), $"nrt_rule_flags", $"nrt_rule_flag54"))
      .withColumn("CGuidPubL", verifyByBitUdf1(lit(4), $"nrt_rule_flags", $"nrt_rule_flag56"))
      .withColumn("SnidS", verifyByBitUdf1(lit(7), $"nrt_rule_flags", $"rt_rule_flag12"))
      .withColumn("SnidL", verifyByBitUdf1(lit(8), $"nrt_rule_flags", $"rt_rule_flag13"))
      .withColumn("Prefetch", verifyByBitUdf1(lit(1), $"rt_rule_flags", $"rt_rule_flag2"))
      .withColumn("IABBot", verifyByBitUdf2(lit(3), $"rt_rule_flags", $"rt_rule_flag3", $"rt_rule_flag4"))
      .withColumn("Internal", verifyByBitUdf1(lit(2), $"rt_rule_flags", $"rt_rule_flag7"))
      .withColumn("MissingReferrer", verifyByBitUdf1(lit(12), $"rt_rule_flags", $"rt_rule_flag8"))
      .withColumn("Protocol", verifyByBitUdf1(lit(11), $"rt_rule_flags", $"rt_rule_flag1"))
      .withColumn("CGuidStaleness", verifyByBitUdf1(lit(6), $"rt_rule_flags", $"rt_rule_flag10"))
      .withColumn("EpnDomainBlacklist", verifyByBitUdf1(lit(4), $"rt_rule_flags", $"rt_rule_flag15"))
      .withColumn("IPBlacklist", verifyByBitUdf1(lit(5), $"rt_rule_flags", $"rt_rule_flag6"))
      .withColumn("EbayBot", verifyByBitUdf1(lit(10), $"rt_rule_flags", $"rt_rule_flag5"))
      .drop("uri")
      .cache()

    println("sampling 10 records:")
    df.show(numRows = 10, truncate = false)

    val total = df.count()

    val ipPubS = df.where($"IPPubS" === false).count()
    val ipPubL = df.where($"IPPubL" === false).count()
    val cGuidS = df.where($"CGuidS" === false).count()
    val cGuidL = df.where($"CGuidL" === false).count()
    val cGuidPubS = df.where($"CGuidPubS" === false).count()
    val cGuidPubL = df.where($"CGuidPubL" === false).count()
    val snidS = df.where($"SnidS" === false).count()
    val snidL = df.where($"SnidL" === false).count()

    val prefetch = df.where($"Prefetch" === false).count()
    val iabBot = df.where($"IABBot" === false).count()
    val internal = df.where($"Internal" === false).count()
    val missingReferrer = df.where($"MissingReferrer" === false).count()
    val protocol = df.where($"Protocol" === false).count()
    val cguidStaleness = df.where($"CGuidStaleness" === false).count()
    val epnDomainBlacklist = df.where($"EpnDomainBlacklist" === false).count()
    val ipBlacklist = df.where($"IPBlacklist" === false).count()
    val ebayBot = df.where($"EbayBot" === false).count()

    // Stdout
    println("Total: " + total)

    println("IPPubS inconsistent: " + ipPubS)
    println("IPPubL inconsistent: " + ipPubL)
    println("CGuidS inconsistent: " + cGuidS)
    println("CGuidL inconsistent: " + cGuidL)
    println("CGuidPubS inconsistent: " + cGuidPubS)
    println("CGuidPubL inconsistent: " + cGuidPubL)
    println("SnidS inconsistent: " + snidS)
    println("SnidL inconsistent: " + snidL)

    println("Prefetch inconsistent: " + prefetch)
    println("IABBot inconsistent: " + iabBot)
    println("Internal inconsistent: " + internal)
    println("MissingReferrer inconsistent: " + missingReferrer)
    println("Protocol inconsistent: " + protocol)
    println("CGuidStaleness inconsistent: " + cguidStaleness)
    println("EpnDomainBlacklist inconsistent: " + epnDomainBlacklist)
    println("IPBlacklist inconsistent: " + ipBlacklist)
    println("EbayBot inconsistent: " + ebayBot)
  }

  // should remove raptor=1 from rover URL
  def normalizeUrl(url: String): String = {
    var result = url.replace("raptor=1", "")
    val lastChar = result.charAt(result.length - 1)
    if (lastChar == '&' || lastChar == '?') {
      result = result.substring(0, result.length - 1)
    }
    result
  }

  def verifyByBit(bit: Int, chocolate_rule_flag: Long, epn_rule_flag: String): Boolean = {
    val mask = chocolate_rule_flag & 1L << bit
    if (StringUtils.isNotEmpty(epn_rule_flag) && epn_rule_flag.equals("1")) {
      mask != 0
    } else {
      mask == 0
    }
  }

  def verifyByBit(bit: Int, chocolate_rule_flag: Long, epn_rule_flag1: String, epn_rule_flag2: String): Boolean = {
    val mask = chocolate_rule_flag & 1L << bit
    if ((StringUtils.isNotEmpty(epn_rule_flag1) && epn_rule_flag1.equals("1")) ||
      StringUtils.isNotEmpty(epn_rule_flag2) && epn_rule_flag2.equals("1")) {
      mask != 0
    } else {
      mask == 0
    }
  }

}
