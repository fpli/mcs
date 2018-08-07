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

    // 3. Aggregation - join
    logger.info("start aggregation...")

    // define udf for RT rule verification
    val verifyPrefetchUdf = udf(verifyPrefetch(_: Long, _: String))
    val verifyTwoPassIABUdf = udf(verifyTwoPassIAB(_: Long, _: String, _: String))
    val verifyInternalTrafficUdf = udf(verifyInternalTraffic(_: Long, _: String))
    val verifyMissingReferrerUdf = udf(verifyMissingReferrer(_: Long, _: String))
    val verifyProtocolUdf = udf(verifyProtocol(_: Long, _: String))
    val verifyCGuidStalenessWindowUdf = udf(verifyCGuidStalenessWindow(_: Long, _: String))
    val verifyEpnDomainBlacklistUdf = udf(verifyEpnDomainBlacklist(_: Long, _: String))
    val verifyIpBlacklistUdf = udf(verifyIpBlacklist(_: Long, _: String))
    val verifyEbayRobotUdf = udf(verifyEbayRobot(_: Long, _: String))

    // define udf for NRT rule verification
    val verifyClickCappingIPPubSUdf = udf(verifyClickCappingIPPubS(_: Long, _: String))
    val verifyClickCappingIPPubLUdf = udf(verifyClickCappingIPPubL(_: Long, _: String))
    val verifyClickCappingCGuidSUdf = udf(verifyClickCappingCGuidS(_: Long, _: String))
    val verifyClickCappingCGuidLUdf = udf(verifyClickCappingCGuidL(_: Long, _: String))
    val verifyClickCappingCGuidPubSUdf = udf(verifyClickCappingCGuidPubS(_: Long, _: String))
    val verifyClickCappingCGuidPubLUdf = udf(verifyClickCappingCGuidPubL(_: Long, _: String))
    val verifySnidRuleSUdf = udf(verifySnidRuleS(_: Long, _: String))
    val verifySnidRuleLUdf = udf(verifySnidRuleL(_: Long, _: String))

    val df = df1.join(df2, $"uri" === $"rover_url", "inner")
      .withColumn("IPPubS", verifyClickCappingIPPubSUdf($"nrt_rule_flags", $"nrt_rule_flag39"))
      .withColumn("IPPubL", verifyClickCappingIPPubLUdf($"nrt_rule_flags", $"nrt_rule_flag43"))
      .withColumn("CGuidS", verifyClickCappingCGuidSUdf($"nrt_rule_flags", $"nrt_rule_flag51"))
      .withColumn("CGuidL", verifyClickCappingCGuidLUdf($"nrt_rule_flags", $"nrt_rule_flag53"))
      .withColumn("CGuidPubS", verifyClickCappingCGuidPubSUdf($"nrt_rule_flags", $"nrt_rule_flag54"))
      .withColumn("CGuidPubL", verifyClickCappingCGuidPubLUdf($"nrt_rule_flags", $"nrt_rule_flag56"))
      .withColumn("SnidS", verifySnidRuleSUdf($"nrt_rule_flags", $"rt_rule_flag12"))
      .withColumn("SnidL", verifySnidRuleLUdf($"nrt_rule_flags", $"rt_rule_flag13"))
      .withColumn("Prefetch", verifyPrefetchUdf($"rt_rule_flags", $"rt_rule_flag2"))
      .withColumn("IABBot", verifyTwoPassIABUdf($"rt_rule_flags", $"rt_rule_flag3", $"rt_rule_flag4"))
      .withColumn("Internal", verifyInternalTrafficUdf($"rt_rule_flags", $"rt_rule_flag7"))
      .withColumn("MissingReferrer", verifyMissingReferrerUdf($"rt_rule_flags", $"rt_rule_flag8"))
      .withColumn("Protocol", verifyProtocolUdf($"rt_rule_flags", $"rt_rule_flag1"))
      .withColumn("CGuidStaleness", verifyCGuidStalenessWindowUdf($"rt_rule_flags", $"rt_rule_flag10"))
      .withColumn("EpnDomainBlacklist", verifyEpnDomainBlacklistUdf($"rt_rule_flags", $"rt_rule_flag15"))
      .withColumn("IPBlacklist", verifyIpBlacklistUdf($"rt_rule_flags", $"rt_rule_flag6"))
      .withColumn("EbayBot", verifyEbayRobotUdf($"rt_rule_flags", $"rt_rule_flag5"))
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

  /* RT rule verification */

  def verifyPrefetch(rt_rule_flags: Long, rt_rule_flag2: String): Boolean = {
    var rt_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(rt_rule_flag2) && rt_rule_flag2.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 1
    }
    (rt_rule_flags & rt_rule_bitmap) != 0
  }

  def verifyTwoPassIAB(rt_rule_flags: Long, rt_rule_flag3: String, rt_rule_flag4: String): Boolean = {
    var rt_rule_bitmap = 0L
    if ((StringUtils.isNotEmpty(rt_rule_flag3) && rt_rule_flag3.equals("1")) ||
      StringUtils.isNotEmpty(rt_rule_flag4) && rt_rule_flag4.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 3
    }
    (rt_rule_flags & rt_rule_bitmap) != 0
  }

  def verifyInternalTraffic(rt_rule_flags: Long, rt_rule_flag7: String): Boolean = {
    var rt_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(rt_rule_flag7) && rt_rule_flag7.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 2
    }
    (rt_rule_flags & rt_rule_bitmap) != 0
  }

  def verifyMissingReferrer(rt_rule_flags: Long, rt_rule_flag8: String): Boolean = {
    var rt_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(rt_rule_flag8) && rt_rule_flag8.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 12
    }
    (rt_rule_flags & rt_rule_bitmap) != 0
  }

  def verifyProtocol(rt_rule_flags: Long, rt_rule_flag1: String): Boolean = {
    var rt_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(rt_rule_flag1) && rt_rule_flag1.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 11
    }
    (rt_rule_flags & rt_rule_bitmap) != 0
  }

  def verifyCGuidStalenessWindow(rt_rule_flags: Long, rt_rule_flag10: String): Boolean = {
    var rt_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(rt_rule_flag10) && rt_rule_flag10.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 6
    }
    (rt_rule_flags & rt_rule_bitmap) != 0
  }

  def verifyEpnDomainBlacklist(rt_rule_flags: Long, rt_rule_flag15: String): Boolean = {
    var rt_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(rt_rule_flag15) && rt_rule_flag15.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 4
    }
    (rt_rule_flags & rt_rule_bitmap) != 0
  }

  def verifyIpBlacklist(rt_rule_flags: Long, rt_rule_flag6: String): Boolean = {
    var rt_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(rt_rule_flag6) && rt_rule_flag6.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 5
    }
    (rt_rule_flags & rt_rule_bitmap) != 0
  }

  def verifyEbayRobot(rt_rule_flags: Long, rt_rule_flag5: String): Boolean = {
    var rt_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(rt_rule_flag5) && rt_rule_flag5.equals("1")) {
      rt_rule_bitmap = rt_rule_bitmap | 1L << 10
    }
    (rt_rule_flags & rt_rule_bitmap) != 0
  }

  /* NRT rule verification */

  def verifyClickCappingIPPubS(nrt_rule_flags: Long, nrt_rule_flag39: String): Boolean = {
    var ams_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(nrt_rule_flag39) && nrt_rule_flag39.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 1
    }
    (nrt_rule_flags & ams_rule_bitmap) != 0
  }

  def verifyClickCappingIPPubL(nrt_rule_flags: Long, nrt_rule_flag43: String): Boolean = {
    var ams_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(nrt_rule_flag43) && nrt_rule_flag43.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 2
    }
    (nrt_rule_flags & ams_rule_bitmap) != 0
  }

  def verifyClickCappingCGuidS(nrt_rule_flags: Long, nrt_rule_flag51: String): Boolean = {
    var ams_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(nrt_rule_flag51) && nrt_rule_flag51.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 5
    }
    (nrt_rule_flags & ams_rule_bitmap) != 0
  }

  def verifyClickCappingCGuidL(nrt_rule_flags: Long, nrt_rule_flag53: String): Boolean = {
    var ams_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(nrt_rule_flag53) && nrt_rule_flag53.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 6
    }
    (nrt_rule_flags & ams_rule_bitmap) != 0
  }

  def verifyClickCappingCGuidPubS(nrt_rule_flags: Long, nrt_rule_flag54: String):Boolean = {
    var ams_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(nrt_rule_flag54) && nrt_rule_flag54.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 3
    }
    (nrt_rule_flags & ams_rule_bitmap) != 0
  }

  def verifyClickCappingCGuidPubL(nrt_rule_flags: Long, nrt_rule_flag56: String): Boolean = {
    var ams_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(nrt_rule_flag56) && nrt_rule_flag56.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 4
    }
    (nrt_rule_flags & ams_rule_bitmap) != 0
  }

  def verifySnidRuleS(nrt_rule_flags: Long, rt_rule_flag12: String): Boolean = {
    var ams_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(rt_rule_flag12) && rt_rule_flag12.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 7
    }
    (nrt_rule_flags & ams_rule_bitmap) != 0
  }

  def verifySnidRuleL(nrt_rule_flags: Long, rt_rule_flag13: String): Boolean = {
    var ams_rule_bitmap = 0L
    if (StringUtils.isNotEmpty(rt_rule_flag13) && rt_rule_flag13.equals("1")) {
      ams_rule_bitmap = ams_rule_bitmap | 1L << 8
    }
    (nrt_rule_flags & ams_rule_bitmap) != 0
  }

}
