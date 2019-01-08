package com.ebay.traffic.chocolate.sparknrt.verifier

import com.ebay.traffic.chocolate.sparknrt.BaseSparkNrtJob
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
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

  lazy val workDir = params.workPath + "/tmp/"

  override def run(): Unit = {
    // 1. Load chocolate data
    logger.info("load data for inputpath1: " + params.srcPath)

    val removeParamsUdf = udf(removeParams(_: String))
    var df1 = readFilesAsDF(params.srcPath)
      .where($"channel_action" === "CLICK" and $"channel_type" === "EPN")
      .withColumn("new_uri", removeParamsUdf($"uri"))
      .drop("uri")
      .select($"new_uri", $"rt_rule_flags", $"nrt_rule_flags")
      .dropDuplicates("new_uri")

    val path1 = new Path(workDir + "/chocolate/", (new Path(params.srcPath).getName))
    fs.delete(path1, true)
    saveDFToFiles(df1, path1.toString)
    df1 = readFilesAsDF(path1.toString)
    val count1 = df1.count()

    println("number of records in df1: " + count1)

    // 2. Load EPN data fetched from ams_click
    logger.info("load data for inputpath2: " + params.targetPath)

    val normalizeUrlUdf = udf((roverUrl: String) => normalizeUrl(roverUrl))
    // assume df2 only has columns that we want!
    var df2 = readFilesAsDF(params.targetPath, inputFormat = "csv", schema = amsClickSchema, delimiter = "bel")
      .withColumn("rover_url", normalizeUrlUdf(col("rover_url_txt")))
      .drop("rover_url_txt")
      .dropDuplicates("rover_url")

    val path2 = new Path(workDir + "/epn/", (new Path(params.targetPath).getName))
    fs.delete(path2, true)
    saveDFToFiles(df2, path2.toString)
    df2 = readFilesAsDF(path2.toString)
    val count2 = df2.count()

    println("number of records in df2: " + count2)

    // 3. Aggregation - join
    logger.info("start aggregation...")

    // define udf for RT rule verification
    val verifyByBitUdf1 = udf(verifyByBit(_: Int, _: Long, _: String))
    val verifyByBitUdf2 = udf(verifyByBit(_: Int, _: Long, _: String, _: String))

    var df = df1.join(df2, $"new_uri" === $"rover_url", "inner")
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
      .drop("new_uri")

    val path = new Path(workDir + "/join/", (new Path(params.srcPath).getName))
    fs.delete(path, true)
    saveDFToFiles(df, path.toString)
    df = readFilesAsDF(path.toString)
    val total = df.count()

    println("sampling 10 records:")
    df.show(numRows = 10, truncate = false)

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
    val cGuidStaleness = df.where($"CGuidStaleness" === false).count()
    val epnDomainBlacklist = df.where($"EpnDomainBlacklist" === false).count()
    val ipBlacklist = df.where($"IPBlacklist" === false).count()
    val ebayBot = df.where($"EbayBot" === false).count()

    // 4. Write out result to file on hdfs
    var outputStream: FSDataOutputStream = null
    try {
      outputStream = fs.create(new Path(params.outputPath))
      outputStream.writeChars(s"Chocolate Total: $count1 \n")
      outputStream.writeChars(s"EPN Total: $count2 \n")
      outputStream.writeChars(s"Join Total: $total \n")
      outputStream.writeChars(s"Chocolate join ratio: ${((total.toFloat/count1)*100).toInt}% \n")
      outputStream.writeChars(s"EPN join ratio: ${((total.toFloat/count2)*100).toInt}% \n")

      outputStream.writeChars("--------------------------------------------------------" + "\n")
      outputStream.writeChars("IPPubS inconsistent: " + ipPubS.toFloat/total + "\n")
      outputStream.writeChars("IPPubL inconsistent: " + ipPubL.toFloat/total + "\n")
      outputStream.writeChars("CGuidS inconsistent: " + cGuidS.toFloat/total + "\n")
      outputStream.writeChars("CGuidL inconsistent: " + cGuidL.toFloat/total + "\n")
      outputStream.writeChars("CGuidPubS inconsistent: " + cGuidPubS.toFloat/total + "\n")
      outputStream.writeChars("CGuidPubL inconsistent: " + cGuidPubL.toFloat/total + "\n")
      outputStream.writeChars("SnidS inconsistent: " + snidS.toFloat/total + "\n")
      outputStream.writeChars("SnidL inconsistent: " + snidL.toFloat/total + "\n")

      outputStream.writeChars("Prefetch inconsistent: " + prefetch.toFloat/total + "\n")
      outputStream.writeChars("IABBot inconsistent: " + iabBot.toFloat/total + "\n")
      outputStream.writeChars("Internal inconsistent: " + internal.toFloat/total + "\n")
      outputStream.writeChars("MissingReferrer inconsistent: " + missingReferrer.toFloat/total + "\n")
      outputStream.writeChars("Protocol inconsistent: " + protocol.toFloat/total + "\n")
      outputStream.writeChars("CGuidStaleness inconsistent: " + cGuidStaleness.toFloat/total + "\n")
      outputStream.writeChars("EpnDomainBlacklist inconsistent: " + epnDomainBlacklist.toFloat/total + "\n")
      outputStream.writeChars("IPBlacklist inconsistent: " + ipBlacklist.toFloat/total + "\n")
      outputStream.writeChars("EbayBot inconsistent: " + ebayBot.toFloat/total + "\n")

      outputStream.flush()
    } finally {
      if (outputStream != null) {
        outputStream.close()
      }
    }

    if (params.selfCheck) {

      val ipPubS_choco = df1.where($"nrt_rule_flags".bitwiseAND(2) =!= 0).count()
      val ipPubL_choco = df1.where($"nrt_rule_flags".bitwiseAND(4) =!= 0).count()
      val cGuidS_choco = df1.where($"nrt_rule_flags".bitwiseAND(32) =!= 0).count()
      val cGuidL_choco = df1.where($"nrt_rule_flags".bitwiseAND(64) =!= 0).count()
      val cGuidPubS_choco = df1.where($"nrt_rule_flags".bitwiseAND(8) =!= 0).count()
      val cGuidPubL_choco = df1.where($"nrt_rule_flags".bitwiseAND(16) =!= 0).count()
      val snidS_choco = df1.where($"nrt_rule_flags".bitwiseAND(128) =!= 0).count()
      val snidL_choco = df1.where($"nrt_rule_flags".bitwiseAND(256) =!= 0).count()

      val prefetch_choco = df1.where($"rt_rule_flags".bitwiseAND(2) =!= 0).count()
      val iabBot_choco = df1.where($"rt_rule_flags".bitwiseAND(8) =!= 0).count()
      val internal_choco = df1.where($"rt_rule_flags".bitwiseAND(4) =!= 0).count()
      val missingReferrer_choco = df1.where($"rt_rule_flags".bitwiseAND(4096) =!= 0).count()
      val protocol_choco = df1.where($"rt_rule_flags".bitwiseAND(2048) =!= 0).count()
      val tGuidStaleness_choco = df1.where($"rt_rule_flags".bitwiseAND(64) =!= 0).count()
      val epnDomainBlacklist_choco = df1.where($"rt_rule_flags".bitwiseAND(16) =!= 0).count()
      val ipBlacklist_choco = df1.where($"rt_rule_flags".bitwiseAND(32) =!= 0).count()
      val ebayBot_choco = df1.where($"rt_rule_flags".bitwiseAND(1024) =!= 0).count()

      val ipPubS_epn = df2.where($"nrt_rule_flag39" === 1).count()
      val ipPubL_epn = df2.where($"nrt_rule_flag43" === 1).count()
      val cGuidS_epn = df2.where($"nrt_rule_flag51" === 1).count()
      val cGuidL_epn = df2.where($"nrt_rule_flag53" === 1).count()
      val cGuidPubS_epn = df2.where($"nrt_rule_flag54" === 1).count()
      val cGuidPubL_epn = df2.where($"nrt_rule_flag56" === 1).count()
      val snidS_epn = df2.where($"rt_rule_flag12" === 1).count()
      val snidL_epn = df2.where($"rt_rule_flag13" === 1).count()

      val prefetch_epn = df2.where($"rt_rule_flag2" === 1).count()
      val iabBot_epn = df2.where($"rt_rule_flag3" === 1 or $"rt_rule_flag4" === 1).count()
      val internal_epn = df2.where($"rt_rule_flag7" === 1).count()
      val missingReferrer_epn = df2.where($"rt_rule_flag8" === 1).count()
      val protocol_epn = df2.where($"rt_rule_flag1" === 1).count()
      val tGuidStaleness_epn = df2.where($"rt_rule_flag10" === 1).count()
      val epnDomainBlacklist_epn = df2.where($"rt_rule_flag15" === 1).count()
      val ipBlacklist_epn = df2.where($"rt_rule_flag6" === 1).count()
      val ebayBot_epn = df2.where($"rt_rule_flag5" === 1).count()

      //join rule fail count
      val ipPubS_choco_join = df.where($"nrt_rule_flags".bitwiseAND(2) =!= 0).count()
      val ipPubL_choco_join = df.where($"nrt_rule_flags".bitwiseAND(4) =!= 0).count()
      val cGuidS_choco_join = df.where($"nrt_rule_flags".bitwiseAND(32) =!= 0).count()
      val cGuidL_choco_join = df.where($"nrt_rule_flags".bitwiseAND(64) =!= 0).count()
      val cGuidPubS_choco_join = df.where($"nrt_rule_flags".bitwiseAND(8) =!= 0).count()
      val cGuidPubL_choco_join = df.where($"nrt_rule_flags".bitwiseAND(16) =!= 0).count()
      val snidS_choco_join = df.where($"nrt_rule_flags".bitwiseAND(128) =!= 0).count()
      val snidL_choco_join = df.where($"nrt_rule_flags".bitwiseAND(256) =!= 0).count()

      val prefetch_choco_join = df.where($"rt_rule_flags".bitwiseAND(2) =!= 0).count()
      val iabBot_choco_join = df.where($"rt_rule_flags".bitwiseAND(8) =!= 0).count()
      val internal_choco_join = df.where($"rt_rule_flags".bitwiseAND(4) =!= 0).count()
      val missingReferrer_choco_join = df.where($"rt_rule_flags".bitwiseAND(4096) =!= 0).count()
      val protocol_choco_join = df.where($"rt_rule_flags".bitwiseAND(2048) =!= 0).count()
      val tGuidStaleness_choco_join = df.where($"rt_rule_flags".bitwiseAND(64) =!= 0).count()
      val epnDomainBlacklist_choco_join = df.where($"rt_rule_flags".bitwiseAND(16) =!= 0).count()
      val ipBlacklist_choco_join = df.where($"rt_rule_flags".bitwiseAND(32) =!= 0).count()
      val ebayBot_choco_join = df.where($"rt_rule_flags".bitwiseAND(1024) =!= 0).count()

      val ipPubS_epn_join = df.where($"nrt_rule_flag39" === 1).count()
      val ipPubL_epn_join = df.where($"nrt_rule_flag43" === 1).count()
      val cGuidS_epn_join = df.where($"nrt_rule_flag51" === 1).count()
      val cGuidL_epn_join = df.where($"nrt_rule_flag53" === 1).count()
      val cGuidPubS_epn_join = df.where($"nrt_rule_flag54" === 1).count()
      val cGuidPubL_epn_join = df.where($"nrt_rule_flag56" === 1).count()
      val snidS_epn_join = df.where($"rt_rule_flag12" === 1).count()
      val snidL_epn_join = df.where($"rt_rule_flag13" === 1).count()

      val prefetch_epn_join = df.where($"rt_rule_flag2" === 1).count()
      val iabBot_epn_join = df.where($"rt_rule_flag3" === 1 or $"rt_rule_flag4" === 1).count()
      val internal_epn_join = df.where($"rt_rule_flag7" === 1).count()
      val missingReferrer_epn_join = df.where($"rt_rule_flag8" === 1).count()
      val protocol_epn_join = df.where($"rt_rule_flag1" === 1).count()
      val tGuidStaleness_epn_join = df.where($"rt_rule_flag10" === 1).count()
      val epnDomainBlacklist_epn_join = df.where($"rt_rule_flag15" === 1).count()
      val ipBlacklist_epn_join = df.where($"rt_rule_flag6" === 1).count()
      val ebayBot_epn_join = df.where($"rt_rule_flag5" === 1).count()

      try {
        outputStream = fs.append(new Path(params.outputPath))

        outputStream.writeChars("-----------------------self check--------------------------" + "\n")
        outputStream.writeChars(s"ipPubS_choco: $ipPubS_choco, ipPubS_epn: $ipPubS_epn," +
          s"ipPubS_choco_join: $ipPubS_choco_join, ipPubS_epn_join: $ipPubS_epn_join \n")
        outputStream.writeChars(s"ipPubL_choco: $ipPubL_choco, ipPubL_epn: $ipPubL_epn," +
          s"ipPubL_choco_join: $ipPubL_choco_join, ipPubL_epn_join: $ipPubL_epn_join \n")
        outputStream.writeChars(s"cGuidS_choco: $cGuidS_choco, cGuidS_epn: $cGuidS_epn," +
          s"cGuidS_choco_join: $cGuidS_choco_join, cGuidS_epn_join: $cGuidS_epn_join \n")
        outputStream.writeChars(s"cGuidL_choco: $cGuidL_choco, cGuidL_epn: $cGuidL_epn," +
          s"cGuidL_choco_join: $cGuidL_choco_join, cGuidL_epn_join: $cGuidL_epn_join \n")
        outputStream.writeChars(s"cGuidPubS_choco: $cGuidPubS_choco, cGuidPubS_epn: $cGuidPubS_epn," +
          s"cGuidPubS_choco_join: $cGuidPubS_choco_join, cGuidPubS_epn_join: $cGuidPubS_epn_join \n")
        outputStream.writeChars(s"cGuidPubL_choco: $cGuidPubL_choco, cGuidPubL_epn: $cGuidPubL_epn," +
          s"cGuidPubL_choco_join: $cGuidPubL_choco_join, cGuidPubL_epn_join: $cGuidPubL_epn_join \n")
        outputStream.writeChars(s"snidS_choco: $snidS_choco, snidS_epn: $snidS_epn," +
          s"snidS_choco_join: $snidS_choco_join, snidS_epn_join: $snidS_epn_join \n")
        outputStream.writeChars(s"snidL_choco: $snidL_choco, snidL_epn: $snidL_epn," +
          s"snidL_choco_join: $snidL_choco_join, snidL_epn_join: $snidL_epn_join \n")
        outputStream.writeChars(s"prefetch_choco: $prefetch_choco, prefetch_epn: $prefetch_epn," +
          s"prefetch_choco_join: $prefetch_choco_join, prefetch_epn_join: $prefetch_epn_join \n")
        outputStream.writeChars(s"iabBot_choco: $iabBot_choco, iabBot_epn: $iabBot_epn," +
          s"iabBot_choco_join: $iabBot_choco_join, iabBot_epn_join: $iabBot_epn_join \n")
        outputStream.writeChars(s"internal_choco: $internal_choco, internal_epn: $internal_epn," +
          s"internal_choco_join: $internal_choco_join, internal_epn_join: $internal_epn_join \n")
        outputStream.writeChars(s"missingReferrer_choco: $missingReferrer_choco, missingReferrer_epn: $missingReferrer_epn," +
          s"missingReferrer_choco_join: $missingReferrer_choco_join, missingReferrer_epn_join: $missingReferrer_epn_join \n")
        outputStream.writeChars(s"protocol_choco: $protocol_choco, protocol_epn: $protocol_epn," +
          s"protocol_choco_join: $protocol_choco_join, protocol_epn_join: $protocol_epn_join \n")
        outputStream.writeChars(s"tGuidStaleness_choco: $tGuidStaleness_choco, tGuidStaleness_epn: $tGuidStaleness_epn," +
          s"tGuidStaleness_choco_join: $tGuidStaleness_choco_join, tGuidStaleness_epn_join: $tGuidStaleness_epn_join \n")
        outputStream.writeChars(s"epnDomainBlacklist_choco: $epnDomainBlacklist_choco, epnDomainBlacklist_epn: $epnDomainBlacklist_epn," +
          s"epnDomainBlacklist_choco_join: $epnDomainBlacklist_choco_join, epnDomainBlacklist_epn_join: $epnDomainBlacklist_epn_join \n")
        outputStream.writeChars(s"ipBlacklist_choco: $ipBlacklist_choco, ipBlacklist_epn: $ipBlacklist_epn," +
          s"ipBlacklist_choco_join: $ipBlacklist_choco_join, ipBlacklist_epn_join: $ipBlacklist_epn_join \n")
        outputStream.writeChars(s"ebayBot_choco: $ebayBot_choco, ebayBot_epn: $ebayBot_epn," +
          s"ebayBot_choco_join: $ebayBot_choco_join, ebayBot_epn_join: $ebayBot_epn_join \n")
        outputStream.flush()
      } finally {
        if (outputStream != null) {
          outputStream.close()
        }
      }
    }
  }

  /**
    * filter out dashenId, dashenCnt
    */
  def removeParams(url: String): String = {
    val splitter = url.split("&").filter(item => !item.startsWith("dashenId") && !item.startsWith("dashenCnt"))
    splitter.mkString("&")
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
