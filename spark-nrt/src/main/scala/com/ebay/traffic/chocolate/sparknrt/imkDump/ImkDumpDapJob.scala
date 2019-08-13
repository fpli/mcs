package com.ebay.traffic.chocolate.sparknrt.imkDump

import java.nio.file.Paths

import com.ebay.kernel.patternmatch.dawg.{Dawg, DawgDictionary}
import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

import scala.io.Source
import scala.reflect.io.{File, Path}

/**
 * Read display channel capping result and generate files for imk table
 * Calculate mgvalue and mgvaluereason for display channel.
 * Currently, mgvalue is always 0.
 * @author Zhiyuan Wang
 * @since 2019-08-08
 */
object ImkDumpDapJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new ImkDumpDapJob(params)

    import java.nio.file.Paths
    Paths.get("string").toUri.toURL
    job.run()
    job.stop()
  }
}

class ImkDumpDapJob(params: Parameter) extends ImkDumpJob(params: Parameter){

  @transient override lazy val inputMetadata: Metadata = {
    val usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("imkdump.upstream.display"))
    Metadata(params.workDir, params.channel, usage)
  }

  @transient lazy val userAgentBotDawgDictionary : DawgDictionary =  {
    val lines = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("dap_user_agent_robot.text")).getLines.toArray
    new DawgDictionary(lines, true)
  }

  @transient lazy val ipBotDawgDictionary : DawgDictionary =  {
    val lines = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("dap_ip_robot.txt")).getLines.toArray
    new DawgDictionary(lines, true)
  }

  override def imkDumpCore(df: DataFrame): DataFrame = {
    val imkDf = super.imkDumpCore(df)
    imkDf
      .withColumn("mgvalue", lit("0"))
      .withColumn("mgvaluereason", getMgvaluereasonUdf(col("brwsr_name"), col("clnt_remote_ip")))
  }

  val getMgvaluereasonUdf: UserDefinedFunction = udf((brwsrName: String, clntRemoteIp: String) => getMgvaluereason(brwsrName, clntRemoteIp))

  /**
   * Generate mgvaluereason, "BOT" or empty string
   * @param brwsrName alias for user agent
   * @param clntRemoteIp alias for remote ip
   * @return "BOT" or empty string
   */
  def getMgvaluereason(brwsrName: String, clntRemoteIp: String): String = {
    if (isBotByUserAgent(brwsrName) || isBotByIp(clntRemoteIp)) {
      "BOT"
    } else {
      ""
    }
  }

  /**
   * Check if this request is bot with brwsr_name
   * @param brwsrName alias for user agent
   * @return is bot or not
   */
  def isBotByUserAgent(brwsrName: String): Boolean = {
    isBot(brwsrName, userAgentBotDawgDictionary)
  }

  /**
   * Check if this request is bot with clnt_remote_ip
   * @param clntRemoteIp alias for remote ip
   * @return is bot or not
   */
  def isBotByIp(clntRemoteIp: String): Boolean = {
    isBot(clntRemoteIp, ipBotDawgDictionary)
  }

  def isBot(info: String, dawgDictionary: DawgDictionary): Boolean = {
    if (StringUtils.isEmpty(info)) {
      false
    } else {
      val dawg = new Dawg(dawgDictionary)
      val result = dawg.findAllWords(info.toLowerCase, false)
      if (result.isEmpty) {
        false
      } else {
        true
      }
    }
  }

}