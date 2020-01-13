package com.ebay.traffic.chocolate.sparknrt.imkDump

import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}

/**
  * Read natural search channel dedupe result and generate files for imk table.
  * Adding natural search related columns in imk_rvr_trckng_event:
  *
  * @author zhofan
  * @since 2019-10-30
  */

object ImkDumpNSJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new ImkDumpNSJob(params)

    job.run()
    job.stop()
  }
}

class ImkDumpNSJob(params: Parameter) extends ImkDumpJob(params: Parameter) {

  @transient override lazy val inputMetadata: Metadata = {
    val usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("imkdump.upstream.natural-search"))
    Metadata(params.workDir, params.channel, usage)
  }

  // override meta. Natural search events don't need capping rules. Consume dedupe meta.
  @transient override lazy val metaPostFix = ""

  override def imkDumpCore(df: DataFrame): DataFrame = {
    val imkDf = super.imkDumpCommon(df)
      .withColumn("ref",getDecodeParamUrlValueFromQueryUdf(col("temp_uri_query"), lit("mpvl")))
      .withColumn("user_query",getUserQueryFromRefUdf(col("ref")))
      .withColumn("rfrr_url", col("ref"))
      .withColumn("rfrr_dmn_name", getLandingPageDomainUdf(col("ref")))
      .withColumn("lndng_page_url", getLandingPageUrlUdf(getDecodeParamUrlValueFromQueryUdf(col("temp_uri_query"), lit("mpcl")), col("referer")))
      .withColumn("lndng_page_dmn_name", getLandingPageDomainUdf(col("lndng_page_url")))
      .withColumn("item_id", getParamFromQueryUdf(col("temp_uri_query"), lit("itemId")))
      .withColumn("geo_id", getParamFromQueryUdf(col("temp_uri_query"), lit("geo_id")))
      .withColumn("dst_client_id",getClientIdFromRoverUrlUdf(col("uri")))
      .withColumn("perf_track_name_value", getDecodePerfTrackNameValueUdf(col("temp_uri_query")))
      .drop("lang_cd")

    imkDumpEx(imkDf)
  }

  val getClientIdFromRoverUrlUdf: UserDefinedFunction = udf((uri: String) => tools.getClientIdFromRoverUrl(uri))
  val getUserQueryFromRefUdf: UserDefinedFunction = udf((ref: String) => tools.getUserQueryFromRef(ref))
  val getLandingPageUrlUdf: UserDefinedFunction = udf((mpcl: String, referer: String) => tools.getLandingPageUrlFromUriOrRfrr(mpcl, referer))
  val getDecodeParamUrlValueFromQueryUdf: UserDefinedFunction = udf((query: String, key: String) => tools.getDecodeParamUrlValueFromQuery(query, key))
  val getDecodePerfTrackNameValueUdf: UserDefinedFunction = udf((query: String) => tools.getDecodePerfTrackNameValue(query))
}

