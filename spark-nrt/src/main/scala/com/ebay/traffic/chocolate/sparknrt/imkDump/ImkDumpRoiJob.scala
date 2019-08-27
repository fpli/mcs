package com.ebay.traffic.chocolate.sparknrt.imkDump

import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}

/**
  * Read roi channel dedupe result and generate files for imk table.
  * Adding roi related columns in imk_rvr_trckng_event: item_id, transaction_id, transaction_type, cart_id
  * and columns needed for imk_rvr_trckng_mgvalue: mgvalue, mgvaluereason
  *
  * @author Xiang Li
  * @since 2019-08-08
  */

object ImkDumpRoiJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new ImkDumpRoiJob(params)

    job.run()
    job.stop()
  }
}

class ImkDumpRoiJob(params: Parameter) extends ImkDumpJob(params: Parameter) {

  @transient override lazy val inputMetadata: Metadata = {
    val usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("imkdump.upstream.roi"))
    Metadata(params.workDir, params.channel, usage)
  }

  // override meta. Roi events don't need capping rules. Consume dedupe meta.
  @transient override lazy val metaPostFix = ""

  override def imkDumpCore(df: DataFrame): DataFrame = {
    val imkDf = super.imkDumpCommon(df)
      .drop("dst_client_id")
      .withColumn("dst_client_id", getClientIdFromRotationIdUdf(col("dst_rotation_id")))
      .withColumn("roi_item_id", getItemIdUdf(col("uri"), col("channel_type")))
      .withColumn("transaction_id", getRoiIdsUdf(lit(2), col("temp_uri_query")))
      .withColumn("transaction_type", getParamFromQueryUdf(col("temp_uri_query"), lit("tranType")))
      .withColumn("cart_id", getRoiIdsUdf(lit(3), col("temp_uri_query")))
      .withColumn("mgvalue", lit("0"))
      .withColumn("mgvaluereason", getMgvaluereasonUdf(col("brwsr_name"), col("clnt_remote_ip")))
      .drop("lang_cd")

    imkDumpEx(imkDf)
  }

  val getRoiIdsUdf: UserDefinedFunction = udf((index: Int, uri: String) => tools.getRoiIdFromUri(index, uri))
  val getClientIdFromRotationIdUdf: UserDefinedFunction = udf((rotationId: String) => tools.getClientIdFromRotationId(rotationId))
}
