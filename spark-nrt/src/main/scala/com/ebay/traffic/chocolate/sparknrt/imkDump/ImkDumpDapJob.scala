package com.ebay.traffic.chocolate.sparknrt.imkDump

import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

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

    job.run()
    job.stop()
  }
}

class ImkDumpDapJob(params: Parameter) extends ImkDumpJob(params: Parameter){

  @transient override lazy val inputMetadata: Metadata = {
    val usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("imkdump.upstream.display"))
    Metadata(params.workDir, params.channel, usage)
  }

  override def imkDumpCore(df: DataFrame): DataFrame = {
    val imkDf = super.imkDumpCore(df)
    imkDf
      .withColumn("dst_client_id", getClientIdUdf(col("temp_uri_query"), lit("mkrid")))
      .withColumn("item_id", getItemIdUdf(col("uri")))
      .withColumn("mgvalue", lit("0"))
      .withColumn("mgvaluereason", getMgvaluereasonUdf(col("brwsr_name"), col("clnt_remote_ip")))
  }

}