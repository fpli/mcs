package com.ebay.traffic.chocolate.sparknrt.imkDump

import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.spark.sql.DataFrame


class ImkDumpSocialJob(params: Parameter) extends ImkDumpJob(params: Parameter){

  @transient override lazy val inputMetadata: Metadata = {
    val usage = MetadataEnum.convertToMetadataEnum(properties.getProperty("imkdump.upstream.social"))
    Metadata(params.workDir, params.channel, usage)
  }

  override def imkDumpCore(df: DataFrame): DataFrame = {
    val imkDf = super.imkDumpCore(df)
    imkDf
  }
}