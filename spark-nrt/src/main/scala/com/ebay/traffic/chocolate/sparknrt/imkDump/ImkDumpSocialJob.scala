package com.ebay.traffic.chocolate.sparknrt.imkDump

import com.ebay.traffic.chocolate.sparknrt.meta.{Metadata, MetadataEnum}
import org.apache.spark.sql.DataFrame


/**
  * Read social channel capping result and generate files for imk table
  * @author huiclu
  * @since 2019-08-018
  */
object ImkDumpSocialJob extends App {
  override def main(args: Array[String]): Unit = {
    val params = Parameter(args)

    val job = new ImkDumpSocialJob(params)

    job.run()
    job.stop()
  }
}

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