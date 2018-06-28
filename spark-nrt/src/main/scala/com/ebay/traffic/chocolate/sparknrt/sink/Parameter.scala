package com.ebay.traffic.chocolate.sparknrt.sink

import scopt.OptionParser

/**
  * Created by yliu29 on 3/8/18.
  */
case class Parameter(appName: String = "DedupeAndSink",
                     mode: String = "yarn",
                     channel: String = "",
                     kafkaTopic: String = "",
                     workDir: String = "",
                     outputDir: String = "",
                     partitions: Int = 3,
                     elasticsearchUrl: String = "",
                     maxConsumeSize: Long = 1000000l,
                     couchbaseDedupe: Boolean = false,
                     couchbaseTTL: Int = 3 * 24 * 60 * 60)

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("DedupeAndSink") {
    head("DedupeAndSink")

    opt[String]("appName")
      .optional
      .valueName("application name")
      .action((cont, param) => param.copy(appName = cont))

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))

    opt[String]("channel")
      .required
      .valueName("channel")
      .action((cont, param) => param.copy(channel = cont))

    opt[String]("kafkaTopic")
      .required
      .valueName("kafka topic")
      .action((cont, param) => param.copy(kafkaTopic = cont))

    opt[String]("workDir")
      .required
      .valueName("workDir")
      .action((cont, param) => param.copy(workDir = cont))

    opt[String]("outputDir")
      .required
      .valueName("outputDir")
      .action((cont, param) => param.copy(outputDir = cont))

    opt[Int]("partitions")
      .optional
      .valueName("partitions")
      .action((cont, param) => param.copy(partitions = cont))

    opt[String]("elasticsearchUrl")
      .optional
      .valueName("elasticsearchUrl")
      .action((cont, param) => param.copy(elasticsearchUrl = cont))

    opt[Long]("maxConsumeSize")
      .optional
      .valueName("maxConsumeSize")
      .action((cont, param) => param.copy(maxConsumeSize = cont))

    opt[Boolean]("couchbaseDedupe")
      .optional()
      .valueName("couchbaseDedupe")
      .action((cont, param) => param.copy(couchbaseDedupe = cont))

    opt[Int]("couchbaseTTL")
      .optional()
      .valueName("couchbaseTTL")
      .action((cont, param) => param.copy(couchbaseTTL = cont))
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}