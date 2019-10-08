package com.ebay.traffic.chocolate.sparknrt.crabTransformWatermark

import scopt.OptionParser

case class Parameter(appName: String = "calCrabTransformWatermark",
                     mode: String = "yarn",
                     crabTransformDataDir: String = "",
                     imkCrabTransformDataDir: String = "",
                     dedupAndSinkKafkaLagDir: String = "",
                     channels: String = "",
                     outputDir: String = "",
                     elasticsearchUrl: String = "")

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("calCrabTransformWatermark") {
    head("calCrabTransformWatermark")

    opt[String]("appName")
      .optional
      .valueName("application name")
      .action((cont, param) => param.copy(appName = cont))

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))

    opt[String]("dedupAndSinkKafkaLagDir")
      .required
      .valueName("dedupAndSinkKafkaLagDir")
      .action((cont, param) => param.copy(dedupAndSinkKafkaLagDir = cont))

    opt[String]("channels")
      .required
      .valueName("channels")
      .action((cont, param) => param.copy(channels = cont))

    opt[String]("imkCrabTransformDataDir")
      .required
      .valueName("imkCrabTransformDataDir")
      .action((cont, param) => param.copy(imkCrabTransformDataDir = cont))

    opt[String]("crabTransformDataDir")
      .required
      .valueName("crabTransformDataDir")
      .action((cont, param) => param.copy(crabTransformDataDir = cont))

    opt[String]("outputDir")
      .required
      .valueName("outputDir")
      .action((cont, param) => param.copy(outputDir = cont))

    opt[String]("elasticsearchUrl")
      .optional
      .valueName("elasticsearchUrl")
      .action((cont, param) => param.copy(elasticsearchUrl = cont))

  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}