package com.ebay.traffic.chocolate.sparknrt.imkCrabTransformRotation

import scopt.OptionParser

case class Parameter(appName: String = "crabTransformRotationJob",
                     mode: String = "yarn",
                     transformedPrefix: String = "",
                     inputDir: String = "",
                     outputDir: String = "",
                     elasticsearchUrl: String = "")

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("crabTransformRotationJob") {
    head("crabTransformRotationJob")

    opt[String]("appName")
      .optional
      .valueName("application name")
      .action((cont, param) => param.copy(appName = cont))

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))

    opt[String]("transformedPrefix")
      .required
      .valueName("transformedPrefix")
      .action((cont, param) => param.copy(transformedPrefix = cont))

    opt[String]("inputDir")
      .required
      .valueName("inputDir")
      .action((cont, param) => param.copy(inputDir = cont))

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