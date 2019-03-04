package com.ebay.traffic.chocolate.sparknrt.imkTransform

import scopt.OptionParser

case class Parameter(appName: String = "crabTransform",
                     mode: String = "yarn",
                     channel: String = "",
                     transformedPrefix: String = "",
                     workDir: String = "",
                     outputDir: String = "",
                     compressOutPut: Boolean = false)

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("crabTransform") {
    head("crabTransform")

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

    opt[String]("transformedPrefix")
      .required
      .valueName("transformedPrefix")
      .action((cont, param) => param.copy(transformedPrefix = cont))

    opt[String]("workDir")
      .required
      .valueName("workDir")
      .action((cont, param) => param.copy(workDir = cont))

    opt[String]("outputDir")
      .required
      .valueName("outputDir")
      .action((cont, param) => param.copy(outputDir = cont))

    opt[Boolean]("compressOutPut")
      .required
      .valueName("compressOutPut")
      .action((cont, param) => param.copy(compressOutPut = cont))
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}