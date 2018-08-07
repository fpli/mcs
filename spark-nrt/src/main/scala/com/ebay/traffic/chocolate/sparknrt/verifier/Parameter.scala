package com.ebay.traffic.chocolate.sparknrt.verifier

import scopt.OptionParser

case class Parameter(appName: String = "RuleVerifier",
                     mode: String = "yarn",
                     inputPath1: String = "",
                     inputPath2: String = "")

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("RuleVerifier") {
    head("RuleVerifier", "version", "1.0")

    opt[String]("appName")
      .optional()
      .valueName("<appName>")
      .text("spark application name")
      .action((x, c) => c.copy(appName = x))

    opt[String]("mode")
      .optional()
      .valueName("<mode>")
      .text("spark job running on yarn or local")
      .action((x, c) => c.copy(mode = x))

    opt[String]("inputPath1")
      .required()
      .valueName("<path>")
      .text("source data to be verified")
      .action((x, c) => c.copy(inputPath1 = x))

    opt[String]("inputPath2")
      .required()
      .valueName("<path>")
      .text("target data that source data to be verified against")
      .action((x, c) => c.copy(inputPath2 = x))
  }

  def apply(args: Array[String]): Parameter = {
    parser.parse(args, Parameter()) match {
      case Some(param) => param
      case None =>
        System.exit(1)
        null
    }
  }
}
