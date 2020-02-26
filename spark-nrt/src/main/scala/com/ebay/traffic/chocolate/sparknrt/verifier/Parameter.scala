package com.ebay.traffic.chocolate.sparknrt.verifier

import scopt.OptionParser

case class Parameter(appName: String = "RuleVerifier",
                     mode: String = "yarn",
                     workPath: String = "/tmp",
                     srcPath: String = "",
                     targetPath: String = "",
                     outputPath: String = "",
                     selfCheck: Boolean = false)

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("RuleVerifier") {
    head("RuleVerifier", "version", "1.0")

    opt[String]("appName")
      .optional()
      .valueName("application name")
      .text("spark application name")
      .action((x, c) => c.copy(appName = x))

    opt[String]("mode")
      .optional()
      .valueName("mode")
      .text("spark job running on yarn or local")
      .action((x, c) => c.copy(mode = x))

    opt[String]("workPath")
      .optional()
      .valueName("workPath")
      .text("work path")
      .action((x, c) => c.copy(workPath = x))

    opt[String]("srcPath")
      .required()
      .valueName("srcPath")
      .text("source data to be verified")
      .action((x, c) => c.copy(srcPath = x))

    opt[String]("targetPath")
      .required()
      .valueName("targetPath")
      .text("target data that source data to be verified against")
      .action((x, c) => c.copy(targetPath = x))

    opt[String]("outputPath")
      .required()
      .valueName("outputPath")
      .text("file path where result data will be stored")
      .action((x, c) => c.copy(outputPath = x))

    opt[Boolean]("selfCheck")
      .optional()
      .valueName("selfCheck")
      .text("whether to open selfCheck")
      .action((x, c) => c.copy(selfCheck = x))
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
