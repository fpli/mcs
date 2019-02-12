package com.ebay.traffic.chocolate.sparknrt.verifier0

import scopt.OptionParser

/**
  * Created by jialili1 on 11/23/18.
  */
case class Parameter(appName: String = "RuleVerifier0",
                     mode: String = "yarn",
                     workPath: String = "/tmp",
                     chocoTodayPath: String = "",
                     chocoYesterdayPath: String = "",
                     chocoInputFormat: String = "parquet",
                     chocoInputDelimiter: String = "del",
                     chocoQuickCheck: Boolean = false,
                     epnTodayPath: String = "",
                     epnYesterdayPath: String = "",
                     epnInputFormat: String = "csv",
                     epnInputDelimiter: String = "bel",
                     epnQuickCheck: Boolean = false,
                     outputPath: String = "")

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("RuleVerifier") {
    head("RuleVerifier0", "version", "1.0")

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

    opt[String]("workPath")
      .optional()
      .valueName("<path>")
      .text("work path")
      .action((x, c) => c.copy(workPath = x))

    opt[String]("chocoTodayPath")
      .optional()
      .valueName("<path>")
      .text("choco today data to be verified")
      .action((x, c) => c.copy(chocoTodayPath = x))

    opt[String]("chocoYesterdayPath")
      .optional()
      .valueName("<path>")
      .text("choco yesterday data to be verified")
      .action((x, c) => c.copy(chocoYesterdayPath = x))

    opt[String]("chocoInputFormat")
      .optional()
      .valueName("<format>")
      .text("choco input format")
      .action((x, c) => c.copy(chocoInputFormat = x))

    opt[String]("chocoInputDelimiter")
      .optional()
      .valueName("<delimiter>")
      .text("choco input delimiter")
      .action((x, c) => c.copy(chocoInputDelimiter = x))

    opt[Boolean]("chocoQuickCheck")
      .optional()
      .valueName("<check>")
      .text("choco quick check")
      .action((x, c) => c.copy(chocoQuickCheck = x))

    opt[String]("epnTodayPath")
      .optional()
      .valueName("<path>")
      .text("epn today data to be verified")
      .action((x, c) => c.copy(epnTodayPath = x))

    opt[String]("epnYesterdayPath")
      .optional()
      .valueName("<path>")
      .text("epn yesterday data to be verified")
      .action((x, c) => c.copy(epnYesterdayPath = x))

    opt[String]("epnInputFormat")
      .optional()
      .valueName("<format>")
      .text("epn input format")
      .action((x, c) => c.copy(epnInputFormat = x))

    opt[String]("epnInputDelimiter")
      .optional()
      .valueName("<delimiter>")
      .text("epn input delimiter")
      .action((x, c) => c.copy(epnInputDelimiter = x))

    opt[Boolean]("epnQuickCheck")
      .optional()
      .valueName("<check>")
      .text("epn quick check")
      .action((x, c) => c.copy(epnQuickCheck = x))

    opt[String]("outputPath")
      .required()
      .valueName("<path>")
      .text("file path where result data will be stored")
      .action((x, c) => c.copy(outputPath = x))
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