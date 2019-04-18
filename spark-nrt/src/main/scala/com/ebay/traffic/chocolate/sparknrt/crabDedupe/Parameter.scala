package com.ebay.traffic.chocolate.sparknrt.crabDedupe

import scopt.OptionParser

case class Parameter(appName: String = "crabDedupe",
                     mode: String = "yarn",
                     workDir: String = "",
                     inputDir: String = "",
                     outputDir: String = "",
                     elasticsearchUrl: String = "",
                     maxDataFiles: Int = 100,
                     couchbaseDedupe: Boolean = false,
                     couchbaseTTL: Int = 2 * 24 * 60 * 60,
                     partitions: Int = 3,
                     snappyCompression: Boolean = true,
                     couchbaseDatasource: String = "")

object Parameter {
  private lazy val parser = new OptionParser[Parameter]("crabDedupe") {
    head("crabDedupe")

    opt[String]("appName")
      .optional
      .valueName("application name")
      .action((cont, param) => param.copy(appName = cont))

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))

    opt[String]("workDir")
      .required
      .valueName("workDir")
      .action((cont, param) => param.copy(workDir = cont))

    opt[String]("inputDir")
      .required
      .valueName("inputDir")
      .action((cont, param) => param.copy(inputDir = cont))

    opt[String]("outputDir")
      .required
      .valueName("outputDir")
      .action((cont, param) => param.copy(outputDir = cont))

    opt[Int]("maxDataFiles")
      .optional
      .valueName("maxDataFiles")
      .action((cont, param) => param.copy(maxDataFiles = cont))

    opt[String]("elasticsearchUrl")
      .optional
      .valueName("elasticsearchUrl")
      .action((cont, param) => param.copy(elasticsearchUrl = cont))

    opt[Boolean]("couchbaseDedupe")
      .optional()
      .valueName("couchbaseDedupe")
      .action((cont, param) => param.copy(couchbaseDedupe = cont))

    opt[Int]("couchbaseTTL")
      .optional()
      .valueName("couchbaseTTL")
      .action((cont, param) => param.copy(couchbaseTTL = cont))

    opt[Int]("partitions")
      .optional()
      .valueName("partitions")
      .action((cont, param) => param.copy(partitions = cont))

    opt[Boolean]("snappyCompression")
      .optional()
      .valueName("snappyCompression")
      .action((cont, param) => param.copy(snappyCompression = cont))

    opt[String]("couchbaseDatasource")
      .optional
      .valueName("couchbaseDatasource")
      .action((cont, param) => param.copy(couchbaseDatasource = cont))

  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }

}
