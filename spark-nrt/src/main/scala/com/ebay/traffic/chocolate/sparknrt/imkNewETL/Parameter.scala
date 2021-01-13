package com.ebay.traffic.chocolate.sparknrt.imkNewETL

import scopt.OptionParser

case class Parameter(appName: String = "Sword",
                     mode: String = "yarn",
                     channel: String = "",
                     workDir: String = "",
                     outPutDir: String = "",
                     partitions: Int = 3,
                     elasticsearchUrl: String = "",
                     transformedPrefix: String = "",
                     outputFormat: String = "",
                     compressOutPut: Boolean = false,
                     xidParallelNum: Int = 40)

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("Sword") {
    head("Sword")

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

    opt[String]("outPutDir")
      .required
      .valueName("outPutDir")
      .action((cont, param) => param.copy(outPutDir = cont))

    opt[Int]("partitions")
      .optional
      .valueName("partitions")
      .action((cont, param) => param.copy(partitions = cont))

    opt[String]("elasticsearchUrl")
      .optional
      .valueName("elasticsearchUrl")
      .action((cont, param) => param.copy(elasticsearchUrl = cont))

    opt[String]("outputFormat")
      .required
      .valueName("outputFormat")
      .action((cont, param) => param.copy(outputFormat = cont))

    opt[Boolean]("compressOutPut")
      .required
      .valueName("compressOutPut")
      .action((cont, param) => param.copy(compressOutPut = cont))

    opt[Int]("xidParallelNum")
      .optional
      .valueName("xidParallelNum")
      .action((cont, param) => param.copy(xidParallelNum = cont))
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}
