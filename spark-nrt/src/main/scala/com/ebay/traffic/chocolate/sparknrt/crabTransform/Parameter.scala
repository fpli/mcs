package com.ebay.traffic.chocolate.sparknrt.crabTransform

import scopt.OptionParser

case class Parameter(appName: String = "crabSinkTransform",
                     mode: String = "yarn",
                     channel: String = "",
                     transformedPrefix: String = "",
                     workDir: String = "",
                     outputDir: String = "",
                     kwDataDir: String = "",
                     compressOutPut: Boolean = false,
                     maxMetaFiles: Int = 6,
                     elasticsearchUrl: String = "",
                     metaFile: String = "",
                     hdfsUri: String = "",
                     xidParallelNum: Int = 40)

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("crabSinkTransform") {
    head("crabSinkTransform")

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

    opt[String]("kwDataDir")
      .required
      .valueName("kwDataDir")
      .action((cont, param) => param.copy(kwDataDir = cont))

    opt[String]("outputDir")
      .required
      .valueName("outputDir")
      .action((cont, param) => param.copy(outputDir = cont))

    opt[Boolean]("compressOutPut")
      .required
      .valueName("compressOutPut")
      .action((cont, param) => param.copy(compressOutPut = cont))

    opt[Int]("maxMetaFiles")
      .optional
      .valueName("maxMetaFiles")
      .action((cont, param) => param.copy(maxMetaFiles = cont))

    opt[String]("elasticsearchUrl")
      .optional
      .valueName("elasticsearchUrl")
      .action((cont, param) => param.copy(elasticsearchUrl = cont))

    opt[String]("metaFile")
      .optional
      .valueName("metaFile")
      .action((cont, param) => param.copy(metaFile = cont))

    opt[String]("hdfsUri")
      .optional
      .valueName("hdfsUri")
      .action((cont, param) => param.copy(hdfsUri = cont))

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