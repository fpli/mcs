package com.ebay.traffic.chocolate.sparknrt.mergeSmallFiles

import scopt.OptionParser

case class Parameter(appName: String = "MergeSmallFiles",
                     mode: String = "yarn",
                     table: String = "",
                     partitionName: String = "",
                     partition: String = "",
                     partitionNum: Int = 10,
                     outputDir: String = "")

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("MergeSmallFiles") {
    head("MergeSmallFiles")

    opt[String]("appName")
      .optional
      .valueName("application name")
      .action((cont, param) => param.copy(appName = cont))

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))

    opt[String]("table")
      .required
      .valueName("table")
      .action((cont, param) => param.copy(table = cont))

    opt[String]("partitionName")
      .required
      .valueName("partitionName")
      .action((cont, param) => param.copy(partitionName = cont))

    opt[String]("partition")
      .required
      .valueName("partition")
      .action((cont, param) => param.copy(partition = cont))

    opt[Int]("partitionNum")
      .required
      .valueName("partitionNum")
      .action((cont, param) => param.copy(partitionNum = cont))

    opt[String]("outputDir")
      .required
      .valueName("outputDir")
      .action((cont, param) => param.copy(outputDir = cont))
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}

