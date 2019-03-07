package com.ebay.traffic.chocolate.sparknrt.imkDump

import scopt.OptionParser

/**
  * Created by ganghuang on 12/3/18.
  */
case class Parameter(appName: String = "Sword",
                     mode: String = "yarn",
                     channel: String = "",
                     workDir: String = "",
                     outPutDir: String = "",
                     partitions: Int = 3,
                     elasticsearchUrl: String = "")

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
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}