package com.ebay.traffic.chocolate.sparknrt.sword

import scopt.OptionParser

/**
  * Created by weibdai on 5/19/18.
  */
case class Parameter(appName: String = "Sword",
                     mode: String = "yarn",
                     channel: String = "",
                     dataDir: String = "",
                     workDir: String = "",
                     bootstrapServers: String = "",
                     kafkaTopic: String = "")

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

    opt[String]("dataDir")
      .required
      .valueName("dataDir")
      .action((cont, param) => param.copy(dataDir = cont))

    opt[String]("workDir")
      .required
      .valueName("workDir")
      .action((cont, param) => param.copy(workDir = cont))

    opt[String]("bootstrapServers")
      .required
      .valueName("bootstrapServers")
      .action((cont, param) => param.copy(bootstrapServers = cont))

    opt[String]("kafkaTopic")
      .required
      .valueName("kafkaTopic")
      .action((cont, param) => param.copy(kafkaTopic = cont))
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}