package com.ebay.traffic.chocolate.sparknrt.epnnrt.dataParity

import scopt.OptionParser

case class Parameter(appName: String = "EpnNrtDataParity",
                     mode: String = "yarn",
                     sqlFile: String = "")

object Parameter {

  private lazy val parser = new OptionParser[Parameter]("EpnNrtDataParity") {
    head("EpnNrtDataParity")

    opt[String]("appName")
      .optional
      .valueName("application name")
      .action((cont, param) => param.copy(appName = cont))

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))

    opt[String]("sqlFile")
      .required
      .valueName("sqlFile")
      .action((cont, param) => param.copy(sqlFile = cont))
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}