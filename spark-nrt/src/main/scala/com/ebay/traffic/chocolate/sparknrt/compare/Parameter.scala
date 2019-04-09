package com.ebay.traffic.chocolate.sparknrt.compare

import scopt.OptionParser

case class Parameter(appName: String = "compareJob",
                     mode: String = "yarn",
                     click_source: String = "",
                     click_dest: String = "",
                     impression_source: String = "",
                     impression_dest: String = "",
                     click_outputPath: String = "",
                     impression_outputPath: String = "",
                     click_run: Boolean = false,
                     impression_run: Boolean = false,
                     workDir: String = "")
object Parameter{
  private lazy val parser = new OptionParser[Parameter]("compareJob") {
    head("compareJob")

    opt[String]("appName")
      .optional
      .valueName("application name")
      .action((cont, param) => param.copy(appName = cont))

    opt[String]("mode")
      .optional
      .valueName("mode")
      .action((cont, param) => param.copy(mode = cont))

    opt[String]("click_source")
      .required
      .valueName("click_source")
      .action((cont, param) => param.copy(click_source = cont))

    opt[String]("click_dest")
      .required
      .valueName("click_dest")
      .action((cont, param) => param.copy(click_dest = cont))

    opt[String]("impression_source")
      .required
      .valueName("impression_source")
      .action((cont, param) => param.copy(impression_source = cont))

    opt[String]("impression_dest")
      .required
      .valueName("impression_dest")
      .action((cont, param) => param.copy(impression_dest = cont))

    opt[String]("click_outputPath")
      .required
      .valueName("click_outputPath")
      .action((cont, param) => param.copy(click_outputPath = cont))

    opt[String]("impression_outputPath")
      .required
      .valueName("impression_outputPath")
      .action((cont, param) => param.copy(impression_outputPath = cont))

    opt[Boolean]("click_run")
      .required
      .valueName("click_run")
      .action((cont, param) => param.copy(click_run = cont))

    opt[Boolean]("impression_run")
      .required
      .valueName("impression_run")
      .action((cont, param) => param.copy(impression_run = cont))

    opt[String]("workDir")
      .required
      .valueName("workDir")
      .action((cont, param) => param.copy(workDir = cont))
  }

  def apply(args: Array[String]): Parameter = parser.parse(args, Parameter()) match {
    case Some(param) => param
    case None =>
      System.exit(1)
      null
  }
}
