package com.ebay.traffic.chocolate.job

import scopt.OptionParser


/**
  * Created by zhofan on 2019/06/11.
  */


case class BaseJobOptions(argMap: Map[String, String] = Map())

object BaseJobOptions {
  val parser = new OptionParser[BaseJobOptions]("mktJob") {
    arg[(String, String)]("argP")
      .keyValueName("argName", "argValue")
      .action((arg, options) => options.copy(argMap = options.argMap + arg))
      .text("variant arguments in key=value pairs")
      .optional()
      .unbounded()

    override def errorOnUnknownArgument: Boolean = false

    help("help")
    version("version")
  }

  def apply(args: Array[String]): BaseJobOptions = parser.parse(args, BaseJobOptions()) match {
    case Some(x) => x
    case None =>
      System.exit(1)
      null
  }
}