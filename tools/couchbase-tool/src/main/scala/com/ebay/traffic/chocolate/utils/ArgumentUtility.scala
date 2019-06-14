package com.ebay.traffic.chocolate.utils

import org.slf4j.LoggerFactory


/**
  * Created by zhofan on 2019/06/11.
  */
object ArgumentUtility {
  private val logger = LoggerFactory.getLogger(this.getClass);

  lazy val MACRO_PATTERN = """\{/((\w|\.)+)\/}""".r

  def instantiateArgsWithMacro(macroArgMap: Map[String, String]): Map[String, String] = {
    macroArgMap.map{case (argName, _) =>
      argName -> parseArg(argName, macroArgMap)
    }
  }

  def parseArg(argName: String, argMap: Map[String, String]): String = {
    logger.info("parseArg START")
    logger.info(argName)
    argMap.get(argName) match {
      case Some(argValue) =>
        MACRO_PATTERN.findAllMatchIn(argValue)
          .foldLeft(argValue){(originStr, matcher) =>
            val key = matcher.group(1)
            originStr.replaceAll("""\{/%s/\}""".format(key), parseArg(key, argMap - argName))
          }
      case None => ""
    }
  }
  logger.info("parseArg END")

}
