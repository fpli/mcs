package com.ebay.traffic.chocolate.job

import com.ebay.traffic.chocolate.utils.ArgumentUtility
import org.joda.time.DateTime


/**
  * Created by zhofan on 2019/06/11.
  */
abstract class BaseJob {
  protected var runTime: DateTime = null

  protected val retryCount = 0

  final def main(args: Array[String]): Unit = {
    val baseJobOptions = BaseJobOptions(args)

    val argMap = baseJobOptions.argMap


    val parsedArgMap = ArgumentUtility.instantiateArgsWithMacro(argMap)

    preExecuteStep(parsedArgMap)
    executeStep(parsedArgMap, retryCount)
    postExecuteStep(parsedArgMap)
  }

  def preExecuteStep(argMap: Map[String, String]): Unit = {
    try {
      preExecute(argMap)
    } catch {
      case e : Exception => {
        System.exit(1)
      }
    }
  }

  def executeStep(argMap: Map[String, String], retry : Int): Unit = {

    try {
      execute(argMap)
    } catch {
      case e : Exception => {
        if (retry > 0) {
          Thread.sleep(30000L)
          executeStep(argMap, retry - 1)
        } else {
          System.exit(1)
        }
      }
    }
  }

  def postExecuteStep(argMap: Map[String, String]): Unit = {
    try {
      postExecute(argMap)
    } catch {
      case e : Exception => {
        System.exit(1)
      }
    }
  }

  def preExecute(argMap: Map[String, String]): Unit = {}

  def execute(argMap: Map[String, String]): Unit

  def postExecute(argMap: Map[String, String]): Unit = {}
}

