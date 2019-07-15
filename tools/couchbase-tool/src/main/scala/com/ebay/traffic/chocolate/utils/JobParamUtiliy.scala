package com.ebay.traffic.chocolate.utils

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}


/**
  * Created by zhofan on 2019/06/11.
  */
object JobParamUtiliy {
  def getStringParam(paramName: String)
                    (implicit paramMap: Map[String, String]): String =
    paramMap.get(paramName) match {
      case Some(str) => str
      case None =>
        throw new Exception(s"Parameter $paramName is not found.")
    }

  def getStringParamOptional(paramName: String, defaultValue : String)
                            (implicit paramMap: Map[String, String]): String =
    paramMap.get(paramName) match {
      case Some(str) => str
      case None => defaultValue
    }

  def getDateParam(paramName: String, dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd"))
                  (implicit paramMap: Map[String, String]): Date =
    paramMap.get(paramName) match {
      case Some(str) =>
        new Date(dateFormat.parse(str).getTime)

      case None =>
        throw new Exception(s"Parameter $paramName is not found.")
    }

  def getTimestampParam(paramName: String, format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss"))
                       (implicit paramMap: Map[String, String]): Timestamp =
    paramMap.get(paramName) match {
      case Some(str) =>
        new Timestamp(format.parse(str).getTime)

      case None =>
        throw new Exception(s"Parameter $paramName is not found.")
    }

  def getIntParam(paramName: String)
                 (implicit paramMap: Map[String, String]): Int =
    paramMap.get(paramName) match {
      case Some(str) => str.toInt
      case None =>
        throw new Exception(s"Parameter $paramName is not found.")
    }

  def getIntParamOptional(paramName : String, defaultValue : Int)
                         (implicit paramMap: Map[String, String]) : Int = {
    paramMap.get(paramName) match {
      case Some(str) => str.toInt
      case None =>
        defaultValue
    }
  }

  def getLongParam(paramName: String)
                  (implicit paramMap: Map[String, String]): Long =
    paramMap.get(paramName) match {
      case Some(str) => str.toLong
      case None =>
        throw new Exception(s"Parameter $paramName is not found.")
    }

  def getBoolParam(paramName: String)
                  (implicit paramMap: Map[String, String]): Boolean =
    paramMap.get(paramName) match {
      case Some(str) => str.toBoolean
      case None =>
        throw new Exception(s"Parameter $paramName is not found.")
    }

  def getArray[T: ClassTag](paramName: String, delimiter: String)
                           (convertFunc: String => T)
                           (implicit paramMap: Map[String, String]): Array[T] =
    paramMap.get(paramName) match {
      case Some(str) =>
        str.split(delimiter)
          .map(valueStr => convertFunc(valueStr))

      case None =>
        throw new Exception(s"Parameter $paramName is not found.")
    }

  def getOption[T](paramName: String)
                  (getFunc: String => T)
                  (implicit paramMap: Map[String, String]): Option[T] = Try {
    getFunc(paramName)
  } match {
    case Success(x) => Some(x)
    case Failure(_) => None
  }

  def convertDelimiterFromHexToStr(delimiterHexStr: String): String = {
    val delimiterChar = Integer.valueOf(delimiterHexStr, 16).toChar

    delimiterChar.toString
  }

  def getDateTimeParam(paramName: String, timePattern: String = "yyyy-MM-dd_HH-mm-ss")
                      (implicit paramMap: Map[String, String]): DateTime = {
    paramMap.get(paramName) match {
      case Some(str) =>
        DateTime.parse(str, DateTimeFormat.forPattern(timePattern))

      case None =>
        throw new Exception(s"Parameter $paramName is not found.")
    }
  }
}
