package com.ebay.traffic.chocolate.job.util

import java.time.LocalDate
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar

object DateUtil {

  val dateFormat = "yyyy-MM-dd"

  def getToady(isTest: Boolean): String = {
    if (!isTest) {
      getDateBeforeNDay(0)
    } else {
      "2019-07-28"
    }
  }

  def getYesterday(isTest: Boolean): String = {
    if (!isTest) {
      getDateBeforeNDay(1)
    } else {
      "2019-07-27"
    }
  }

  def getBeforeYesterday(isTest: Boolean): String = {
    if (!isTest) {
      getDateBeforeNDay(2)
    } else {
      "2019-07-26"
    }
  }

  def getHalfMonthYesterday(input: String, isTest: Boolean): String = {
    if (!isTest) {
      val inputDir = new StringBuffer()
      for (i <- 0 to 14) {
        inputDir.append(input + "date=" + getDateBeforeNDay(i) + "/dw_ams.ams_clicks_cs_*.dat.gz,")
      }
      new String(inputDir.append(input + "date=" + getDateBeforeNDay(14) + "/dw_ams.ams_clicks_cs_*.dat.gz"))
    } else {
      val inputDir = new StringBuffer()
      inputDir.append(input + "date=2019-07-28/dw_ams.ams_clicks_cs_*.dat.gz" + ",")
      inputDir.append(input + "date=2019-07-27/dw_ams.ams_clicks_cs_*.dat.gz" + ",")
      inputDir.append(input + "date=2019-07-26/dw_ams.ams_clicks_cs_*.dat.gz" + ",")
      inputDir.append(input + "date=2019-07-25/dw_ams.ams_clicks_cs_*.dat.gz" + ",")
      inputDir.append(input + "date=2019-07-24/dw_ams.ams_clicks_cs_*.dat.gz" + ",")
      inputDir.append(input + "date=2019-07-23/dw_ams.ams_clicks_cs_*.dat.gz" + ",")
      inputDir.append(input + "date=2019-07-22/dw_ams.ams_clicks_cs_*.dat.gz" + ",")
      inputDir.append(input + "date=2019-07-21/dw_ams.ams_clicks_cs_*.dat.gz" + ",")
      inputDir.append(input + "date=2019-07-20/dw_ams.ams_clicks_cs_*.dat.gz" + ",")
      inputDir.append(input + "date=2019-07-19/dw_ams.ams_clicks_cs_*.dat.gz" + ",")
      inputDir.append(input + "date=2019-07-18/dw_ams.ams_clicks_cs_*.dat.gz" + ",")
      inputDir.append(input + "date=2019-07-17/dw_ams.ams_clicks_cs_*.dat.gz" + ",")
      inputDir.append(input + "date=2019-07-16/dw_ams.ams_clicks_cs_*.dat.gz" + ",")
      inputDir.append(input + "date=2019-07-15/dw_ams.ams_clicks_cs_*.dat.gz" + ",")
      new String(inputDir.append(input + "date=2019-07-14/dw_ams.ams_clicks_cs_*.dat.gz"))
    }
  }

  def getDateBeforeNDay(n: Integer): String = {
    val date: LocalDate = LocalDate.now.minusDays(n.toInt)
    date.toString
  }

  def getDateBeforeNDay(date: String, n: Integer): String = {
    val format = new SimpleDateFormat(dateFormat)
    val currentDate = format.parse(date)
    val c = Calendar.getInstance()
    c.setTime(currentDate)
    c.add(Calendar.DATE, n)
    format.format(c.getTime())
  }

}
