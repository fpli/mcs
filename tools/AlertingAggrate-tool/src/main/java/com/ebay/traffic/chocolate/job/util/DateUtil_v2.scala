package com.ebay.traffic.chocolate.job.util

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util.Calendar

object DateUtil_v2 {

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
        inputDir.append(input + "click_dt=" + getDateBeforeNDay(i) + "/dw_ams.ams_clicks_cs_*.snappy.parquet,")
      }
      new String(inputDir.append(input + "click_dt=" + getDateBeforeNDay(14) + "/dw_ams.ams_clicks_cs_*.snappy.parquet"))
    } else {
      val inputDir = new StringBuffer()
      inputDir.append(input + "click_dt=2019-07-28/part-00000-bb5e0287-b52d-42c5-94c2-134ea30cdd90.c000.snappy.parquet" + ",")
      inputDir.append(input + "click_dt=2019-07-27/part-00000-bb5e0287-b52d-42c5-94c2-134ea30cdd90.c000.snappy.parquet" + ",")
      inputDir.append(input + "click_dt=2019-07-26/part-00000-bb5e0287-b52d-42c5-94c2-134ea30cdd90.c000.snappy.parquet" + ",")
      inputDir.append(input + "click_dt=2019-07-25/part-00000-bb5e0287-b52d-42c5-94c2-134ea30cdd90.c000.snappy.parquet" + ",")
      inputDir.append(input + "click_dt=2019-07-24/part-00000-bb5e0287-b52d-42c5-94c2-134ea30cdd90.c000.snappy.parquet" + ",")
      inputDir.append(input + "click_dt=2019-07-23/part-00000-bb5e0287-b52d-42c5-94c2-134ea30cdd90.c000.snappy.parquet" + ",")
      inputDir.append(input + "click_dt=2019-07-22/part-00000-bb5e0287-b52d-42c5-94c2-134ea30cdd90.c000.snappy.parquet" + ",")
      inputDir.append(input + "click_dt=2019-07-21/part-00000-bb5e0287-b52d-42c5-94c2-134ea30cdd90.c000.snappy.parquet" + ",")
      inputDir.append(input + "click_dt=2019-07-20/part-00000-bb5e0287-b52d-42c5-94c2-134ea30cdd90.c000.snappy.parquet" + ",")
      inputDir.append(input + "click_dt=2019-07-19/part-00000-bb5e0287-b52d-42c5-94c2-134ea30cdd90.c000.snappy.parquet" + ",")
      inputDir.append(input + "click_dt=2019-07-18/part-00000-bb5e0287-b52d-42c5-94c2-134ea30cdd90.c000.snappy.parquet" + ",")
      inputDir.append(input + "click_dt=2019-07-17/part-00000-bb5e0287-b52d-42c5-94c2-134ea30cdd90.c000.snappy.parquet" + ",")
      inputDir.append(input + "click_dt=2019-07-16/part-00000-bb5e0287-b52d-42c5-94c2-134ea30cdd90.c000.snappy.parquet" + ",")
      inputDir.append(input + "click_dt=2019-07-15/part-00000-bb5e0287-b52d-42c5-94c2-134ea30cdd90.c000.snappy.parquet" + ",")
      new String(inputDir.append(input + "click_dt=2019-07-14/part-00000-bb5e0287-b52d-42c5-94c2-134ea30cdd90.c000.snappy.parquet"))
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
