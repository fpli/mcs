package com.ebay.traffic.chocolate.job.util

import scala.collection.mutable

object AmsDiffReportGenerator {
  def getTable(totalCount:(Long, Long),
               userIdPercent: (Double, Double),
               lastVwdItemIdPercent: (Double, Double),
               diff_count:mutable.LinkedHashMap[String,Long]):String={

    var diffResultHtml:String="";
    diff_count.foreach(t => {
      val columnName: String = t._1
      val diffCount: Long = t._2
      val tr:String="<tr>\n        <td class=\"a\">/column_name/</td>\n        <td class=\"a\">/diff_count/</td>\n    </tr> \n"
        .replaceAll("/column_name/",columnName)
        .replaceAll("/diff_count/",diffCount+"")
      diffResultHtml+=tr
    })
    val html:String=scala.io.Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("html/amsDiffReport.html")).mkString
    html
      .replaceAll("/new_total_count/",totalCount._1.toString)
      .replaceAll("/old_total_count/",totalCount._2.toString)
      .replaceAll("/old_user_id_percent/",userIdPercent._2.toString+"%")
      .replaceAll("/new_last_percent/",lastVwdItemIdPercent._1.toString+"%")
      .replaceAll("/old_last_percent/",lastVwdItemIdPercent._2.toString+"%")
      .replaceAll("/diff_result_html/",diffResultHtml)
  }
}
