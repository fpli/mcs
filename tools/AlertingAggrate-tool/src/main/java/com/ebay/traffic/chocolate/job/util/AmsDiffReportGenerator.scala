package com.ebay.traffic.chocolate.job.util

object AmsDiffReportGenerator {
  def getTable(new_count:Long,
               old_count:Long,
               new_user_id_percent:Double,
               old_user_id_percent:Double,
               new_last_percent:Double,
               old_last_percent:Double,
               diff_count:Map[String,Long]):String={

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
      .replaceAll("/new_count/",new_count.toString)
      .replaceAll("/old_count/",old_count.toString)
      .replaceAll("/new_user_id_percent/",new_user_id_percent.toString+"%")
      .replaceAll("/old_user_id_percent/",old_user_id_percent.toString+"%")
      .replaceAll("/new_last_percent/",new_last_percent.toString+"%")
      .replaceAll("/old_last_percent/",old_last_percent.toString+"%")
      .replaceAll("/diff_result_html/",diffResultHtml)
  }
}
