package com.ebay.traffic.chocolate.utils

/**
  * Created by zhofan on 2019/06/11.
  */
object TDLoginParser {
  def getLogin(loginStr : String) : (String, String) = {
    val login = loginStr.split(",")
    (login.apply(0), login.apply(1))
  }
}
