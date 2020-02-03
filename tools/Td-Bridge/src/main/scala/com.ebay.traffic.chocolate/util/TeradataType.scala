package com.ebay.traffic.chocolate.util

/**
  * Created by lxiong1
  */
object TeradataType extends Enumeration {

  type TeradataType = Value

  val Hopper = Value("hopper")
  val Mozart = Value("mozart")

  def getTdType(tdTypeStr : String) : TeradataType = {
    if (Hopper.toString.equalsIgnoreCase(tdTypeStr)) {
      return Hopper
    } else if (Mozart.toString.equalsIgnoreCase(tdTypeStr)) {
      return Mozart
    }

    return null
  }

}