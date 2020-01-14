package com.ebay.traffic.chocolate.util;

public class ToolsUtil {

  public static String getDiff(String a, String b) {
    String diff = "0";

    try {
      diff = String.valueOf(Integer.parseInt(a) - Integer.parseInt(b));
    } catch (Exception e){
      return "0";
    }

    return diff;
  }

}
