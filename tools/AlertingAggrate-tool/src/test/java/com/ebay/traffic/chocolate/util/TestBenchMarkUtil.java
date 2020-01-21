package com.ebay.traffic.chocolate.util;

import org.junit.Test;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class TestBenchMarkUtil {

  private static String getTimeRange(int daynums) {
    LocalDate yesterday = LocalDate.now().minus(1, ChronoUnit.DAYS);
    LocalDate startDay = LocalDate.now().minus(daynums, ChronoUnit.DAYS);

    if (daynums == 1) {
      return "= '" + yesterday + "'";
    } else if (daynums > 1) {
      return "between '" + startDay + "' and '" + yesterday + "'";
    } else {
      return "= '1970-01-01'";
    }
  }

  @Test
  public void testIMKSQL() {
    String sql = "select rvr_chnl_type_cd, rvr_cmnd_type_cd, count(*) from choco_data.imk_rvr_trckng_event where dt %s group by rvr_chnl_type_cd, rvr_cmnd_type_cd order by rvr_chnl_type_cd, rvr_cmnd_type_cd";
    String formatSql= String.format(sql, getTimeRange(2));

    System.out.println(formatSql);
  }

  @Test
  public void testEPNClickSQL() {
    String sql = "select count(*) from choco_data.ams_click where click_dt %s";
    String formatSql = String.format(sql, getTimeRange(1));

    System.out.println(formatSql);
  }

  @Test
  public void testEPNImpressionSQL() {
    String sql = "select count(*) from choco_data.ams_imprsn where imprsn_dt %s";
    String formatSql = String.format(sql, getTimeRange(2));

    System.out.println(formatSql);

    String str=String.format("Hi,%s", "王力");
    System.out.println(str);
  }

}
