package com.ebay.traffic.chocolate.util;

import com.ebay.traffic.chocolate.parse.AirflowHTMLParse;
import com.ebay.traffic.chocolate.pojo.AirflowDag;
import com.ebay.traffic.chocolate.report.AirflowReport;

import java.util.ArrayList;
import java.util.HashMap;

public class AirflowUtil {

    public static String getAirflowReportHtml() {
        HashMap<String, ArrayList<AirflowDag>> map = AirflowReport.getAirflowMap();
        return AirflowHTMLParse.parse(map);
    }

}
