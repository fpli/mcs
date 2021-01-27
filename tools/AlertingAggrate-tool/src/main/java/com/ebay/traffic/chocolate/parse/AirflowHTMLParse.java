package com.ebay.traffic.chocolate.parse;

import com.ebay.traffic.chocolate.html.AirflowTable;
import com.ebay.traffic.chocolate.pojo.AirflowDag;

import java.util.ArrayList;
import java.util.HashMap;

public class AirflowHTMLParse {

    public static String parse(HashMap<String, ArrayList<AirflowDag>> map) {
        String html = "";
        html = AirflowTable.parseProject(map);
        return html;
    }

}
