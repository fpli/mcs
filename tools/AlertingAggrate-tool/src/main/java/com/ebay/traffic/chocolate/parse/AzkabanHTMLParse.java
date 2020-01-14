package com.ebay.traffic.chocolate.parse;

import com.ebay.traffic.chocolate.html.AzkabanFlowTable;
import com.ebay.traffic.chocolate.pojo.AzkabanFlow;

import java.util.ArrayList;
import java.util.HashMap;

public class AzkabanHTMLParse {

	public static String parse(HashMap<String, ArrayList<AzkabanFlow>> map) {
		String html = "";

		html = AzkabanFlowTable.parseProject(map);

		return html;
	}

}
