package com.ebay.traffic.chocolate.email;

public class HTMLTemplate {

	public String getContent() {
		String html = "";
		html = html + "<html>"
			+ "<head>"
			+ "<title>alerting report</title>"
			+ "</head>"
			+ "</html>";

		return html;
	}

}
