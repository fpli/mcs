package com.ebay.traffic.chocolate.listener.channel;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * A simple interface to indicate the required behavior for each channel
 */
public interface Channel {
	/**
	 * Process request
	 * 
	 * @param request incoming client request
	 * @param response response from proxy
	 */
	void process(HttpServletRequest request, HttpServletResponse response);

}
