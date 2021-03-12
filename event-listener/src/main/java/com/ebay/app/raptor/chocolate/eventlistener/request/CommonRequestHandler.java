/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.request;

import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import static com.ebay.app.raptor.chocolate.constant.Constants.AUTH_HEADER;

/**
 * @author xiangli4
 * Handle common request covering tracking header, end user context etc.
 */
@Component
@DependsOn("EventListenerService")
public class CommonRequestHandler {
  public Map<String, String> getHeaderMaps(HttpServletRequest clientRequest) {
    Map<String, String> headers = new HashMap<>();
    if(clientRequest.getHeaderNames() != null) {
      for (Enumeration<String> e = clientRequest.getHeaderNames(); e.hasMoreElements(); ) {
        String headerName = e.nextElement();
        // skip auth header
        if (headerName.equalsIgnoreCase(AUTH_HEADER)) {
          continue;
        }
        headers.put(headerName, clientRequest.getHeader(headerName));
      }
    }
    return headers;
  }
}
