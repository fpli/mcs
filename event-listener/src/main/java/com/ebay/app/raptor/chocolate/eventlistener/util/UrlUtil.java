package com.ebay.app.raptor.chocolate.eventlistener.util;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;

import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jialili1 on 11/2/20
 */
public class UrlUtil {
  /**
   * Remove one url parameter
   */
  public static String removeParam(String url, String param) throws URISyntaxException {
    URIBuilder uriBuilder = new URIBuilder(url);
    List<NameValuePair> queryParameters = uriBuilder.getQueryParams();
    for (Iterator<NameValuePair> queryParameterItr = queryParameters.iterator(); queryParameterItr.hasNext(); ) {
      NameValuePair queryParameter = queryParameterItr.next();
      if (queryParameter.getName().equals(param)) {
        queryParameterItr.remove();
      }
    }
    uriBuilder.setParameters(queryParameters);

    return uriBuilder.build().toString();
  }
}
