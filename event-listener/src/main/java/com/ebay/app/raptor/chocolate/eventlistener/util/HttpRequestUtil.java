package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.kernel.util.HeaderMultiValue;
import com.ebay.kernel.util.RequestUtil;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by jialili1 on 11/2/20
 */
public class HttpRequestUtil {
  private static final Logger logger = LoggerFactory.getLogger(HttpRequestUtil.class);

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

  /**
   * Get request header value
   */
  public static String getHeaderValue(String header, String key) {
    try {
      HeaderMultiValue headerMultiValue;
      if (header != null) {
        headerMultiValue = new HeaderMultiValue(header, "utf-8");
        return headerMultiValue.get(key);
      }
    } catch (UnsupportedEncodingException e) {
      logger.warn("Failed to parse header {}", header, e);
    }

    return null;
  }

  /**
   * Get remote ip
   */
  public static String getRemoteIp(HttpServletRequest request) {
    String remoteIp = null;
    String xForwardFor = request.getHeader("X-Forwarded-For");
    if (xForwardFor != null && !xForwardFor.isEmpty()) {
      remoteIp = xForwardFor.split(",")[0];
    }

    if (remoteIp == null || remoteIp.isEmpty()) {
      remoteIp = RequestUtil.getRemoteAddr(request);
    }

    return remoteIp == null ? "" : remoteIp;
  }

  /**
   * Parse tag from url query string
   */
  public static String parseTagFromParams(MultiValueMap<String, String> parameters, String param) {
    if (parameters.containsKey(param) && parameters.getFirst(param) != null) {
      return parameters.getFirst(param);
    }

    return "";
  }
}
