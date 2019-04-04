package com.ebay.traffic.chocolate.sparknrt.epnnrt;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

public class utils {
  public static String removeQueryParameter(String url, String parameterName) throws URISyntaxException {
    URIBuilder uriBuilder = new URIBuilder(url);
    List<NameValuePair> queryParameters = uriBuilder.getQueryParams()
        .stream()
        .filter(p -> !p.getName().equals(parameterName)).collect(Collectors.toList());
    if (queryParameters.isEmpty()) {
      uriBuilder.removeQuery();
    } else {
      uriBuilder.setParameters(queryParameters);
    }
    return uriBuilder.build().toString();
  }

  public static String findDomainInUrl(String url) throws MalformedURLException {
    URL domain = new URL(url);
    return domain.getProtocol() + "://" + domain.getHost();
  }
}
