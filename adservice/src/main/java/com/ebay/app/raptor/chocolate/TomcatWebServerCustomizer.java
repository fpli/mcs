package com.ebay.app.raptor.chocolate;


import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * In some cases, a placeholder will not be replaced. The ad request url looks like
 * www.ebayadservices.com/marketingtracking/v1/ar?mkrid=521351&mkcid=4&mkevt=6&mpt=[CACHEBUSTER]&siteid=0&icep_siteid=0
 * &ipn=admain2&adtype=2&size=120x600&pgroup=521351&mpvc= )
 * Without this configure, adservice ar interface will return 400.
 *
 * @author Zhiyuan Wang
 * @since 2020/4/7
 */
@Component
public class TomcatWebServerCustomizer
        implements WebServerFactoryCustomizer<TomcatServletWebServerFactory> {

  @Override
  public void customize(TomcatServletWebServerFactory factory) {
    Map<String, String> hashMap = new HashMap<>();
    for (String o : hashMap.keySet()) {
      String value = hashMap.get(o);
      System.out.println(o + value);
    }
    factory.addConnectorCustomizers(connector -> {
      connector.setAttribute("relaxedPathChars", "<>[\\]^`{|}");
      connector.setAttribute("relaxedQueryChars", "<>[\\]^`{|}");
    });
  }
}