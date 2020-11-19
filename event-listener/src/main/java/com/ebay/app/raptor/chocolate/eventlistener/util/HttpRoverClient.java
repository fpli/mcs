package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import io.netty.handler.codec.http.HttpHeaders;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.asynchttpclient.*;
import org.asynchttpclient.util.HttpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

import static org.asynchttpclient.Dsl.asyncHttpClient;
import static org.asynchttpclient.Dsl.config;

/**
 * @author xiangli4
 * Async call rover client. It's used for forwarding roverized deeplink to rover.
 * This is short term fix for native app missing clicks. In the long run, the roverized deeplink should be completely
 * replace by universal link.
 */
@Component
@DependsOn("EventListenerService")
public class HttpRoverClient {
  private static final Logger logger = LoggerFactory.getLogger(HttpRoverClient.class);
  private Metrics metrics;
  private AsyncHttpClient asyncHttpClient;
  private static final int TIMEOUT=200;

  @PostConstruct
  public void postInit() {
    this.metrics = ESMetrics.getInstance();
    AsyncHttpClientConfig config = config()
        .setRequestTimeout(TIMEOUT)
        .setConnectTimeout(TIMEOUT)
        .setReadTimeout(TIMEOUT)
        .build();
    this.asyncHttpClient = asyncHttpClient(config);
  }

  private String generateTimestampForCookie() {
    LocalDateTime now = LocalDateTime.now();

    // GUID, CGUID has 2 years expiration time
    LocalDateTime expiration = now.plusYears(2);

    // the last 8 hex number is the unix timestamp in seconds
    long timeInSeconds = expiration.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() / 1000;
    return Long.toHexString(timeInSeconds);
  }

  private String buildDebugLog(Request roverRequest, String prefix) {
    StringBuilder headers = new StringBuilder();
    for (Map.Entry<String, String> header : roverRequest.getHeaders()) {
      headers.append(header.toString()).append(",");
    }
    return prefix + roverRequest.getUrl() + ", headers: " + headers;
  }

  /**
   * Forward deeplink URL to rover
   * @param targetUrl original rover URL
   * @param request http request
   * @param internalHost internal hostevent-listener/src/main/java/com/ebay/app/raptor/chocolate/eventlistener/util/HttpRoverClient.java
   */
  public void forwardRequestToRover(String targetUrl, String internalHost, HttpServletRequest request) {
    try {
      URIBuilder uriBuilder = new URIBuilder(targetUrl);
      List<NameValuePair> queryParameters = uriBuilder.getQueryParams();
      Set<String> queryNames = new HashSet<>();
      for (Iterator<NameValuePair> queryParameterItr = queryParameters.iterator(); queryParameterItr.hasNext(); ) {
        NameValuePair queryParameter = queryParameterItr.next();
        //remove mpre if necessary. When there is mpre, rover won't overwrite guid by udid
        if (queryParameter.getName().equals("mpre")) {
          queryParameterItr.remove();
        }
        queryNames.add(queryParameter.getName());
      }
      uriBuilder.setParameters(queryParameters);
      uriBuilder.setHost(internalHost);

      String guid = "";
      String cguid = "";
      String trackingHeader = request.getHeader("X-EBAY-C-TRACKING");
      for (String seg : trackingHeader.split(",")) {
        String[] keyValue = seg.split("=");
        if (keyValue.length == 2) {
          if (keyValue[0].equalsIgnoreCase("guid")) {
            guid = keyValue[1];
          }
          if (keyValue[0].equalsIgnoreCase("cguid")) {
            cguid = keyValue[1];
          }
        }
      }
      // add udid parameter from tracking header's guid if udid is not in rover url. The udid will be set as guid by rover later
      if (!queryNames.contains("udid")) {
        if (!guid.isEmpty()) {
          uriBuilder.addParameter("udid", guid);
        }
      }

      // add nrd=1 if not exist
      if (!queryNames.contains("nrd")) {
        uriBuilder.addParameter("nrd", "1");
      }

      // add mcs=1 for marking mcs forwarding
      uriBuilder.addParameter("mcs", "1");

      final String rebuiltRoverUrl = uriBuilder.build().toString();

      RequestBuilder requestBuilder = new RequestBuilder();
      final Enumeration<String> headers = request.getHeaderNames();
      while (headers.hasMoreElements()) {
        final String header = headers.nextElement();
        if (header.equalsIgnoreCase("x-forwarded-for") ||
            header.equalsIgnoreCase("user-agent")) {
          final Enumeration<String> values = request.getHeaders(header);
          //just pass one header value to rover. Multiple value will cause parse exception on [] brackets.
          requestBuilder.addHeader(header, values.nextElement());
        }
      }

      // add guid and cguid in request cookie header
      if (!guid.isEmpty() || !cguid.isEmpty()) {
        String cookie = "npii=";
        String timestamp = generateTimestampForCookie();
        if (!guid.isEmpty())
          cookie += "btguid/" + guid + timestamp + "^";
        if (!cguid.isEmpty())
          cookie += "cguid/" + cguid + timestamp + "^";
        requestBuilder.addHeader("Cookie", cookie);
      }

      requestBuilder.setMethod(HttpConstants.Methods.GET);
      requestBuilder.setUrl(rebuiltRoverUrl);
      Request roverRequest = requestBuilder.build();

      asyncHttpClient.prepareRequest(roverRequest).execute(new AsyncHandler<Integer>() {
        private Integer status;
        @Override
        public State onStatusReceived(HttpResponseStatus responseStatus) throws Exception {
          status = responseStatus.getStatusCode();
          if (status == HttpConstants.ResponseStatusCodes.MOVED_PERMANENTLY_301) {
            metrics.meter("ForwardRoverRedirect");
            logger.warn(buildDebugLog(roverRequest, "ForwardRoverRedirect req. URI: "));
          } else if (status == HttpConstants.ResponseStatusCodes.OK_200) {
            metrics.meter("ForwardRoverSuccess");
          } else {
            metrics.meter("ForwardRoverFail");
            logger.warn(buildDebugLog(roverRequest, "ForwardRoverFail req. URI: "));
          }
          return State.ABORT;
        }

        @Override
        public State onHeadersReceived(HttpHeaders httpHeaders) throws Exception {
          return null;
        }

        @Override
        public State onBodyPartReceived(HttpResponseBodyPart httpResponseBodyPart) throws Exception {
          return null;
        }

        @Override
        public void onThrowable(Throwable t) {
          metrics.meter("ForwardRoverException");
          logger.warn(buildDebugLog(roverRequest, "ForwardRoverException req. URI: "));
        }

        @Override
        public Integer onCompleted() throws Exception {
          return null;
        }
      });
    } catch (Exception e) {
      metrics.meter("ForwardRoverExceptionOther");
      logger.warn(e.getMessage());
    }
  }
}