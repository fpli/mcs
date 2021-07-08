package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.kernel.util.StringUtils;
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
import javax.ws.rs.core.Response;
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
  private AsyncHttpClient asyncHttpClient;
  private static final int TIMEOUT=200;
  private static final int EXPIRE_COOKIE = 2;
  private static final int MILLISECOND_DIVISOR = 1000;
  private static final String UDID = "udid";
  private static final String NOT_REDIRECT = "nrd";
  private static final String MCS_FLAG = "mcs";
  private static final String FLAG_TRUE = "1";
  private static final String GUID_CONNECTOR = "^";
  private static final String GUID = "guid";
  private static final String CGUID = "cguid";
  private static final String NPII_PREFIX = "npii_";
  private static final String BTGUID_PREFIX = "btguid/";
  private static final String CGUID_PREFIX = "cguid/";

  @PostConstruct
  public void postInit() {
    AsyncHttpClientConfig config = config()
        .setRequestTimeout(TIMEOUT)
        .setConnectTimeout(TIMEOUT)
        .setReadTimeout(TIMEOUT)
        .build();
    this.asyncHttpClient = asyncHttpClient(config);
  }

  // For unit test
  void setAsyncHttpClient(AsyncHttpClient asyncHttpClient) {
    this.asyncHttpClient = asyncHttpClient;
  }

  void setMetrics(Metrics metrics) {
    MonitorUtil.setEsMetrics(metrics);
  }

  private String generateTimestampForCookie() {
    LocalDateTime now = LocalDateTime.now();

    // GUID, CGUID has 2 years expiration time
    LocalDateTime expiration = now.plusYears(EXPIRE_COOKIE);

    // the last 8 hex number is the unix timestamp in seconds
    long timeInSeconds = expiration.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli() / MILLISECOND_DIVISOR;
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
   * @param internalHost internal host
   * @param request incoming http request
   * @return Future object
   */
  public ListenableFuture<Integer> forwardRequestToRover(String targetUrl, String internalHost,
                                                         HttpServletRequest request) {
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

      RequestBuilder requestBuilder = new RequestBuilder();

      String guid = "";
      String cguid = "";
      String trackingHeader = request.getHeader("X-EBAY-C-TRACKING");
      if(!StringUtils.isEmpty(trackingHeader)) {
        for (String seg : trackingHeader.split(",")) {
          String[] keyValue = seg.split("=");
          if (keyValue.length == 2) {
            if (keyValue[0].equalsIgnoreCase(GUID)) {
              guid = keyValue[1];
            }
            if (keyValue[0].equalsIgnoreCase(CGUID)) {
              cguid = keyValue[1];
            }
          }
        }
      }
      // add guid and cguid in request cookie header
      if (!guid.isEmpty() || !cguid.isEmpty()) {
        String cookie = NPII_PREFIX;
        String timestamp = generateTimestampForCookie();
        if (!guid.isEmpty())
          cookie += BTGUID_PREFIX + guid + timestamp + GUID_CONNECTOR;
        if (!cguid.isEmpty())
          cookie += CGUID_PREFIX + cguid + timestamp + GUID_CONNECTOR;
        requestBuilder.addHeader("Cookie", cookie);
      }

      // add udid parameter from tracking header's guid if udid is not in rover url. The udid will be set as guid by rover later
      if (!queryNames.contains(UDID)) {
        if (!guid.isEmpty()) {
          uriBuilder.addParameter(UDID, guid);
        }
      }

      // add nrd=1 if not exist
      if (!queryNames.contains(NOT_REDIRECT)) {
        uriBuilder.addParameter(NOT_REDIRECT, FLAG_TRUE);
      }

      // add mcs=1 for marking mcs forwarding
      uriBuilder.addParameter(MCS_FLAG, FLAG_TRUE);

      final String rebuiltRoverUrl = uriBuilder.build().toString();

      final Enumeration<String> headers = request.getHeaderNames();
      if(headers!=null) {
        while (headers.hasMoreElements()) {
          final String header = headers.nextElement();
          if (header.equalsIgnoreCase("x-forwarded-for") ||
              header.equalsIgnoreCase("user-agent")) {
            final Enumeration<String> values = request.getHeaders(header);
            //just pass one header value to rover. Multiple value will cause parse exception on [] brackets.
            requestBuilder.addHeader(header, values.nextElement());
          }
        }
      }

      requestBuilder.setMethod(HttpConstants.Methods.GET);
      requestBuilder.setUrl(rebuiltRoverUrl);
      Request roverRequest = requestBuilder.build();

      return asyncHttpClient.prepareRequest(roverRequest).execute(new AsyncHandler<Integer>() {
        private Integer status;
        @Override
        public State onStatusReceived(HttpResponseStatus responseStatus) {
          status = responseStatus.getStatusCode();
          if (status == HttpConstants.ResponseStatusCodes.MOVED_PERMANENTLY_301) {
            MonitorUtil.info("ForwardRoverRedirect");
            logger.warn(buildDebugLog(roverRequest, "ForwardRoverRedirect req. URI: "));
          } else if (status == HttpConstants.ResponseStatusCodes.OK_200) {
            MonitorUtil.info("ForwardRoverSuccess");
          } else {
            MonitorUtil.info("ForwardRoverFail");
            logger.warn(buildDebugLog(roverRequest, "ForwardRoverFail req. URI: "));
          }
          return State.ABORT;
        }

        @Override
        public State onHeadersReceived(HttpHeaders httpHeaders) {
          return null;
        }

        @Override
        public State onBodyPartReceived(HttpResponseBodyPart httpResponseBodyPart) throws Exception {
          return null;
        }

        @Override
        public void onThrowable(Throwable t) {
          MonitorUtil.info("ForwardRoverException");
          logger.warn(buildDebugLog(roverRequest, "ForwardRoverException req. URI: "));
        }

        @Override
        public Integer onCompleted() throws Exception {
          return Response.Status.OK.getStatusCode();
        }
      });
    } catch (Exception e) {
      MonitorUtil.info("ForwardRoverExceptionOther");
      logger.warn(e.getMessage());
    }
    return null;
  }
}