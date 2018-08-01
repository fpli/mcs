package com.ebay.traffic.chocolate.listener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.kernel.util.StringUtils;
import com.ebay.traffic.chocolate.listener.ListenerProxyServlet;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;
import org.springframework.http.server.ServletServerHttpRequest;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for parsing the POJOs
 * 
 * @author kanliu
 */
public class MessageObjectParser {

    /** Logging instance */
    private static final Logger logger = Logger.getLogger(MessageObjectParser.class);
    private static MessageObjectParser INSTANCE;
    private static final long DEFAULT_PUBLISHER_ID = -1L;
    private static final String REDIRECT_SPECIAL_TAG = "chocolateSauce=";
    public static final String REDIRECT_HEADER = "Location";
    /* singleton class */
    private MessageObjectParser() {
    }
    
    /**
     * Convert a HTTP request to a listener message for Kafka.
     * 
     * @param clientRequest
     *            to use in parsing uri and timestamp
     * @param proxyResponse
     *            to use in parsing response headers
     * @param startTime
     *            as start time of the request
     * @return ListenerMessage  as the parse result.
     */
    public ListenerMessage parseHeader(
        final HttpServletRequest clientRequest,
        final HttpServletResponse proxyResponse, Long startTime, Long campaignId,
        final ChannelType channelType, final ChannelActionEnum action, String snid, String requestUrl) {

        ListenerMessage record = new ListenerMessage();
        // Set URI first as multipleRedirectHandler may overwrite URI
        if(StringUtils.isEmpty(requestUrl)){
           requestUrl = new ServletServerHttpRequest(clientRequest).getURI().toString();
        }
        record.setUri(requestUrl);

        // Handle multiple redirect logic, rover use 301 for redirect
        // Set the channel type + HTTP headers + channel action

        record.setChannelType(channelType);
        record.setHttpMethod(this.getMethod(clientRequest).getAvro());
        record.setChannelAction(action.getAvro());
        // Format record
        record.setRequestHeaders(serializeRequestHeaders(clientRequest));
        record.setResponseHeaders(serializeResponseHeaders(proxyResponse));
        record.setTimestamp(startTime);

        // Get snapshotId from request
        Long snapshotId = SnapshotId.getNext(ListenerOptions.getInstance().getDriverId(), startTime).getRepresentation();
        record.setSnapshotId(snapshotId);

        record.setCampaignId(campaignId);
        record.setPublisherId(DEFAULT_PUBLISHER_ID);
        record.setSnid((snid != null) ? snid : "");
        record.setIsTracked(false);     //TODO No messages are Durability-tracked for now

        return record;
    }

    /**
     * Get request method and validate from request
     * @param clientRequest Request
     * @return HttpMethodEnum
     */
    protected HttpMethodEnum getMethod(HttpServletRequest clientRequest) {
        HttpMethodEnum httpMethod = HttpMethodEnum.parse(clientRequest.getMethod());
        Validate.notNull(httpMethod, "Could not parse HTTP method from HTTP request=" + clientRequest.getMethod());
        return httpMethod;
    }


  private static final String CHOCO_TAG = "dashenId";
  private static final String REDIRECTION_CNT_TAG = "dashenCnt";
  private static MetricsClient metrics = MetricsClient.getInstance();
  public String appendURLWithChocolateTag(String urlStr) {
    URL url = null;
    try {
      url = new URL(urlStr);
    } catch (MalformedURLException e) {
      metrics.meter(ListenerProxyServlet.MALFORMED_URL);
    }
    String query = url.getQuery();
    // append snapshotId into URL
    if(!urlStr.contains(CHOCO_TAG)){
      if(query != null && ! query.isEmpty()) {
        urlStr += "&";
      }else{
        urlStr += "?";
      }
      urlStr += getChocoTag(urlStr);
    }
    // append redirection count into URL
    if(urlStr.contains(REDIRECTION_CNT_TAG)){
      Pattern p = Pattern.compile(REDIRECTION_CNT_TAG + "(=|%3D)[0-9]");
      Matcher m = p.matcher(urlStr);
      if (m.find()) {
        String urlWithCnt = m.group();
        int cnt = Integer.valueOf(urlWithCnt.substring(urlWithCnt.length()-1)) + 1;
        urlWithCnt = REDIRECTION_CNT_TAG + "=" + cnt;
        urlStr = urlStr.replaceAll(p.pattern(), urlWithCnt);
      }
    }else{
      urlStr += "&" + REDIRECTION_CNT_TAG + "=" + 0;
    }
    return urlStr;
  }

  public String getRedirectionCount(String requestUrl){
    // append redirection count into URL
    String urlWithCnt = null;
    if(requestUrl.contains(REDIRECTION_CNT_TAG)){
      Pattern p = Pattern.compile(REDIRECTION_CNT_TAG + "(=|%3D)[0-9]");
      Matcher m = p.matcher(requestUrl);
      if (m.find()) {
        urlWithCnt = m.group();
      }
    }else{
      urlWithCnt = REDIRECTION_CNT_TAG + "=" + 0;
    }
    return urlWithCnt;
  }

  public String getChocoTag(String requestUrl){
    String chocoTag = null;
    // append snapshotId into URL
    if(requestUrl.contains(CHOCO_TAG)){
      Pattern p = Pattern.compile(CHOCO_TAG + "(=|%3D)\\d+");
      Matcher m = p.matcher(requestUrl);
      if (m.find()) {
        chocoTag = m.group();
      }
    }else{
      Long snapshotId = SnapshotId.getNext(ListenerOptions.getInstance().getDriverId(), System.currentTimeMillis()).getRepresentation();
      chocoTag = CHOCO_TAG + "=" + snapshotId;
    }
    return chocoTag;
  }

    /**
     * Filter on Response Location header
     * @param clientRequest Request
     * @param proxyResponse Response, for first redirect to Rover, original request will
     *                      be attached to Location header as part of the URL query
     * @return filter result, true for filtered response
     */
    public boolean responseShouldBeFiltered(HttpServletRequest clientRequest, HttpServletResponse proxyResponse, String requestUrl)
            throws MalformedURLException, UnsupportedEncodingException {
      // no action if no redirect
      if (proxyResponse.getStatus() != 301 || proxyResponse.getHeader(REDIRECT_HEADER) == null) {
        return false;
      }

      // assume Rover always return valid URL
      URL redirectUrl = new URL(proxyResponse.getHeader(REDIRECT_HEADER));

      // redirect to other domain
      if (!redirectUrl.getHost().startsWith("rover")) {
        return false;
      }

      boolean withQuery = (redirectUrl.getQuery() != null);
      //SET chocolate tags in location
      String chocoTag = getChocoTag(requestUrl);
      String redirectCnt = getRedirectionCount(requestUrl);
      String location = proxyResponse.getHeader(REDIRECT_HEADER);
      // append snapshotId into URL if not exist
      if (!location.contains(CHOCO_TAG)) {
        location = withQuery ? location + "&" : location + "?";
        location += chocoTag;
      }
      // append redirection count into URL if not exist
      if (location.contains(REDIRECTION_CNT_TAG)) {
        Pattern p = Pattern.compile(REDIRECTION_CNT_TAG + "(=|%3D)[0-9]");
        location = location.replaceAll(p.pattern(), redirectCnt);
      } else {
        location += "&" + redirectCnt;
      }
      proxyResponse.setHeader(REDIRECT_HEADER, location);

      // redirect to Rover case
      // no need to send message to Kafka
      // the first time redirect to Rover, append the request URL to query
      // seems Rover will not keep the REDIRECT_SPECIAL_TAG in Location header, need append again from the original request
//      String requestUrlEncoded = URLEncoder.encode(this.getRequestURL(clientRequest), StandardCharsets.UTF_8.toString());
//      if (withQuery) {
//          proxyResponse.setHeader(REDIRECT_HEADER, redirectUrl.toString() + "&" + REDIRECT_SPECIAL_TAG + requestUrlEncoded);
//      } else {
//          proxyResponse.setHeader(REDIRECT_HEADER, redirectUrl.toString() + "?" + REDIRECT_SPECIAL_TAG + requestUrlEncoded);
//      }
      return true;
    }



    private String serializeRequestHeaders(HttpServletRequest clientRequest) {
        StringBuilder requestHeaders = new StringBuilder();
        for (Enumeration<String> e = clientRequest.getHeaderNames(); e.hasMoreElements();) {
            String headerName = e.nextElement();
            requestHeaders.append("|").append(headerName).append(": ").append(clientRequest.getHeader(headerName));
        }
        if(!StringUtils.isEmpty(requestHeaders.toString())) requestHeaders.deleteCharAt(0);
        return requestHeaders.toString();
    }

    private String serializeResponseHeaders(HttpServletResponse response) {
        StringBuilder requestHeaders = new StringBuilder();
        for (String headerName: response.getHeaderNames()) {
            requestHeaders.append("|").append(headerName).append(": ").append(response.getHeader(headerName));
        }
        requestHeaders.deleteCharAt(0);
        return requestHeaders.toString();
    }

    /** returns the singleton instance */
    public static MessageObjectParser getInstance() {
        return INSTANCE;
    }

    /**
     * Initialize singleton instance
     */
    public static synchronized void init() {
        if (INSTANCE == null) {
            INSTANCE = new MessageObjectParser();
        }
    }
}