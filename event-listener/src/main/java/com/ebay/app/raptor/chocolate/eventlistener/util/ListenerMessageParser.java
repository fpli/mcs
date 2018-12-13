package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.HttpMethod;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.kernel.util.StringUtils;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for parsing the POJOs
 *
 * @author xiangli4
 */
public class ListenerMessageParser {

  /**
   * Logging instance
   */
  private static final Logger logger = Logger.getLogger(ListenerMessageParser.class);
  private static ListenerMessageParser INSTANCE;
  private static final long DEFAULT_PUBLISHER_ID = -1L;

  /* singleton class */
  private ListenerMessageParser() {
  }

  /**
   * Convert a HTTP request to a listener message for Kafka.
   *
   * @param clientRequest to use in parsing uri and timestamp
   * @param startTime     as start time of the request
   * @return ListenerMessage  as the parse result.
   */
  public ListenerMessage parse(
    final HttpServletRequest clientRequest, Long startTime, Long campaignId,
    final ChannelType channelType, final ChannelActionEnum action, String uri, String snid, Map<String, String>
      addHeaders, Map<String, String> responseHeaders) {

    ListenerMessage record = new ListenerMessage();

    record.setUri(uri);
    // Set the channel type + HTTP headers + channel action
    record.setChannelType(channelType);
    record.setHttpMethod(HttpMethod.GET);
    record.setChannelAction(action.getAvro());
    // Format record
    record.setRequestHeaders(serializeRequestHeaders(clientRequest, addHeaders));
    record.setResponseHeaders(serializeResponseHeaders(responseHeaders));
    record.setTimestamp(startTime);

    // Get snapshotId from request
    Long snapshotId = SnapshotId.getNext(ApplicationOptions.getInstance().getDriverId(), startTime).getRepresentation();
    record.setSnapshotId(snapshotId);

    record.setCampaignId(campaignId);
    record.setPublisherId(DEFAULT_PUBLISHER_ID);
    record.setSnid((snid != null) ? snid : "");
    record.setIsTracked(false);

    return record;
  }

  /**
   * Serialize the headers
   *
   * @param clientRequest input request
   * @return headers string
   */
  // Or we change the result schema. Trade off needs to be considered here.
  private String serializeRequestHeaders(HttpServletRequest clientRequest, Map<String, String> addHeaders) {

    StringBuilder headersBuilder = new StringBuilder();

    Map<String, String> headers = new HashMap<>();
    for (Enumeration<String> e = clientRequest.getHeaderNames(); e.hasMoreElements(); ) {
      String headerName = e.nextElement();
      // skip auth header
      if(headerName.equalsIgnoreCase("Authorization")) {
        continue;
      }
      headers.put(headerName, clientRequest.getHeader(headerName));
      headersBuilder.append("|").append(headerName).append(": ").append(clientRequest.getHeader(headerName));
    }

    /** Add compatible headers. it will overwrite User-Agent, Referrer, X-eBay-Client-IP if have
     *  User-Agent is always GingerClient when calling from handler
     *  Referrer is null when calling from handler
     *  X-eBay-Client-IP is null when calling from handler
     */

    for (String headerName : addHeaders.keySet()
      ) {
      headersBuilder.append("|").append(headerName).append(": ").append(addHeaders.get(headerName));
    }

    if (!StringUtils.isEmpty(headersBuilder.toString())) headersBuilder.deleteCharAt(0);

    return headersBuilder.toString();
  }

  /**
   * Serialize the headers
   *
   * @param responseHeaders compatible response headers
   * @return headers string
   */
  // Or we change the result schema. Trade off needs to be considered here.
  private String serializeResponseHeaders(Map<String, String> responseHeaders) {

    StringBuilder headersBuilder = new StringBuilder();

    /* Add compatible headers. Set-Cookie maybe more in future */

    for (String headerName : responseHeaders.keySet()
      ) {
      headersBuilder.append("|").append(headerName).append(": ").append(responseHeaders.get(headerName));
    }
    if (!StringUtils.isEmpty(headersBuilder.toString())) headersBuilder.deleteCharAt(0);

    return headersBuilder.toString();
  }

  /**
   * returns the singleton instance
   */
  public static ListenerMessageParser getInstance() {
    return INSTANCE;
  }

  /**
   * Initialize singleton instance
   */
  public static synchronized void init() {
    if (INSTANCE == null) {
      INSTANCE = new ListenerMessageParser();
    }
  }
}