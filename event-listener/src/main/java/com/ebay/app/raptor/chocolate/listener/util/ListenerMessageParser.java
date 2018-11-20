package com.ebay.app.raptor.chocolate.listener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.app.raptor.chocolate.listener.ApplicationOptions;
import com.ebay.kernel.util.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Enumeration;

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
    final ChannelType channelType, final ChannelActionEnum action, String uri, String snid) {

    ListenerMessage record = new ListenerMessage();

    record.setUri(uri);
    // Set the channel type + HTTP headers + channel action
    record.setChannelType(channelType);
    record.setHttpMethod(this.getMethod(clientRequest).getAvro());
    record.setChannelAction(action.getAvro());
    // Format record
    record.setRequestHeaders(serializeRequestHeaders(clientRequest));
    record.setResponseHeaders("");
    record.setTimestamp(startTime);

    // Get snapshotId from request
    Long snapshotId = SnapshotId.getNext(ApplicationOptions.getInstance().getDriverId(), startTime).getRepresentation();
    record.setSnapshotId(snapshotId);

    record.setCampaignId(campaignId);
    record.setPublisherId(DEFAULT_PUBLISHER_ID);
    record.setSnid((snid != null) ? snid : "");
    record.setIsTracked(false);     //TODO No messages are Durability-tracked for now

    return record;
  }

  /**
   * Get request method and validate from request
   *
   * @param clientRequest Request
   * @return HttpMethodEnum
   */
  protected HttpMethodEnum getMethod(HttpServletRequest clientRequest) {
    HttpMethodEnum httpMethod = HttpMethodEnum.parse(clientRequest.getMethod());
    Validate.notNull(httpMethod, "Could not parse HTTP method from HTTP request=" + clientRequest.getMethod());
    return httpMethod;
  }

  /**
   * Return if it is core site url
   *
   * @param clientRequest http servlet request
   * @return is core site
   */
  public boolean isCoreSite(HttpServletRequest clientRequest) {
    String serverName = clientRequest.getServerName();
    if (ListenerOptions.getInstance().getRoverCoreSites().contains(serverName)) {
      return true;
    }
    return false;
  }

  private String serializeRequestHeaders(HttpServletRequest clientRequest) {
    StringBuilder requestHeaders = new StringBuilder();
    for (Enumeration<String> e = clientRequest.getHeaderNames(); e.hasMoreElements(); ) {
      String headerName = e.nextElement();
      requestHeaders.append("|").append(headerName).append(": ").append(clientRequest.getHeader(headerName));
    }
    if (!StringUtils.isEmpty(requestHeaders.toString())) requestHeaders.deleteCharAt(0);
    return requestHeaders.toString();
  }

  private String serializeResponseHeaders(HttpServletResponse response) {
    StringBuilder requestHeaders = new StringBuilder();
    for (String headerName : response.getHeaderNames()) {
      requestHeaders.append("|").append(headerName).append(": ").append(response.getHeader(headerName));
    }
    requestHeaders.deleteCharAt(0);
    return requestHeaders.toString();
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