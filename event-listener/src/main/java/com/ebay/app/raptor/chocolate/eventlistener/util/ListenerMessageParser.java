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
    record.setHttpMethod(HttpMethod.GET);
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
    record.setIsTracked(false);

    return record;
  }

  /**
   * Serialize the headers
   * @param clientRequest input request
   * @return headers string
   */
  //TODO: We may need to map the headers to the current format of chocolate result for backward compatibility.
  // Or we change the result schema. Trade off needs to be considered here.
  private String serializeRequestHeaders(HttpServletRequest clientRequest) {
    StringBuilder requestHeaders = new StringBuilder();
    for (Enumeration<String> e = clientRequest.getHeaderNames(); e.hasMoreElements(); ) {
      String headerName = e.nextElement();
      requestHeaders.append("|").append(headerName).append(": ").append(clientRequest.getHeader(headerName));
    }
    if (!StringUtils.isEmpty(requestHeaders.toString())) requestHeaders.deleteCharAt(0);
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