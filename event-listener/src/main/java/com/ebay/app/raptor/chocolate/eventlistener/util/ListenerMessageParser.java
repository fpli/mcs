package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.HttpMethod;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.ShortSnapshotId;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.kernel.util.StringUtils;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
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
   * @param clientRequest   raw client request
   * @param startTime       as start time of the request
   * @param campaignId      campaign id
   * @param channelType     channel type
   * @param action          action
   * @param endUserContext  X-C-EBAY-ENDUSERCTX
   * @param uri             landing page url
   * @param referer         ad referer
   * @param snid            snid
   * @return                ListenerMessage object
   */
  public ListenerMessage parse(
    final HttpServletRequest clientRequest, Long startTime, Long campaignId,
    final ChannelType channelType, final ChannelActionEnum action, String userId, IEndUserContext endUserContext, String uri, String referer, String snid) {

    ListenerMessage record = new ListenerMessage();

    // user id
    record.setUserId(Long.valueOf(userId));

    // guid, cguid from tracking header
    String trackingHeader = clientRequest.getHeader("X-EBAY-C-TRACKING");
    if (!org.springframework.util.StringUtils.isEmpty(trackingHeader)) {
      for (String seg : trackingHeader.split(",")
        ) {
        String[] keyValue = seg.split("=");
        if (keyValue.length == 2) {
          if (keyValue[0].equalsIgnoreCase("guid")) {
            record.setGuid(keyValue[1]);
          }
          if (keyValue[0].equalsIgnoreCase("cguid")) {
            record.setCguid(keyValue[1]);
          }
        }
      }
    }

    // remote ip
    record.setRemoteIp(endUserContext.getIPAddress());

    // language code
    record.setLangCd("");

    // user agent
    record.setUserAgent(endUserContext.getUserAgent());

    // geography identifier
    record.setGeoId(-1L);

    // udid
    record.setUdid(endUserContext.getDeviceId());

    // referer
    record.setReferer(referer);

    // site id
    record.setSiteId(-1L);

    // landing page url
    record.setLandingPageUrl(uri);

    // source and destination rotation id
    record.setSrcRotationId(-1L);
    record.setDstRotationId(-1L);

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
    ShortSnapshotId shortSnapshotId = new ShortSnapshotId(record.getSnapshotId().longValue());
    record.setShortSnapshotId(shortSnapshotId.getRepresentation());

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
  private String serializeRequestHeaders(HttpServletRequest clientRequest) {

    StringBuilder headersBuilder = new StringBuilder();

    Map<String, String> headers = new HashMap<>();
    for (Enumeration<String> e = clientRequest.getHeaderNames(); e.hasMoreElements(); ) {
      String headerName = e.nextElement();
      // skip auth header
      if (headerName.equalsIgnoreCase("Authorization")) {
        continue;
      }
      headers.put(headerName, clientRequest.getHeader(headerName));
      headersBuilder.append("|").append(headerName).append(": ").append(clientRequest.getHeader(headerName));
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