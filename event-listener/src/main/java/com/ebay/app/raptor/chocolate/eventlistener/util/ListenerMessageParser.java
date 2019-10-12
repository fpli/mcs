package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.HttpMethod;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.ShortSnapshotId;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.kernel.util.StringUtils;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.raptor.geo.context.UserPrefsCtx;
import com.ebay.raptor.kernel.util.RaptorConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
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
  private static final Logger logger = LoggerFactory.getLogger(ListenerMessageParser.class);
  private static ListenerMessageParser INSTANCE;
  private static final long DEFAULT_PUBLISHER_ID = -1L;

  /* singleton class */
  private ListenerMessageParser() {
  }

  /**
   * Convert a HTTP request to a listener message for Kafka.
   *
   * @param clientRequest  raw client request
   * @param requestContext raptor request context
   * @param startTime      as start time of the request
   * @param campaignId     campaign id
   * @param channelType    channel type
   * @param action         action
   * @param endUserContext X-C-EBAY-ENDUSERCTX
   * @param uri            landing page url
   * @param referer        ad referer
   * @param rotationId     rotation id
   * @param snid           snid
   * @return ListenerMessage object
   */
  public ListenerMessage parse(
    final HttpServletRequest clientRequest, final ContainerRequestContext requestContext, Long startTime, Long campaignId,
    final ChannelType channelType, final ChannelActionEnum action, String userId, IEndUserContext endUserContext,
    String uri, String referer, long rotationId, String snid) {

    // set default values to prevent unable to serialize message exception
    ListenerMessage record = new ListenerMessage(0L, 0L, 0L, 0L, "", "", "", "", "", 0L, "", "", -1L, -1L, 0L, "",
      0L, 0L, "", "", "", ChannelAction.CLICK, ChannelType.DEFAULT, HttpMethod.GET, "", false);

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

    // user agent
    record.setUserAgent(endUserContext.getUserAgent());

    // parse user prefs
    try {

      // get geo info
      UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);

      // language code
      record.setLangCd(userPrefsCtx.getLangLocale().toLanguageTag());

      // geography identifier
      record.setGeoId((long) userPrefsCtx.getGeoContext().getCountryId());

      // site id
      record.setSiteId((long)userPrefsCtx.getGeoContext().getSiteId());

    } catch (Exception e) {
      logger.error("Parse geo info error");
    }

    // udid
    if (endUserContext.getDeviceId() != null) {
      record.setUdid(endUserContext.getDeviceId());
    } else {
      record.setUdid("");
    }

    // referer
    record.setReferer(referer);

    // landing page url
    record.setLandingPageUrl(uri);

    // source and destination rotation id
    record.setSrcRotationId(rotationId);
    record.setDstRotationId(rotationId);

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
    Long snapshotId = SnapshotId.getNext(ApplicationOptions.getInstance().getDriverId()).getRepresentation();
    record.setSnapshotId(snapshotId);
    ShortSnapshotId shortSnapshotId = new ShortSnapshotId(record.getSnapshotId());
    record.setShortSnapshotId(shortSnapshotId.getRepresentation());

    record.setCampaignId(campaignId);
    record.setPublisherId(DEFAULT_PUBLISHER_ID);
    record.setSnid((snid != null) ? snid : "");
    record.setIsTracked(false);

    return record;
  }

  /**
   * Serialize the headers, except Authorization
   *
   * @param clientRequest input request
   * @return headers string
   */
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