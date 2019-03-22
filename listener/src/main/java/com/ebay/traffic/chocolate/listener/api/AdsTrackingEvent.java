package com.ebay.traffic.chocolate.listener.api;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.traffic.chocolate.listener.util.ChannelActionEnum;
import com.ebay.traffic.chocolate.listener.util.ChannelIdEnum;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.commons.lang3.Validate;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * The event format for the traffic from "www.ebayadservices.com"
 * ex: https://www.ebayadservices.com/adTracking/v1?mkevt=2&mktcid=1&mkrid=711-1245-1245-235&mksid=17382973291738213921738291&siteid=1&[other channel parameters]
 *
 * Mandatory Fields: <br/>
 * mkevt  -- event id
 * mktcid -- channel id
 * mkrid -- rotation id
 * mksid -- session id
 *
 * without the mandatory fields, request won't be tracked in tracking system
 *
 */
public class AdsTrackingEvent {

  public static final int CURRENT_VERSION = 1;
  /* hard coded 1x1 gif pixel, for impression serving */

  private int version, channelID;
  private String collectionID;
  private ChannelActionEnum action;
  private Map<String, Object> payload;
  private ChannelType channel;
  private Long campaignID;
  private Metrics metrics;

  /**
   * For Unit Testing
   * @param url
   * @param params
   * @param metrics
   * @throws NumberFormatException
   */
  public AdsTrackingEvent(URL url, Map<String, String[]> params, Metrics metrics) throws NumberFormatException {
    this.metrics = metrics;
    payload = new HashMap<>();
    parsePath(url.getPath());
    setTrackingCommonParams(params);
    validateParams(params);
  }

  public AdsTrackingEvent(URL url, Map<String, String[]> params) throws NumberFormatException {
    if (metrics == null) metrics = ESMetrics.getInstance();
    payload = new HashMap<>();
    parsePath(url.getPath());
    setTrackingCommonParams(params);
    validateParams(params);
  }

  private void validateParams(Map<String, String[]> params) throws NumberFormatException {
    if (params.isEmpty()) return;
    Object val;
    for (Map.Entry<String, String[]> entry : params.entrySet()) {
      if (entry.getKey().equals(EventConstant.ITEM) || entry.getKey().equals(EventConstant.PRODUCT)) {
        val = Long.parseLong(entry.getValue()[0]);
      } else val = entry.getValue()[0];
      payload.put(entry.getKey(), val);
    }
  }

  /**
   * Parses the various pieces of the URL path
   *
   * @param path from the URL
   */
  private void parsePath(String path) {
    String[] parts = path.split("/");
    setVersion(parts[2]); // parts[0] will be empty string because path starts with a "/"
  }

  private void setTrackingCommonParams(Map<String, String[]> params) {
    if (params.get(EventConstant.MK_EVENT) == null || params.get(EventConstant.MK_EVENT)[0] == null) {
      Validate.isTrue(false, "Unknown action encountered - null");
    } else {
      Integer type = Integer.valueOf(params.get(EventConstant.MK_EVENT)[0]);
      switch (type) {
        case 1:
          action = ChannelActionEnum.CLICK;
          break;
        case 2:
          action = ChannelActionEnum.IMPRESSION;
          break;
        case 3:
          action = ChannelActionEnum.VIMP;
          break;
        case 4:
          action = ChannelActionEnum.SERVE;
          break;
        default:
          metrics.meter("TrackingFail", 1, Field.of(EventConstant.CHANNEL_ACTION, EventConstant.UNKNOWN));
          Validate.isTrue(false, "Unknown action encountered - " + type);
      }
    }

    channelID = params.get(EventConstant.MK_CHANNEL_ID) != null && params.get(EventConstant.MK_CHANNEL_ID)[0] != null ? Integer.valueOf(params.get(EventConstant.MK_CHANNEL_ID)[0]) : 999;
    collectionID = params.get(EventConstant.MK_ROTATION_ID) != null && params.get(EventConstant.MK_ROTATION_ID)[0] != null ? params.get(EventConstant.MK_ROTATION_ID)[0] : null;
    campaignID = params.get(EventConstant.CAMPAIGN_ID) != null && params.get(EventConstant.CAMPAIGN_ID)[0] != null ? Long.valueOf(params.get(EventConstant.CAMPAIGN_ID)[0]) : -1;
    channel = getChannelType(String.valueOf(channelID));
  }

  private ChannelType getChannelType(String channel) {
    ChannelIdEnum channelId = ChannelIdEnum.parse(channel);
    return channelId.getLogicalChannel().getAvro();
  }

  /**
   * Depending on the request type, this will either redirect or send a 1x1 gif
   * Theoretically, this could be an abstract method and there could be subclasses
   * of TrackingEvent that override this. However, there are currently only two
   * possible responses, so for simplicity they are both implemented here. If we
   * ever add a third response, this should be refactored.
   *
   * @param response response to the client
   * @throws IOException in case of any errors
   */
  void respond(HttpServletResponse response) throws IOException {
    setCommonHeaders(response);
    if (ChannelActionEnum.CLICK.equals(action)) {
      redirect(response);
    } else {
      respondWithPixel(response);
    }
  }

  // TODO incorporate cguid library
  private void setCommonHeaders(HttpServletResponse response) {
    //Aidan ya goof
    //Cookie cookie = new Cookie("npii", "fixme with some guids please");
    //response.addCookie(cookie);
  }

  private void respondWithPixel(HttpServletResponse response) throws IOException {
    response.setContentType("image/gif");
    // Set no cache header to avoid cache on browser like Chrome
    response.setHeader("Pragma", "no-cache");
    response.setContentLength(EventConstant.pixel.length);
    OutputStream out = response.getOutputStream();
    out.write(EventConstant.pixel);
    out.close();
  }

  private void redirect(HttpServletResponse response) throws IOException {
    URL url = new URL((String) payload.get(EventConstant.MK_LND_PAGE));
    // Use lowercase to avoid case sensitive
    String host = url.getHost().toLowerCase();
    String destination = response.encodeRedirectURL(EventConstant.DEFAULT_DESTINATION);

    if (host.contains("ebay")) {
      for (String validHost : EventConstant.EBAY_HOSTS) {
        // Ensure the host is end with the domains in the list
        if (host.endsWith(validHost)) {
          destination = response.encodeRedirectURL(url.toString());
        }
      }
    }
    response.sendRedirect(destination);
  }

  public int getVersion() {
    return version;
  }

  /**
   * Parse, validate and set version and event type
   *
   * @param versionAndType - two character string with format <version number><event type>
   */
  private void setVersion(String versionAndType) {
    Validate.matchesPattern(versionAndType, "^\\w\\d$");
    version = Character.getNumericValue(versionAndType.charAt(1));
    Validate.isTrue(version <= CURRENT_VERSION);
  }

  public ChannelActionEnum getAction() {
    return action;
  }

  public int getChannelID() {
    return channelID;
  }

  public String getCollectionID() {
    return collectionID;
  }

  public Map<String, Object> getPayload() {
    return payload;
  }

  public ChannelType getChannel() {
    return channel;
  }

  public Long getCampaignID() {
    return campaignID;
  }
}