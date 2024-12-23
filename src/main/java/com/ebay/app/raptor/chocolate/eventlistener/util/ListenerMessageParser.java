package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.HttpMethod;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.ShortSnapshotId;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Errors;
import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.app.raptor.chocolate.util.MonitorUtil;
import com.ebay.kernel.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;

import java.net.URLDecoder;
import java.util.Map;

import static com.ebay.app.raptor.chocolate.constant.Constants.TRACKING_HEADER;
import static org.apache.commons.compress.utils.CharsetNames.UTF_8;

/**
 * Utility class for parsing the POJOs
 *
 * @author xiangli4
 */
public class ListenerMessageParser {

  /**
   * Logging instance
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(ListenerMessageParser.class);
  private static ListenerMessageParser INSTANCE;
  private static final long DEFAULT_PUBLISHER_ID = -1L;

  /* singleton class */
  private ListenerMessageParser() {
  }

  private long parseRotationId(MultiValueMap<String, String> parameters) {
    long rotationId = -1L;
    if (parameters.containsKey(Constants.MKRID) && parameters.get(Constants.MKRID).get(0) != null) {
      try {
        String rawRotationId = parameters.get(Constants.MKRID).get(0);
        // decode rotationId if rotation is encoded
        // add decodeCnt to avoid looping infinitely
        int decodeCnt = 0;
        while (rawRotationId.contains("%") && decodeCnt < 5) {
          rawRotationId = URLDecoder.decode(rawRotationId, UTF_8);
          decodeCnt = decodeCnt + 1;
        }
        rotationId = Long.parseLong(rawRotationId.replaceAll("-", ""));
      } catch (Exception e) {
        LOGGER.warn(Errors.ERROR_INVALID_MKRID);
        MonitorUtil.info("InvalidMkrid");
      }
    } else {
      LOGGER.warn(Errors.ERROR_NO_MKRID);
      MonitorUtil.info("NoMkrid");
    }

    return rotationId;
  }

  private String parseSessionId(MultiValueMap<String, String> parameters) {
    String sessionId = "";
    if (parameters.containsKey(Constants.MKSID) && parameters.get(Constants.MKSID).get(0) != null) {
      try {
        sessionId = parameters.get(Constants.MKSID).get(0);
      } catch (Exception e) {
        LOGGER.warn(Errors.ERROR_INVALID_MKSID);
        MonitorUtil.info("InvalidMksid");
      }
    } else {
      LOGGER.warn(Errors.ERROR_NO_MKSID);
      MonitorUtil.info("NoMksid");
    }

    return sessionId;
  }

  public ListenerMessage parse(BaseEvent baseEvent) {
    // set default values to prevent unable to serialize message exception
    ListenerMessage record = new ListenerMessage(0L, 0L, 0L, 0L, "",
        "", "", "", "", 0L, "", "", -1L, -1L,
        0L, "", 0L, 0L, "", "", "",
        ChannelAction.CLICK, ChannelType.DEFAULT, HttpMethod.GET, "", false);

    // user id
    String userId = baseEvent.getUid();
    if (!StringUtils.isEmpty(userId)) {
      record.setUserId(Long.valueOf(userId));
    }

    String trackingHeader = baseEvent.getRequestHeaders().get(TRACKING_HEADER);
    // guid, cguid from tracking header
    if (!StringUtils.isEmpty(trackingHeader)) {
      String guid = HttpRequestUtil.getHeaderValue(trackingHeader, Constants.GUID);
      if (!StringUtils.isEmpty(guid)) {
        record.setGuid(guid);
      }
      String cguid = HttpRequestUtil.getHeaderValue(trackingHeader, Constants.CGUID);
      if (!StringUtils.isEmpty(cguid)) {
        record.setCguid(cguid);
      }
    }
    // Overwrite cguid using guid for ePN channel in mcs to avoid the impact on capping rules related to cguid  XC-2125
    if (baseEvent.getChannelType() == ChannelIdEnum.EPN && StringUtils.isEmpty(record.getCguid())) {
      record.setCguid(record.getGuid());
    }
    // remote ip
    record.setRemoteIp(baseEvent.getRemoteIp());

    // user agent
    record.setUserAgent(baseEvent.getEndUserContext().getUserAgent());

    // parse user prefs
    try {

      // language code
      record.setLangCd(baseEvent.getUserPrefsCtx().getLangLocale().toLanguageTag());

      // geography identifier
      record.setGeoId((long) baseEvent.getUserPrefsCtx().getGeoContext().getCountryId());

      // site id
      record.setSiteId((long) baseEvent.getUserPrefsCtx().getGeoContext().getSiteId());

    } catch (Exception e) {
      LOGGER.error("Parse geo info error");
    }

    // udid
    if (baseEvent.getEndUserContext().getDeviceId() != null) {
      record.setUdid(baseEvent.getEndUserContext().getDeviceId());
    } else {
      record.setUdid("");
    }

    // referer
    record.setReferer(baseEvent.getReferer());

    // landing page url
    record.setLandingPageUrl(baseEvent.getUrl());

    // source and destination rotation id
    long rotationId = parseRotationId(baseEvent.getUrlParameters());
    record.setSrcRotationId(rotationId);
    record.setDstRotationId(rotationId);

    record.setUri(baseEvent.getUrl());
    // Set the channel type + HTTP headers + channel action
    record.setChannelType(baseEvent.getChannelType().getLogicalChannel().getAvro());
    record.setHttpMethod(HttpMethod.GET);
    record.setChannelAction(baseEvent.getActionType().getAvro());
    // Format record
    record.setRequestHeaders(serializeRequestHeaders(baseEvent.getRequestHeaders()));
    record.setResponseHeaders("");
    record.setTimestamp(baseEvent.getTimestamp());

    // Get snapshotId from request
    Long snapshotId = SnapshotId.getNext(ApplicationOptions.getInstance().getDriverId()).getRepresentation();
    record.setSnapshotId(snapshotId);
    ShortSnapshotId shortSnapshotId = new ShortSnapshotId(record.getSnapshotId());
    record.setShortSnapshotId(shortSnapshotId.getRepresentation());

    long campaignId = -1L;
    try {
      campaignId = Long.parseLong(baseEvent.getUrlParameters().get(Constants.CAMPID).get(0));
    } catch (Exception e) {
      LOGGER.debug("No campaign id");
    }
    record.setCampaignId(campaignId);
    record.setPublisherId(DEFAULT_PUBLISHER_ID);

    // parse session id for EPN channel
    String snid = "";
    if (baseEvent.getChannelType() == ChannelIdEnum.EPN) {
      snid = parseSessionId(baseEvent.getUrlParameters());
    }
    record.setSnid((snid != null) ? snid : "");
    record.setIsTracked(false);

    return record;
  }


  /**
   * Serialize the headers, except Authorization
   *
   * @param headers request headerss
   * @return headers string
   */
  private String serializeRequestHeaders(Map<String, String> headers) {

    StringBuilder headersBuilder = new StringBuilder();

    if(!headers.isEmpty()) {

      for (Map.Entry<String, String> entry : headers.entrySet()) {
        String headerName = entry.getKey();
        // skip auth header
        if (headerName.equalsIgnoreCase("Authorization")) {
          continue;
        }
        String headerValue = entry.getValue();
        headers.put(headerName, headerValue);
        headersBuilder.append("|").append(headerName).append(": ").append(headerValue);
      }
    }

    if (!StringUtils.isEmpty(headersBuilder.toString())) {
      headersBuilder.deleteCharAt(0);
    }

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
