/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.utp;

import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.util.EncryptUtil;
import com.ebay.traffic.chocolate.utp.common.ActionTypeEnum;
import com.ebay.traffic.chocolate.utp.common.ChannelTypeEnum;
import com.ebay.traffic.chocolate.utp.common.MessageConstantsEnum;
import com.ebay.traffic.chocolate.utp.common.model.Message;
import com.ebay.traffic.chocolate.utp.common.model.MessageRoot;
import com.ebay.traffic.chocolate.utp.common.model.Recommendation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Longs;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.*;

/**
 * @author Xiang Li
 * @since 2020/12/04
 * For UTP payload construction for open and click events
 */
public class UepPayloadHelper {
  public static final String BEST_GUESS_USER = "bu";
  public static final String TRACKING_ID = "trkId";
  public static final String EXPERIMENT_ID = "exe";
  public static final String EXPERIMENT_TYPE = "ext";
  public static final String MOB_TRK_ID = "osub";
  public static final String MESSAGE_ID = "mesgId";
  public static final String PLACEMENT_ID = "plmtId";
  public static final String PLACEMENT_POS = "plmtPos";
  public static final String RECO_ID = "recoId";
  public static final String RECO_POS = "recoPos";
  public static final String FEEDBACK = "fdbk";
  public static final String IS_UEP = "isUEP";
  public static final String MXT = "mxt";

  // for ORS migration
  public static final String TIMESTAMP_CREATED = "timestamp.created";
  public static final String TIMESTAMP_UPDATED = "timestamp.updated";
  public static final String STATUS_SENT = "SENT";
  public static final String ANNOTATION_MESSAGE_NAME = "annotation.message.name";
  public static final String ANNOTATION_CANVAS_UNIQ_ID = "annotation.canvas.uniq.id";
  public static final ZoneOffset DEFAULT_ZONE_OFFSET = ZoneOffset.ofHours(-7);

  // UEP canvas id
  private static final String ANNOTATION_CNV_ID = "annotation.cnv.id";
  private static final String CANVAS_ID = "cnvId";

  private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(DEFAULT_ZONE_OFFSET);
  private final DateTimeFormatter eventDateStringFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(DEFAULT_ZONE_OFFSET);

  // for message name
  private static final String WHITELIST_PATTERN_MARKETING_EMAIL_PA = "TE1798";
  private static final String WHITELIST_PATTERN_MARKETING_EMAIL_ESPRESSO = "TE7";
  private static final String WHITELIST_EXACTMATCH_SITE_EMAIL_AXO = "11403";
  private static final String WHITELIST_EXACTMATCH_SITE_EMAIL_SS = "11021";
  private static final String WHITELIST_EXACTMATCH_SITE_EMAIL_SIO = "11304";
  private static final String WHITELIST_EXACTMATCH_SITE_EMAIL_BOSD = "11967";
  private static final String WHITELIST_EXACTMATCH_SITE_EMAIL_SOSD = "12995";
  private static final String MESSAGE_PA = "PA";
  private static final String MESSAGE_ESPRESSO = "ESPRESSO";
  private static final String MESSAGE_AXO = "AXO";
  private static final String MESSAGE_SS = "SAVEDSEARCH";
  private static final String MESSAGE_SIO = "SellerInitiatedOffer";
  private static final String MESSAGE_BOSD = "BuyerOrderShipmentDelay";
  private static final String MESSAGE_SOSD = "SellerOrderShipmentDelay";

  private static final Logger LOGGER = LoggerFactory.getLogger(UepPayloadHelper.class);

  private UepPayloadHelper() {
  }

  public static UepPayloadHelper getInstance() {
    return SingletonHolder.instance;
  }

  private static class SingletonHolder {
    private static final UepPayloadHelper instance = new UepPayloadHelper();
  }

  private String getOrDefault(String input) {
    if (input == null) {
      return "";
    } else {
      return input;
    }
  }

  /**
   * Generate UEP payload
   *
   * @param url            the tracking url
   * @param actionTypeEnum action type
   * @return the payload
   */
  public Map<String, String> getUepPayload(String url, Long userId, ActionTypeEnum actionTypeEnum,
                                           ChannelTypeEnum channelTypeEnum) {
    Map<String, String> payload = new HashMap<>();
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(url).build();
    MultiValueMap<String, String> parameters = uriComponents.getQueryParams();

    // userid
    // Use real click user id first.
    // If it's not Pulsar event, use email best guess user id instead.
    if (userId != null) {
      payload.put(MessageConstantsEnum.USER_ID.getValue(), String.valueOf(userId));
    } else {
      String bu = parameters.getFirst(BEST_GUESS_USER);
      if (StringUtils.isNotEmpty(bu)) {
        Long encryptedUserId = Longs.tryParse(bu);
        if (encryptedUserId != null) {
          long emailUserId = EncryptUtil.decryptUserId(encryptedUserId);
          payload.put(MessageConstantsEnum.USER_ID.getValue(), String.valueOf(emailUserId));
        }
      } else {
        payload.put(MessageConstantsEnum.USER_ID.getValue(), "0");
      }
    }

    // annotation.message.name
    String segmentCode = parseTagFromParams(parameters, Constants.SEGMENT_NAME);
    if (segmentCode != null) {
      if (segmentCode.contains(WHITELIST_PATTERN_MARKETING_EMAIL_PA)) {
        payload.put(ANNOTATION_MESSAGE_NAME, MESSAGE_PA);
      } else if (segmentCode.contains(WHITELIST_PATTERN_MARKETING_EMAIL_ESPRESSO)) {
        payload.put(ANNOTATION_MESSAGE_NAME, MESSAGE_ESPRESSO);
      } else if (segmentCode.equalsIgnoreCase(WHITELIST_EXACTMATCH_SITE_EMAIL_AXO)) {
        payload.put(ANNOTATION_MESSAGE_NAME, MESSAGE_AXO);
      } else if (segmentCode.equalsIgnoreCase(WHITELIST_EXACTMATCH_SITE_EMAIL_SS)) {
        payload.put(ANNOTATION_MESSAGE_NAME, MESSAGE_SS);
      } else if (segmentCode.equalsIgnoreCase(WHITELIST_EXACTMATCH_SITE_EMAIL_SIO)) {
        payload.put(ANNOTATION_MESSAGE_NAME, MESSAGE_SIO);
      }else if (segmentCode.equalsIgnoreCase(WHITELIST_EXACTMATCH_SITE_EMAIL_BOSD)) {
        payload.put(ANNOTATION_MESSAGE_NAME, MESSAGE_BOSD);
      }else if (segmentCode.equalsIgnoreCase(WHITELIST_EXACTMATCH_SITE_EMAIL_SOSD)) {
        payload.put(ANNOTATION_MESSAGE_NAME, MESSAGE_SOSD);
      }
    }
    // rundate
    String actualRunDateString = "";
    String runDate = "";
    try {
      actualRunDateString = parameters.getFirst("crd");
      if(StringUtils.isNotEmpty(actualRunDateString)) {
        TemporalAccessor tempRunDate = dateFormatter.parse(actualRunDateString);
        runDate = eventDateStringFormatter.format(tempRunDate);
        payload.put(MessageConstantsEnum.RUN_DATE.getValue(), runDate);
      }
    } catch (Exception e) {
      LOGGER.warn("Rundate is malformed" + actualRunDateString, e);
    }
    // annotation.canvas.uniq.id
    if(channelTypeEnum.equals(ChannelTypeEnum.SITE_EMAIL)
        || channelTypeEnum.equals(ChannelTypeEnum.SITE_MESSAGE_CENTER)) {
      payload.put(ANNOTATION_CANVAS_UNIQ_ID,
          getOrDefault(parameters.getFirst(Constants.EMAIL_UNIQUE_ID)));
    } else if(channelTypeEnum.equals(ChannelTypeEnum.MRKT_EMAIL)
        || channelTypeEnum.equals(ChannelTypeEnum.MRKT_MESSAGE_CENTER)) {
      payload.put(ANNOTATION_CANVAS_UNIQ_ID, runDate);
    }
//    // interaction.type
//    payload.put(MessageConstantsEnum.INTERACTION_TYPE.getValue(), actionTypeEnum.getValue());
    // timestamp.created, use current ts
    long ts = System.currentTimeMillis();
    payload.put(TIMESTAMP_CREATED, String.valueOf(ts));
    // timestamp.updated, use current ts
    payload.put(TIMESTAMP_UPDATED, String.valueOf(ts));
    // status, all SENT for click/open
    payload.put(MessageConstantsEnum.STATUS.getValue(), STATUS_SENT);

    // tracking id
    String trackingId = getOrDefault(parseFromTwoParams(parameters, TRACKING_ID, TRACKING_ID.toLowerCase()));
    payload.put(MessageConstantsEnum.TRACKING_ID.getValue(), trackingId);
    // annotation.cnv.id
    String cnvId = parseFromTwoParams(parameters, CANVAS_ID, CANVAS_ID.toLowerCase());
    if (!StringUtils.isEmpty(cnvId)) {
      payload.put(ANNOTATION_CNV_ID, cnvId);
    }
    // isUep
    if((ChannelTypeEnum.SITE_EMAIL.equals(channelTypeEnum) ||
        ChannelTypeEnum.MRKT_EMAIL.equals(channelTypeEnum) ||
        ChannelTypeEnum.SITE_MESSAGE_CENTER.equals(channelTypeEnum) ||
        ChannelTypeEnum.MRKT_MESSAGE_CENTER.equals(channelTypeEnum)) &&
        StringUtils.isNotEmpty(trackingId)) {
      payload.put(IS_UEP, String.valueOf(true));
    } else {
      payload.put(IS_UEP, String.valueOf(false));
    }

    // experiment ids
    payload.put("exe", getOrDefault(parameters.getFirst(EXPERIMENT_ID)));
    payload.put("ext", getOrDefault(parameters.getFirst(EXPERIMENT_TYPE)));

    // message list
    Message message = new Message();
    message.mobTrkId = parameters.getFirst(MOB_TRK_ID);

    if (actionTypeEnum.equals(ActionTypeEnum.CLICK)) {
      // message level
      message.mesgId = parseFromTwoParams(parameters, MESSAGE_ID, MESSAGE_ID.toLowerCase());
      message.plmtId = parseFromTwoParams(parameters, PLACEMENT_ID, PLACEMENT_ID.toLowerCase());
      message.plmtPos = parseFromTwoParams(parameters, PLACEMENT_POS, PLACEMENT_POS.toLowerCase());
      // feedback click
      String feedback = parameters.getFirst(FEEDBACK);
      if(StringUtils.isNotEmpty(feedback)) {
        message.mesgFdbk = feedback;
      }

      // treated ep
      message.xt = parameters.getFirst(MXT);

      // recommendation level
      Recommendation recommendation = new Recommendation();
      recommendation.recoId = parseFromTwoParams(parameters, RECO_ID, RECO_ID.toLowerCase());
      recommendation.recoPos = parseFromTwoParams(parameters, RECO_POS, RECO_POS.toLowerCase());
      // only include reco.list when valid
      if(StringUtils.isNotEmpty(recommendation.recoId) && StringUtils.isNotEmpty(recommendation.recoPos)) {
        List<Recommendation> recoLists = new ArrayList<>();
        recoLists.add(recommendation);
        message.recoList = recoLists;
      }
    }

    MessageRoot root = new MessageRoot();
    List<Message> mesgLists = new ArrayList<>();
    mesgLists.add(message);
    root.mesgList = mesgLists;
    String msgListString = "[]";
    try {
      msgListString = new ObjectMapper().writeValueAsString(mesgLists);
    } catch (JsonProcessingException ex) {
      LOGGER.error("Error parsing mesgLists into json string: " + mesgLists.toString());
      LOGGER.error(ex.getMessage());
    }
    payload.put("annotation.mesg.list", msgListString);
    return payload;
  }

  /**
   * Parse tag from url query string
   */
  public static String parseTagFromParams(MultiValueMap<String, String> parameters, String param) {
    if (parameters.containsKey(param) && parameters.getFirst(param) != null) {
      return parameters.getFirst(param);
    }

    return "";
  }

  /**
   * Parse tag from two parameters
   */
  private String parseFromTwoParams(MultiValueMap<String, String> parameters, String param,
                                          String alternativeParam) {
    if (parameters.containsKey(param) && parameters.getFirst(param) != null) {
      return parameters.getFirst(param);
    } else if (parameters.containsKey(alternativeParam) && parameters.getFirst(alternativeParam) != null) {
      return parameters.getFirst(alternativeParam);
    }

    return null;
  }
}
