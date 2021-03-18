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
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
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
  public static final String TREATMENT_ID = "trt";
  public static final String EXPERIMENT_TYPE = "ext";
  public static final String MOB_TRK_ID = "osub";
  public static final String MESSAGE_ID = "mesgId";
  public static final String PLACEMENT_ID = "plmtId";
  public static final String PLACEMENT_POS = "plmtPos";
  public static final String RECO_ID = "recoId";
  public static final String RECO_POS = "recoPos";
  public static final String FEEDBACK = "fdbk";
  public static final String IS_UEP = "isUEP";
  // for ORS migration
  public static final String EMAIL = "EMAIL";
  public static final String MESSAGE_CENTER = "MESSAGE_CENTER";
  public static final String TIMESTAMP_CREATED = "timestamp.created";
  public static final String TIMESTAMP_UPDATED = "timestamp.updated";
  public static final String STATUS_SENT = "SENT";
  public static final String C_URL = "cUrl";
  public static final String ANNOTATION_MESSAGE_NAME = "annotation.message.name";
  public static final String ANNOTATION_CANVAS_UNIQ_ID = "annotation.canvas.uniq.id";
  private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyyMMddHHmmss");
  private final SimpleDateFormat eventDateStringFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static final String WHITELIST_PATTERN_MARKETING_EMAIL_PA = "TE1798";
  private static final String WHITELIST_PATTERN_MARKETING_EMAIL_ESPRESSO = "TE7";
  private static final String WHITELIST_EXACTMATCH_SITE_EMAIL_AXO = "11403";
  private static final String WHITELIST_EXACTMATCH_SITE_EMAIL_SS = "11021";
  private static final String MESSAGE_PA = "PA";
  private static final String MESSAGE_ESPRESSO = "ESPRESSO";
  private static final String MESSAGE_AXO = "AXO";
  private static final String MESSAGE_SS = "SAVEDSEARCH";

  private static final Logger LOGGER = LoggerFactory.getLogger(UepPayloadHelper.class);

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
  public Map<String, String> getUepPayload(String url, ActionTypeEnum actionTypeEnum, ChannelTypeEnum channelTypeEnum) {
    Map<String, String> payload = new HashMap<>();
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(url).build();
//    String bu = uriComponents.getQueryParams().getFirst(BEST_GUESS_USER);
//    if (StringUtils.isNotEmpty(bu)) {
//      Long encryptedUserId = Longs.tryParse(bu);
//      if (encryptedUserId != null) {
//        long userId = EncryptUtil.decryptUserId(encryptedUserId);
//        payload.put(MessageConstantsEnum.USER_ID.getValue(), String.valueOf(userId));
//      }
//    } else {
//      payload.put(MessageConstantsEnum.USER_ID.getValue(), "0");
//    }

    // for ORS shor-tem migration
//    // channel.name
//    if(channelTypeEnum.getValue().contains(EMAIL)) {
//      payload.put(MessageConstantsEnum.CHANNEL_NAME.getValue(), EMAIL);
//    } else if(channelTypeEnum.getValue().contains(MESSAGE_CENTER)) {
//      payload.put(MessageConstantsEnum.CHANNEL_NAME.getValue(), MESSAGE_CENTER);
//    } else {
//      payload.put(MessageConstantsEnum.CHANNEL_NAME.getValue(), channelTypeEnum.getValue());
//    }
    // annotation.message.name
    String segmentCode = uriComponents.getQueryParams().getFirst(Constants.SEGMENT_NAME);
    if (segmentCode != null) {
      if (segmentCode.contains(WHITELIST_PATTERN_MARKETING_EMAIL_PA)) {
        payload.put(ANNOTATION_MESSAGE_NAME, MESSAGE_PA);
      }
      else if (segmentCode.contains(WHITELIST_PATTERN_MARKETING_EMAIL_ESPRESSO)) {
        payload.put(ANNOTATION_MESSAGE_NAME, MESSAGE_ESPRESSO);
      }
      else if (segmentCode.equalsIgnoreCase(WHITELIST_EXACTMATCH_SITE_EMAIL_AXO)) {
        payload.put(ANNOTATION_MESSAGE_NAME, MESSAGE_AXO);
      }
      else if (segmentCode.equalsIgnoreCase(WHITELIST_EXACTMATCH_SITE_EMAIL_SS)) {
        payload.put(ANNOTATION_MESSAGE_NAME, MESSAGE_SS);
      }
    }
    // rundate
    String actualRunDateString = "";
    String runDate = "";
    try {
      actualRunDateString = uriComponents.getQueryParams().getFirst("crd");
      if(StringUtils.isNotEmpty(actualRunDateString)) {
        Date tempRunDate = dateFormatter.parse(actualRunDateString);
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
          getOrDefault(uriComponents.getQueryParams().getFirst(Constants.EMAIL_UNIQUE_ID)));
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

//    // cUrl
//    try {
//      payload.put(C_URL, URLEncoder.encode(url, "UTF-8"));
//    } catch (UnsupportedEncodingException e) {
//      LOGGER.warn("Unsupported encoding: " + url, e);
//    }
    // tag_item, no need
    // tag_intrId, no need
    // tag_intrUnsub, no need
    // cnv.id no need in open/click
    // tracking id
    String trackingId = getOrDefault(uriComponents.getQueryParams().getFirst(TRACKING_ID));
//    payload.put(MessageConstantsEnum.TRACKING_ID.getValue(), trackingId);
    // isUep
    if(StringUtils.isNotEmpty(trackingId)) {
      payload.put(IS_UEP, String.valueOf(true));
    } else {
      payload.put(IS_UEP, String.valueOf(false));
    }

    // experiment ids
    payload.put("exe", getOrDefault(uriComponents.getQueryParams().getFirst(EXPERIMENT_ID)));
    payload.put("ext", getOrDefault(uriComponents.getQueryParams().getFirst(TREATMENT_ID)));
    payload.put("trt", getOrDefault(uriComponents.getQueryParams().getFirst(EXPERIMENT_TYPE)));

    // message list
    Message message = new Message();
    message.mobTrkId = uriComponents.getQueryParams().getFirst(MOB_TRK_ID);
    if (actionTypeEnum.equals(ActionTypeEnum.CLICK)) {
      // message level
      message.mesgId = uriComponents.getQueryParams().getFirst(MESSAGE_ID);
      message.plmtId = uriComponents.getQueryParams().getFirst(PLACEMENT_ID);
      message.plmtPos = uriComponents.getQueryParams().getFirst(PLACEMENT_POS);
      // feedback click
      String feedback = uriComponents.getQueryParams().getFirst(FEEDBACK);
      if(StringUtils.isNotEmpty(feedback)) {
        message.mesgFdbk = feedback;
      }

      // recommendation level
      Recommendation recommendation = new Recommendation();
      recommendation.recoId = uriComponents.getQueryParams().getFirst(RECO_ID);
      recommendation.recoPos = uriComponents.getQueryParams().getFirst(RECO_POS);
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
}
