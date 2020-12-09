/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.utp;

import com.ebay.app.raptor.chocolate.util.EncryptUtil;
import com.ebay.kernel.util.StringUtils;
import com.ebay.traffic.chocolate.utp.common.ActionTypeEnum;
import com.ebay.traffic.chocolate.utp.common.MessageConstantsEnum;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  public Map<String, String> getUepPayload(String url, ActionTypeEnum actionTypeEnum) {
    Map<String, String> payload = new HashMap<>();
    UriComponents uriComponents = UriComponentsBuilder.fromUriString(url).build();
    if (uriComponents.getQueryParams().getFirst(BEST_GUESS_USER) != null &&
        StringUtils.isNumeric(uriComponents.getQueryParams().getFirst(BEST_GUESS_USER))) {
      Long encryptedUserId = Longs.tryParse(uriComponents.getQueryParams().getFirst(BEST_GUESS_USER));
      if (encryptedUserId != null) {
        long userId = EncryptUtil.decryptUserId(encryptedUserId);
        payload.put(MessageConstantsEnum.USER_ID.getValue(), String.valueOf(userId));
      }
    } else {
      payload.put(MessageConstantsEnum.USER_ID.getValue(), "0");
    }

    payload.put(MessageConstantsEnum.TRACKING_ID.getValue(),
        getOrDefault(uriComponents.getQueryParams().getFirst(TRACKING_ID)));
    payload.put("exe", getOrDefault(uriComponents.getQueryParams().getFirst(EXPERIMENT_ID)));
    payload.put("ext", getOrDefault(uriComponents.getQueryParams().getFirst(TREATMENT_ID)));
    payload.put("trt", getOrDefault(uriComponents.getQueryParams().getFirst(EXPERIMENT_TYPE)));

    MesgList mesgList = new MesgList();
    mesgList.mobTrkId = uriComponents.getQueryParams().getFirst(MOB_TRK_ID);
    if (actionTypeEnum.equals(ActionTypeEnum.CLICK)) {
      // message level
      mesgList.mesgId = uriComponents.getQueryParams().getFirst(MESSAGE_ID);
      mesgList.plmtId = uriComponents.getQueryParams().getFirst(PLACEMENT_ID);
      mesgList.plmtPos = uriComponents.getQueryParams().getFirst(PLACEMENT_POS);

      // recommendation level
      RecoList recoList = new RecoList();
      recoList.recoId = uriComponents.getQueryParams().getFirst(RECO_ID);
      recoList.recoPos = uriComponents.getQueryParams().getFirst(RECO_POS);
      List<RecoList> recoLists = new ArrayList<>();
      recoLists.add(recoList);
      mesgList.recoList = recoLists;
    }

    MesgRoot root = new MesgRoot();
    List<MesgList> mesgLists = new ArrayList<>();
    mesgLists.add(mesgList);
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
