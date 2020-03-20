package com.ebay.app.raptor.chocolate.adservice.constant;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Email has 4 partenrs, internal for site email, others for marketing email
 * Emarsys is deprecated but still has traffic, we will process them as common events
 * Created by jialili1 on 3/20/20
 */
public enum EmailPartnerIdEnum {
  INTERNAL("0", "internal"),
  KANA("2", "kana"),
  EMARSYS("4", "emarsys"),
  YESMAIL("12", "yesmail"),
  ADOBE("14", "adobe");

  private final String id;
  private final String partner;

  /**
   * Static mapping of partner name to id.
   */
  private static final Map<String, String> MAP;

  static {
    Map<String, String> map = new HashMap<>();
    for (EmailPartnerIdEnum partner : EmailPartnerIdEnum.values()) {
      map.put(partner.getId(), partner.getPartner());
    }

    MAP = Collections.unmodifiableMap(map);
  }

  EmailPartnerIdEnum(final String id, final String partner) {
    this.id = id;
    this.partner = partner;
  }

  public String getId() {
    return id;
  }

  public String getPartner() {
    return partner;
  }

  public static String parse(String id) {
    return MAP.getOrDefault(id, null);
  }
}
