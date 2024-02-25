package com.ebay.app.raptor.chocolate.eventlistener.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum SearchEngineFreeListingsRotationEnum {
  AU(15, "705-159471-026250-9"),
  CA(2, "706-159471-001776-7"),
  DE(77, "707-159471-132488-9"),
  FR(71, "709-159471-063049-0"),
  UK(3, "710-159471-115275-8"),
  US(0, "711-159470-902746-9"),
  IT(101, "724-159471-099392-6"),
  ES(186, "1185-159470-979592-4"),
  NL(146, "1346-159470-942940-1"),
  BE(123, "1553-159471-083154-5"),
  HK(201, "3422-159471-070447-8"),
  SG(216, "3423-159471-141769-9"),
  IN(203, "4686-159471-046749-7"),
  PH(211, "4824-159471-091455-8"),
  MY(207, "4825-159470-923347-5"),
  PL(212, "4908-159471-109682-5"),
  AT(16, "5221-159470-962265-4"),
  CH(193, "5222-159471-149672-9"),
  IE(205, "5282-159471-122916-3");

  private int siteId;
  private String rotation;

  SearchEngineFreeListingsRotationEnum(int siteId, String rotation) {
    this.siteId = siteId;
    this.rotation = rotation;
  }

  public int getSiteId() {
    return siteId;
  }

  public String getRotation() {
    return MAP.get(this.siteId).rotation;
  }

  private static final Map<Integer, SearchEngineFreeListingsRotationEnum> MAP;

  static {
    Map<Integer, SearchEngineFreeListingsRotationEnum> map = new HashMap<>();
    for (SearchEngineFreeListingsRotationEnum c : SearchEngineFreeListingsRotationEnum.values()) {
      map.put(c.getSiteId(), c);
    }

    MAP = Collections.unmodifiableMap(map);
  }

  public static SearchEngineFreeListingsRotationEnum parse(int siteId) {
    return MAP.getOrDefault(siteId, SearchEngineFreeListingsRotationEnum.US);
  }

}
