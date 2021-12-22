package com.ebay.app.raptor.chocolate.adservice.util.idmapping;

import com.ebay.app.raptor.chocolate.adservice.constant.StringConstants;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Local cache id mapping
 * @author xiangli4
 */
@Service("lc")
public class LocalCacheIdMapping implements IdMapable {

  private Map<String, String> adguidIdMap = new HashMap<>();

  @Override
  public boolean addMapping(String adguid, String guidList, String guid, String userId) {
    if (!StringUtils.isEmpty(adguid)) {
      if (!StringUtils.isEmpty(guidList)) {
        adguidIdMap.put(ADGUID_GUID_PREFIX + adguid, guidList);
      }
      if (!StringUtils.isEmpty(userId)) {
        adguidIdMap.put(ADGUID_UID_PREFIX + adguid, userId);
      }
    }

    if (!StringUtils.isEmpty(guid)) {
      if (!StringUtils.isEmpty(userId)) {
        adguidIdMap.put(GUID_UID_PREFIX + guid, userId);
      }
      if (!StringUtils.isEmpty(adguid)) {
        adguidIdMap.put(GUID_ADGUID_PREFIX + guid, adguid);
      }
    }

    if (!StringUtils.isEmpty(userId) && !StringUtils.isEmpty(guid)) {
      adguidIdMap.put(UID_GUID_PREFIX + userId, guid);
    }

    return true;
  }

  @Override
  public String getGuidListByAdguid(String id) {
    return adguidIdMap.getOrDefault(ADGUID_GUID_PREFIX + id, "");
  }

  @Override
  public String getGuidByAdguid(String id) {
    String guidList = adguidIdMap.getOrDefault(ADGUID_GUID_PREFIX + id, "");
    if (!StringUtils.isEmpty(guidList)) {
      String[] guids = guidList.split(StringConstants.AND);
      return guids[guids.length - 1];
    }

    return "";
  }

  @Override
  public String getUidByAdguid(String id) {
    return adguidIdMap.getOrDefault(ADGUID_UID_PREFIX + id, "");
  }

  @Override
  public String getAdguidByGuid(String id) {
    return adguidIdMap.getOrDefault(GUID_ADGUID_PREFIX + id, "");
  }

  @Override
  public String getUidByGuid(String id) {
    return adguidIdMap.getOrDefault(GUID_UID_PREFIX + id, "");
  }

  @Override
  public String getGuidByUid(String id) {
    return adguidIdMap.getOrDefault(UID_GUID_PREFIX + id, "");
  }

}
