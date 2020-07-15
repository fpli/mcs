package com.ebay.app.raptor.chocolate.adservice.util.idmapping;

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
  public boolean addMapping(String adguid, String guid, String userId) {
    adguidIdMap.put(ADGUID_GUID_PREFIX + adguid, guid);
    adguidIdMap.put(ADGUID_UID_PREFIX + adguid, userId);
    adguidIdMap.put(GUID_ADGUID_PREFIX + guid, adguid);
    adguidIdMap.put(GUID_UID_PREFIX + guid, userId);
    adguidIdMap.put(UID_GUID_PREFIX + userId, guid);

    return true;
  }

  @Override
  public String getGuidByAdguid(String id) {
    return adguidIdMap.getOrDefault(ADGUID_GUID_PREFIX + id, "");
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
