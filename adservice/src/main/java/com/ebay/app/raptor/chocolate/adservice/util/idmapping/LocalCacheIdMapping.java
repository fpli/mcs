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
    return true;
  }

  @Override
  public String getGuid(String adguid) {
    return adguidIdMap.getOrDefault(ADGUID_GUID_PREFIX + adguid, "");
  }

  @Override
  public String getUid(String adguid) {
    return adguidIdMap.getOrDefault(ADGUID_UID_PREFIX + adguid, "");
  }

}
