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

  private Map<String, String> adguidGuidMap = new HashMap<>();

  @Override
  public boolean addMapping(String adguid, String guid) {
    adguidGuidMap.put(adguid, guid);
    return true;
  }

  @Override
  public String getGuid(String adguid) {
    return adguidGuidMap.getOrDefault(adguid, "");
  }
}
