package com.ebay.app.raptor.chocolate.adservice.util;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class CouchbaseUtil {

  private Map<String, String> adguidGuidMap = new HashMap<>();

  public void addMapping(String adguid, String guid) {
    adguidGuidMap.put(adguid, guid);
  }

  public String getGuid(String adguid) {
    return adguidGuidMap.getOrDefault(adguid, "No sync");
  }
}
