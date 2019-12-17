package com.ebay.app.raptor.chocolate.adservice.util.idmapping;

import org.springframework.stereotype.Service;

/**
 * Couchbase id mapping
 * @author xiangli4
 */
@Service("cb")
public class CouchbaseIdMapping implements IdMapable {
  @Override
  public void addMapping(String adguid, String guid) {

  }

  @Override
  public String getGuid(String adguid) {
    return null;
  }
}
