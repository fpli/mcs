package com.ebay.app.raptor.chocolate.adservice.util.idmapping;

import com.ebay.app.raptor.chocolate.adservice.util.CouchbaseClient;
import org.springframework.stereotype.Service;

/**
 * Couchbase id mapping
 * @author xiangli4
 */
@Service("cb")
public class CouchbaseIdMapping implements IdMapable {
  @Override
  public boolean addMapping(String adguid, String guid, String userId) {
    return CouchbaseClient.getInstance().addMappingRecord(adguid, String.format(format, guid, userId));
  }

  @Override
  public String getValues(String adguid) {
    return CouchbaseClient.getInstance().getValuesByAdguid(adguid);
  }
}
