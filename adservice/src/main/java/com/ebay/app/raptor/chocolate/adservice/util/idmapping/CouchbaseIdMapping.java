package com.ebay.app.raptor.chocolate.adservice.util.idmapping;

import com.couchbase.client.deps.io.netty.util.internal.StringUtil;
import com.ebay.app.raptor.chocolate.adservice.util.CouchbaseClient;
import org.springframework.stereotype.Service;

/**
 * Couchbase id mapping
 * @author xiangli4
 */
@Service("cb")
public class CouchbaseIdMapping implements IdMapable {
  @Override
  public boolean addMapping(String adguid, String guid, String uid) {
    CouchbaseClient.getInstance().addMappingRecord(adguid, guid, uid);
    return true;
  }

  @Override
  public String getGuid(String adguid) {
    if(!StringUtil.isNullOrEmpty(adguid)) {
      return CouchbaseClient.getInstance().getGuidByAdguid(adguid);
    }
    return "";
  }

  @Override
  public String getUid(String adguid) {
    if(!StringUtil.isNullOrEmpty(adguid)) {
      return CouchbaseClient.getInstance().getUidByAdguid(adguid);
    }
    return "";
  }

}
