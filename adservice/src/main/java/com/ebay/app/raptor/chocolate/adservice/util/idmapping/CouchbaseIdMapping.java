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
  public String getGuidByAdguid(String id) {
    if(!StringUtil.isNullOrEmpty(id)) {
      return CouchbaseClient.getInstance().getGuidByAdguid(id);
    }
    return "";
  }

  @Override
  public String getUidByAdguid(String id) {
    if(!StringUtil.isNullOrEmpty(id)) {
      return CouchbaseClient.getInstance().getUidByAdguid(id);
    }
    return "";
  }

  @Override
  public String getAdguidByGuid(String id) {
    if(!StringUtil.isNullOrEmpty(id)) {
      return CouchbaseClient.getInstance().getAdguidByGuid(id);
    }
    return "";
  }

  @Override
  public String getUidByGuid(String id) {
    if(!StringUtil.isNullOrEmpty(id)) {
      return CouchbaseClient.getInstance().getUidByGuid(id);
    }
    return "";
  }

  @Override
  public String getGuidByUid(String id) {
    if(!StringUtil.isNullOrEmpty(id)) {
      return CouchbaseClient.getInstance().getGuidByUid(id);
    }
    return "";
  }

}
