package com.ebay.app.raptor.chocolate.adservice.util.idmapping;

import com.couchbase.client.deps.io.netty.util.internal.StringUtil;
import com.ebay.app.raptor.chocolate.adservice.constant.StringConstants;
import org.apache.commons.lang3.StringUtils;
import com.ebay.app.raptor.chocolate.adservice.util.CouchbaseClientV2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Couchbase id mapping
 *
 * @author xiangli4
 */
@Service("cb")
public class CouchbaseIdMapping implements IdMapable {
    @Autowired
    CouchbaseClientV2 couchbaseClientV2;

    @Override
    public boolean addMapping(String adguid, String guidList, String guid, String uid) {
        couchbaseClientV2.addMappingRecord(adguid, guidList, guid, uid);
        return true;
    }

    @Override
    public String getGuidListByAdguid(String id) {
        if (!StringUtil.isNullOrEmpty(id)) {
            String guidList = couchbaseClientV2.getGuidListByAdguid(id);
            if ("[guid]".equals(guidList)) {
                return "";
            } else {
                return guidList;
            }
        }
        return "";
    }

    @Override
    public String getGuidByAdguid(String id) {
        if (!StringUtil.isNullOrEmpty(id)) {
            String guidList = getGuidListByAdguid(id);
            if (!StringUtils.isEmpty(guidList)) {
                String[] guids = guidList.split(StringConstants.AND);
                return guids[guids.length - 1];
            }
        }

        return "";
    }

    @Override
    public String getUidByAdguid(String id) {
        if (!StringUtil.isNullOrEmpty(id)) {
            return couchbaseClientV2.getUidByAdguid(id);
        }
        return "";
    }

    @Override
    public String getAdguidByGuid(String id) {
        if (!StringUtil.isNullOrEmpty(id)) {
            return couchbaseClientV2.getAdguidByGuid(id);
        }
        return "";
    }

    @Override
    public String getUidByGuid(String id) {
        if (!StringUtil.isNullOrEmpty(id)) {
            return couchbaseClientV2.getUidByGuid(id);
        }
        return "";
    }

    @Override
    public String getGuidByUid(String id) {
        if (!StringUtil.isNullOrEmpty(id)) {
            return couchbaseClientV2.getGuidByUid(id);
        }
        return "";
    }

}
