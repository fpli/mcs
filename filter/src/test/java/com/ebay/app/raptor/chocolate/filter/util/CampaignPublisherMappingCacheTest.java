package com.ebay.app.raptor.chocolate.filter.util;

import com.ebay.app.raptor.chocolate.FilterApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = FilterApplication.class)
public class CampaignPublisherMappingCacheTest {
    @Autowired
    @Qualifier("CouchbaseClientV2")
    private CouchbaseClientV2 couchbaseClient;

    @Test
    public void testLookupWithCacheMiss() throws Exception {
        couchbaseClient.deleteMappingRecord(10003L);
        couchbaseClient.deleteMappingRecord(10004L);

        couchbaseClient.addMappingRecord(10003, 20003);
        couchbaseClient.addMappingRecord(10004, 20004);

        assertEquals(Long.valueOf(20004L), CampaignPublisherMappingCache.getInstance().lookup(10004L));
        assertEquals(Long.valueOf(20004L), CampaignPublisherMappingCache.getInstance().cache.get(10004L));
    }

    @Test
    public void testInsertAndGetCouchBaseRecord() throws InterruptedException {
        //couchbaseClient.deleteMappingRecord(10005L);
        couchbaseClient.addMappingRecord(10005L, 20005L);
        assertEquals(20005L, couchbaseClient.getPublisherID(10005L));
    }
}
