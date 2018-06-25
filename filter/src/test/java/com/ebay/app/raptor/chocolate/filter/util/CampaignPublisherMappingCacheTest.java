package com.ebay.app.raptor.chocolate.filter.util;

import com.ebay.app.raptor.chocolate.avro.PublisherCacheEntry;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import com.ebay.dukes.CacheClient;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.BaseDelegatingCacheClient;
import com.ebay.dukes.couchbase2.Couchbase2CacheClient;
import com.ebay.traffic.chocolate.common.MiniZookeeperCluster;
import org.junit.*;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CampaignPublisherMappingCacheTest {
    private static ApplicationOptions options = mock(ApplicationOptions.class);
    private static MiniZookeeperCluster zookeeperCluster;
    private static final String zkRootDir = "/chocolate/filter/unit-test-pubcache";
    private static final String zkDriverIdPath = "/chocolate/filter/driverid/23423423";
    private static FilterZookeeperClient zookeeperClient;
    private static CouchbaseClient couchbaseClient;

    @BeforeClass
    public static void setUp() throws Exception{
        zookeeperCluster = new MiniZookeeperCluster();
        zookeeperCluster.start();
        zookeeperClient = new FilterZookeeperClient(zookeeperCluster.getConnectionString(),
            zkRootDir, zkDriverIdPath);
      CouchbaseClientMock.createClient();

      CacheFactory cacheFactory = Mockito.mock(CacheFactory.class);
      BaseDelegatingCacheClient baseDelegatingCacheClient = Mockito.mock(BaseDelegatingCacheClient.class);
      Couchbase2CacheClient couchbase2CacheClient = Mockito.mock(Couchbase2CacheClient.class);
      when(couchbase2CacheClient.getCouchbaseClient()).thenReturn(CouchbaseClientMock.getBucket());
      when(baseDelegatingCacheClient.getCacheClient()).thenReturn(couchbase2CacheClient);
      when(cacheFactory.getClient(any())).thenReturn(baseDelegatingCacheClient);

      couchbaseClient = new CouchbaseClient(cacheFactory);
      CouchbaseClient.init(couchbaseClient);
        zookeeperClient.start(t -> {
        CampaignPublisherMappingCache.getInstance().addMapping(t.getCampaignId(), t.getPublisherId());
        couchbaseClient.addMappingRecord(t.getCampaignId(), t.getPublisherId());
      });
    }

    @After
    public void destroy() {
      CampaignPublisherMappingCache.destroy();
    }

    @AfterClass
  public static void tearDown() throws IOException{
      zookeeperClient.close();
      zookeeperCluster.shutdown();
      CouchbaseClientMock.tearDown();
      CouchbaseClient.close();
  }

    @Test
    public void testClient() throws Exception {
        PublisherCacheEntry entry1 = new PublisherCacheEntry();
        entry1.setCampaignId(10000L);
        entry1.setPublisherId(20000L);
        entry1.setTimestamp(99999L);
        byte[] bytes = zookeeperClient.toBytes(entry1);
        PublisherCacheEntry result = zookeeperClient.fromBytes(bytes);
        assertEquals(result.getCampaignId(), entry1.getCampaignId());
        assertEquals(result.getPublisherId(), entry1.getPublisherId());
    }

    @Test
    public void testLookupWithRecordInCache() throws Exception {
        PublisherCacheEntry entry1 = new PublisherCacheEntry();
        entry1.setCampaignId(10001L);
        entry1.setPublisherId(20001L);
        entry1.setTimestamp(99999L);

        PublisherCacheEntry entry2 = new PublisherCacheEntry();
        entry2.setCampaignId(10002L);
        entry2.setPublisherId(20002L);
        entry2.setTimestamp(88888L);
      zookeeperClient.addEntry(entry1);
      zookeeperClient.addEntry(entry2);

      zookeeperClient.iterate(
                t -> CampaignPublisherMappingCache.getInstance().
                        addMapping(t.getCampaignId(), t.getPublisherId()));

        Thread.sleep(1000);
        assertEquals(Long.valueOf(20001L),CampaignPublisherMappingCache.getInstance().lookup(10001L));
    }

    @Test
    public void testLookupWithCacheMiss() throws Exception {
        PublisherCacheEntry entry1 = new PublisherCacheEntry();
        entry1.setCampaignId(10003L);
        entry1.setPublisherId(20003L);
        entry1.setTimestamp(99999L);

        PublisherCacheEntry entry2 = new PublisherCacheEntry();
        entry2.setCampaignId(10004L);
        entry2.setPublisherId(20004L);
        entry2.setTimestamp(88888L);
      zookeeperClient.addEntry(entry1);
      zookeeperClient.addEntry(entry2);

        CampaignPublisherMappingCache.getInstance().cache.clear();
      CouchbaseClient.getInstance().addMappingRecord(10003, 20003);
      CouchbaseClient.getInstance().addMappingRecord(10004, 20004);

        assertEquals(Long.valueOf(20004L),CampaignPublisherMappingCache.getInstance().lookup(10004L));
        assertEquals(Long.valueOf(20004L),CampaignPublisherMappingCache.getInstance().cache.get(10004L));
    }

    @Test
    public void testInsertAndGetCouchBaseRecord() throws InterruptedException{
      CouchbaseClient.getInstance().addMappingRecord(10005L,20005L);
        assertEquals(20005L, CouchbaseClient.getInstance().getPublisherID(10005L));
    }
}
