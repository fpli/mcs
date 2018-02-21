package com.ebay.app.raptor.chocolate.filter.util;

import com.ebay.app.raptor.chocolate.avro.PublisherCacheEntry;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import org.apache.curator.test.TestingServer;
import org.junit.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CampaignPublisherMappingCacheTest {
    private static ApplicationOptions options = mock(ApplicationOptions.class);
    private static TestingServer zkServer;
    private static final String zkRootDir = "/chocolate/filter/unit-test-pubcache";
    private static final String zkDriverIdPath = "/chocolate/filter/driverid/23423423";

    @BeforeClass
    public static void setUp() throws Exception{
        zkServer = TestHelper.initializeZkServer();
        when(options.getPublisherCacheZkConnectString()).thenReturn(zkServer.getConnectString());
        when(options.getPublisherCacheZkRoot()).thenReturn(zkRootDir);
        when(options.getZkDriverIdNode()).thenReturn(zkDriverIdPath);

        FilterZookeeperClient.init(options);
    }

    @Before
    public void construct() throws Exception{
      CouchbaseClientMock.createClient();
      CouchbaseClient.init(CouchbaseClientMock.getCluster(), CouchbaseClientMock.getBucket());
    }

    @After
    public void destroy() {
      CampaignPublisherMappingCache.destroy();
      CouchbaseClient.close();
    }

    @AfterClass
  public static void tearDown() {
      FilterZookeeperClient.getInstance().close();
      TestHelper.terminateZkServer();
  }

    @Test
    public void testClient() throws Exception {
        PublisherCacheEntry entry1 = new PublisherCacheEntry();
        entry1.setCampaignId(11111L);
        entry1.setPublisherId(55555L);
        entry1.setTimestamp(99999L);
        byte[] bytes = FilterZookeeperClient.toBytes(entry1);
        PublisherCacheEntry result = FilterZookeeperClient.fromBytes(bytes);
        assertEquals(result.getCampaignId(), entry1.getCampaignId());
        assertEquals(result.getPublisherId(), entry1.getPublisherId());
    }

    @Test
    public void testLookupWithRecordInCache() throws Exception {
        PublisherCacheEntry entry1 = new PublisherCacheEntry();
        entry1.setCampaignId(11111L);
        entry1.setPublisherId(55555L);
        entry1.setTimestamp(99999L);

        PublisherCacheEntry entry2 = new PublisherCacheEntry();
        entry2.setCampaignId(22222L);
        entry2.setPublisherId(66666L);
        entry2.setTimestamp(88888L);
        FilterZookeeperClient.getInstance().addEntry(entry1);
        FilterZookeeperClient.getInstance().addEntry(entry2);

        FilterZookeeperClient.getInstance().iterate(
                t -> CampaignPublisherMappingCache.getInstance().
                        addMapping(t.getCampaignId(), t.getPublisherId()));

        Thread.sleep(1000);
        assertEquals(Long.valueOf(55555L),CampaignPublisherMappingCache.getInstance().lookup(11111L));
    }

    @Test
    public void testLookupWithCacheMiss() throws Exception {
        PublisherCacheEntry entry1 = new PublisherCacheEntry();
        entry1.setCampaignId(11111L);
        entry1.setPublisherId(55555L);
        entry1.setTimestamp(99999L);

        PublisherCacheEntry entry2 = new PublisherCacheEntry();
        entry2.setCampaignId(22222L);
        entry2.setPublisherId(66666L);
        entry2.setTimestamp(88888L);
        FilterZookeeperClient.getInstance().addEntry(entry1);
        FilterZookeeperClient.getInstance().addEntry(entry2);

        CampaignPublisherMappingCache.getInstance().cache.clear();
        CouchbaseClient.getInstance().addMappingRecord(11111, 55555);
        CouchbaseClient.getInstance().addMappingRecord(22222, 66666);

        assertEquals(Long.valueOf(66666L),CampaignPublisherMappingCache.getInstance().lookup(22222L));
        assertEquals(Long.valueOf(66666L),CampaignPublisherMappingCache.getInstance().cache.get(22222L));
    }

    @Test
    public void testInsertAndGetCouchBaseRecord() {
        CouchbaseClient.getInstance().addMappingRecord(11111L,55555L);
        assertEquals(55555L, CouchbaseClient.getInstance().getPublisherID(11111L));
    }

}
