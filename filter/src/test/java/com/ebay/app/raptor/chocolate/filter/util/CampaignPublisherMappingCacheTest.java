package com.ebay.app.raptor.chocolate.filter.util;

import com.ebay.app.raptor.chocolate.avro.PublisherCacheEntry;
import com.ebay.app.raptor.chocolate.filter.ApplicationOptions;
import com.ebay.traffic.chocolate.common.MiniZookeeperCluster;
import org.junit.*;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class CampaignPublisherMappingCacheTest {
    private static ApplicationOptions options = mock(ApplicationOptions.class);
    private static MiniZookeeperCluster zookeeperCluster;
    private static final String zkRootDir = "/chocolate/filter/unit-test-pubcache";
    private static final String zkDriverIdPath = "/chocolate/filter/driverid/23423423";
    private static FilterZookeeperClient zookeeperClient;

    @BeforeClass
    public static void setUp() throws Exception{
        zookeeperCluster = new MiniZookeeperCluster();
        zookeeperCluster.start();
        zookeeperClient = new FilterZookeeperClient(zookeeperCluster.getConnectionString(),
            zkRootDir, zkDriverIdPath);
        zookeeperClient.start(t -> {
        CampaignPublisherMappingCache.getInstance().addMapping(t.getCampaignId(), t.getPublisherId());
        CouchbaseClient.getInstance().addMappingRecord(t.getCampaignId(), t.getPublisherId());
      });

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
  public static void tearDown() throws IOException{
      zookeeperClient.close();
      zookeeperCluster.shutdown();
      CouchbaseClient.close();
  }

    @Test
    public void testClient() throws Exception {
        PublisherCacheEntry entry1 = new PublisherCacheEntry();
        entry1.setCampaignId(11111L);
        entry1.setPublisherId(55555L);
        entry1.setTimestamp(99999L);
        byte[] bytes = zookeeperClient.toBytes(entry1);
        PublisherCacheEntry result = zookeeperClient.fromBytes(bytes);
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
      zookeeperClient.addEntry(entry1);
      zookeeperClient.addEntry(entry2);

      zookeeperClient.iterate(
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
      zookeeperClient.addEntry(entry1);
      zookeeperClient.addEntry(entry2);

        CampaignPublisherMappingCache.getInstance().cache.clear();
        CouchbaseClient.getInstance().addMappingRecord(11111, 55555);
        CouchbaseClient.getInstance().addMappingRecord(22222, 66666);

        assertEquals(Long.valueOf(66666L),CampaignPublisherMappingCache.getInstance().lookup(22222L));
        assertEquals(Long.valueOf(66666L),CampaignPublisherMappingCache.getInstance().cache.get(22222L));
    }

    @Test
    public void testInsertAndGetCouchBaseRecord() throws InterruptedException{
        CouchbaseClient.getInstance().addMappingRecord(11111L,55555L);
        assertEquals(55555L, CouchbaseClient.getInstance().getPublisherID(11111L));
    }

}
