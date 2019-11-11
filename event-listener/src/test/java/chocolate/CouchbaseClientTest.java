package chocolate;

import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.app.raptor.chocolate.eventlistener.util.CouchbaseClient;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.BaseDelegatingCacheClient;
import com.ebay.dukes.couchbase2.Couchbase2CacheClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class CouchbaseClientTest {
    private static CouchbaseClient couchbaseClient;

    @BeforeClass
    public static void setUp() throws Exception {
        CouchbaseClientMock.createClient();
        CacheFactory cacheFactory = Mockito.mock(CacheFactory.class);
        BaseDelegatingCacheClient baseDelegatingCacheClient = Mockito.mock(BaseDelegatingCacheClient.class);
        Couchbase2CacheClient couchbase2CacheClient = Mockito.mock(Couchbase2CacheClient.class);
        when(couchbase2CacheClient.getCouchbaseClient()).thenReturn(CouchbaseClientMock.getBucket());
        when(baseDelegatingCacheClient.getCacheClient()).thenReturn(couchbase2CacheClient);
        when(cacheFactory.getClient(any())).thenReturn(baseDelegatingCacheClient);

        couchbaseClient = new CouchbaseClient(cacheFactory);
        CouchbaseClient.init(couchbaseClient);
    }

    @AfterClass
    public static void tearDown(){
        CouchbaseClientMock.tearDown();
        CouchbaseClient.close();
    }

    @Test
    public void testAddMappingRecord() {
        couchbaseClient.addMappingRecord("guidtest123", "cguidtest123");
        assertEquals("cguidtest123", couchbaseClient.getCguid("guidtest123"));
    }
}
