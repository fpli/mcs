/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.adservice.util.idmapping;

import com.ebay.app.raptor.chocolate.adservice.util.CouchbaseClient;
import com.ebay.app.raptor.chocolate.util.CouchbaseClientMock;
import com.ebay.dukes.CacheFactory;
import com.ebay.dukes.base.BaseDelegatingCacheClient;
import com.ebay.dukes.couchbase2.Couchbase2CacheClient;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

public class CouchbaseIdMappingTest {
  private IdMapable idMapping = new CouchbaseIdMapping();
  private static CouchbaseClient couchbaseClient;

  @BeforeClass
  public static void preTest() throws Exception {
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

  @Test
  public void addMapping() {
    idMapping.addMapping("123", "456", "12345");
    idMapping.addMapping("234", "", "12345");
    idMapping.addMapping("345", "456", "");

    assertEquals("456", idMapping.getGuid("123"));
    assertEquals("12345", idMapping.getUid("123"));
    assertEquals("123", idMapping.getAdguid("456"));

    assertEquals("", idMapping.getGuid("234"));
    assertEquals("12345", idMapping.getUid("234"));
    assertEquals("", idMapping.getAdguid(""));

    assertEquals("456", idMapping.getGuid("345"));
    assertEquals("", idMapping.getUid("345"));


    assertEquals("", idMapping.getGuid("2222"));
    assertEquals("", idMapping.getUid("2222"));
  }

  @Test
  public void getGuid() {
    assertEquals("", idMapping.getGuid("222"));
  }
}
