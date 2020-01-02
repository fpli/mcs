package com.ebay.app.raptor.chocolate.adservice.util.idmapping;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class LocalCacheIdMappingTest {

  private IdMapable idMapping = new LocalCacheIdMapping();

  @Test
  public void addMapping() {
    idMapping.addMapping("123", "456");
    assertEquals("456", idMapping.getGuid("123"));
  }

  @Test
  public void getGuid() {
    assertEquals("", idMapping.getGuid("222"));
  }
}