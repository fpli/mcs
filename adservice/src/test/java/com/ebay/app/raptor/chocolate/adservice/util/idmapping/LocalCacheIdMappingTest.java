package com.ebay.app.raptor.chocolate.adservice.util.idmapping;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class LocalCacheIdMappingTest {

  private IdMapable idMapping = new LocalCacheIdMapping();

  @Test
  public void addMapping() {
    idMapping.addMapping("123", "456", "12345");
    idMapping.addMapping("234", "", "12345");
    idMapping.addMapping("345", "456", "");

    assertEquals("456", idMapping.getGuid("123"));
    assertEquals("12345", idMapping.getUid("123"));

    assertEquals("", idMapping.getGuid("234"));
    assertEquals("12345", idMapping.getUid("234"));

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