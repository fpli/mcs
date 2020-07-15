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

    assertEquals("456", idMapping.getGuidByAdguid("123"));
    assertEquals("12345", idMapping.getUidByAdguid("123"));

    assertEquals("", idMapping.getGuidByAdguid("234"));
    assertEquals("12345", idMapping.getUidByAdguid("234"));

    assertEquals("456", idMapping.getGuidByAdguid("345"));
    assertEquals("", idMapping.getUidByAdguid("345"));


    assertEquals("", idMapping.getGuidByAdguid("2222"));
    assertEquals("", idMapping.getUidByAdguid("2222"));
  }

  @Test
  public void getGuid() {
    assertEquals("", idMapping.getGuidByAdguid("222"));
  }
}