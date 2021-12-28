package com.ebay.app.raptor.chocolate.adservice.util.idmapping;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class LocalCacheIdMappingTest {

  private IdMapable idMapping = new LocalCacheIdMapping();

  @Test
  public void addMapping() {
    idMapping.addMapping("123", "456", "456", "12345");
    idMapping.addMapping("234", "", "", "12345");
    idMapping.addMapping("345", "456", "456",  "");
    idMapping.addMapping("345", "456,789", "789", "12345");

    assertEquals("456", idMapping.getGuidListByAdguid("123"));
    assertEquals("456", idMapping.getGuidByAdguid("123"));
    assertEquals("12345", idMapping.getUidByAdguid("123"));

    assertEquals("", idMapping.getGuidListByAdguid("234"));
    assertEquals("", idMapping.getGuidByAdguid("234"));
    assertEquals("12345", idMapping.getUidByAdguid("234"));

    assertEquals("456,789", idMapping.getGuidListByAdguid("345"));
    assertEquals("789", idMapping.getGuidByAdguid("345"));
    assertEquals("12345", idMapping.getUidByAdguid("345"));

    assertEquals("345", idMapping.getAdguidByGuid("456"));
    assertEquals("12345", idMapping.getUidByGuid("456"));

    assertEquals("345", idMapping.getAdguidByGuid("789"));
    assertEquals("12345", idMapping.getUidByGuid("789"));

    assertEquals("789", idMapping.getGuidByUid("12345"));

    assertEquals("", idMapping.getGuidListByAdguid("2222"));
    assertEquals("", idMapping.getGuidByAdguid("2222"));
    assertEquals("", idMapping.getUidByAdguid("2222"));
  }

  @Test
  public void getGuid() {
    assertEquals("", idMapping.getGuidByAdguid("222"));
  }
}
