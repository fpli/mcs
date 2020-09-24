/*
 * Copyright (c) 2020. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.jdbc.model;

import org.junit.Test;
import java.sql.Timestamp;
import java.time.Instant;

import static org.junit.Assert.*;

public class HostnameDriverIdMappingEntityTest {

  @Test
  public void testEquals() {
    Timestamp ts = Timestamp.from(Instant.now());
    HostnameDriverIdMappingEntity entity1 = new HostnameDriverIdMappingEntity();
    entity1.setDriverId(1);
    entity1.setCreateTime(ts);
    entity1.setHostname("localhost");
    entity1.setIp("127.0.0.1");
    entity1.setLastQueryTime(ts);
    HostnameDriverIdMappingEntity entity2 = new HostnameDriverIdMappingEntity();
    entity2.setDriverId(1);
    entity2.setCreateTime(ts);
    entity2.setHostname("localhost");
    entity2.setIp("127.0.0.1");
    entity2.setLastQueryTime(ts);
    assertTrue(entity1.equals(entity2));
  }

  @Test
  public void testHashCode() {
    Timestamp ts = Timestamp.from(Instant.now());
    HostnameDriverIdMappingEntity entity1 = new HostnameDriverIdMappingEntity();
    entity1.setDriverId(1);
    entity1.setCreateTime(ts);
    entity1.setHostname("localhost");
    entity1.setIp("127.0.0.1");
    entity1.setLastQueryTime(ts);
    assertEquals("HostnameDriverIdMappingEntity{hostname='localhost', ip='127.0.0.1', driverId=1, createTime="
        + ts.toString()
        + ", lastQueryTime="
        + ts.toString()
        + "}", entity1.toString());
  }
}