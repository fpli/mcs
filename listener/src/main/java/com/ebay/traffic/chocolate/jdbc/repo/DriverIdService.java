package com.ebay.traffic.chocolate.jdbc.repo;

public interface DriverIdService {
  int getDriverId(String hostname, String ip, int maxDriverId, int retry);
}
