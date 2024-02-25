package com.ebay.app.raptor.chocolate.jdbc.repo;

public interface DriverIdService {
  int getDriverId(String hostname, String ip, int maxDriverId, int retry);
}
