package com.ebay.app.raptor.chocolate.jdbc.model;

import javax.persistence.*;
import java.sql.Timestamp;
import java.util.Objects;

@Entity
@Table(name = "hostname_driver_id_mapping")
public class HostnameDriverIdMappingEntity {
  private String hostname;
  private String ip;
  private int driverId;
  private Timestamp createTime;
  private Timestamp lastQueryTime;

  @Id
  @Basic
  @Column(name = "hostname", nullable = false, length = 100)
  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  @Basic
  @Column(name = "ip", nullable = false, length = 50)
  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  @Basic
  @Column(name = "driver_id", nullable = false)
  public int getDriverId() {
    return driverId;
  }

  public void setDriverId(int driverId) {
    this.driverId = driverId;
  }

  @Basic
  @Column(name = "create_time", nullable = false)
  public Timestamp getCreateTime() {
    return createTime;
  }

  public void setCreateTime(Timestamp createTime) {
    this.createTime = createTime;
  }

  @Basic
  @Column(name = "last_query_time", nullable = false)
  public Timestamp getLastQueryTime() {
    return lastQueryTime;
  }

  public void setLastQueryTime(Timestamp lastQueryTime) {
    this.lastQueryTime = lastQueryTime;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    HostnameDriverIdMappingEntity that = (HostnameDriverIdMappingEntity) o;
    return driverId == that.driverId &&
            Objects.equals(hostname, that.hostname) &&
            Objects.equals(ip, that.ip) &&
            Objects.equals(createTime, that.createTime) &&
            Objects.equals(lastQueryTime, that.lastQueryTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(hostname, ip, driverId, createTime, lastQueryTime);
  }

  @Override
  public String toString() {
    return "HostnameDriverIdMappingEntity{" +
            "hostname='" + hostname + '\'' +
            ", ip='" + ip + '\'' +
            ", driverId=" + driverId +
            ", createTime=" + createTime +
            ", lastQueryTime=" + lastQueryTime +
            '}';
  }
}
