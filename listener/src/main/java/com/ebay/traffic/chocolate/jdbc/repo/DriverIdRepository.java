package com.ebay.traffic.chocolate.jdbc.repo;

import com.ebay.traffic.chocolate.jdbc.model.HostnameDriverIdMappingEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DriverIdRepository extends JpaRepository<HostnameDriverIdMappingEntity, Long> {
}
