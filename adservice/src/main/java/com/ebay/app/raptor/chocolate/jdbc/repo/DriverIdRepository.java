package com.ebay.app.raptor.chocolate.jdbc.repo;

import com.ebay.app.raptor.chocolate.jdbc.model.HostnameDriverIdMappingEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DriverIdRepository extends JpaRepository<HostnameDriverIdMappingEntity, Long> {
}
