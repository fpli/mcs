package com.ebay.app.raptor.chocolate.jdbc.repo;

import com.ebay.app.raptor.chocolate.jdbc.model.ThirdpartyWhitelist;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

/**
 * Query on the table thirdparty_whitelist
 *
 * Created by jialili1 on 11/14/19
 */
public interface ThirdpartyWhitelistRepo extends JpaRepository<ThirdpartyWhitelist, Integer>{

  List<ThirdpartyWhitelist> findByTypeId(Integer typeId);
}
