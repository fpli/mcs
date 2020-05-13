package com.ebay.app.raptor.chocolate.jdbc.repo;

import com.ebay.app.raptor.chocolate.common.Hostname;
import com.ebay.app.raptor.chocolate.jdbc.model.HostnameDriverIdMappingEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Get driver id. If hostname exists in db, then just return the existed driver id. If not, generate a new one.
 * Multi apps may generate the same driver id, but only one app can successfully write to db as driver id is set as
 * unique.
 *
 * @author zhiyuawang
 */
@Service
public class DriverIdServiceImpl implements DriverIdService {
  private static final Logger LOGGER = LoggerFactory.getLogger(DriverIdServiceImpl.class);

  @Resource
  private DriverIdRepository driverIdRepository;

  @Override
  public int getDriverId(String hostname, String ip, int maxDriverId, int retry) {
    List<HostnameDriverIdMappingEntity> all = driverIdRepository.findAll();
    if (Hostname.HOSTNAME.equalsIgnoreCase("lvsadservice2-rmqbc-tess0075.lvs02.dev.ebayc3.com")) {
      throw new IllegalArgumentException("lvsadservice2-rmqbc-tess0075.lvs02.dev.ebayc3.com");
    }
    // if hostname exists in database, use existed driver id
    for (HostnameDriverIdMappingEntity hostnameDriverId : all) {
      if (hostnameDriverId.getHostname().equals(hostname)) {
        int driverId = hostnameDriverId.getDriverId();
        LOGGER.info("{} {} driver id is {}", hostname, ip, driverId);
        return driverId;
      }
    }

    // try multi times to generate new driver id
    int driverId = -1;
    int i = 0;
    while (i < retry) {
      driverId = generateNewDriverId(hostname, ip, maxDriverId);
      if (driverId != -1) {
        break;
      }
      i++;
    }

    if (driverId == -1) {
      throw new IllegalArgumentException("cannot generate a new driver id!");
    }

    LOGGER.info("{} {} driver id is {}", hostname, ip, driverId);

    return driverId;
  }

  /**
   * Generate new driver id
   * @param hostname hostname
   * @param ip ip
   * @return new driver id
   */
  @Transactional(isolation = Isolation.READ_COMMITTED, rollbackFor = Exception.class)
  public int generateNewDriverId(String hostname, String ip, int maxDriverId) {
    long currentTimeMillis = System.currentTimeMillis();
    int driverId = -1;

    List<HostnameDriverIdMappingEntity> all = driverIdRepository.findAll();
    Set<Integer> existedDriverId = all.stream().map(HostnameDriverIdMappingEntity::getDriverId).collect(Collectors.toSet());

    // use the smallest missing number as driver id
    for (int i = 0; i <= maxDriverId; i++) {
      if (!existedDriverId.contains(i)) {
        driverId = i;
        break;
      }
    }

    if (driverId == -1) {
      throw new IllegalArgumentException("all driver ids are in use!");
    }

    // generate failed if this driver id has been inserted in db,
    try {
      LOGGER.info("try save {} {} {} {} to db", hostname, ip, driverId, currentTimeMillis);
      HostnameDriverIdMappingEntity entity = new HostnameDriverIdMappingEntity();
      entity.setHostname(hostname);
      entity.setIp(ip);
      entity.setDriverId(driverId);
      entity.setCreateTime(new Timestamp(currentTimeMillis));
      driverIdRepository.saveAndFlush(entity);
    } catch (DataIntegrityViolationException e) {
      LOGGER.error(e.getMessage());
      driverId = -1;
    }

    return driverId;
  }
}
