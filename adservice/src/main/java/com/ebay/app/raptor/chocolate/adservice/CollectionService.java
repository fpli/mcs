package com.ebay.app.raptor.chocolate.adservice;

import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.util.CookieReader;
import com.ebay.app.raptor.chocolate.adservice.util.DAPResponseHandler;
import com.ebay.app.raptor.chocolate.constant.Errors;
import com.ebay.kernel.util.guid.Guid;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.container.ContainerRequestContext;
import java.net.UnknownHostException;

/**
 * @author xiangli4
 * Handle ar, impression, email open, and redirection
 */
@Component
@DependsOn("AdserviceService")
public class CollectionService {
  private static final Logger logger = LoggerFactory.getLogger(CollectionService.class);
  private Metrics metrics;
  private static CollectionService instance = null;

  @PostConstruct
  public void postInit() {
    this.metrics = ESMetrics.getInstance();
  }

  /**
   * Collect impression event and send pixel response
   *
   * @param request raw request
   * @return OK or Error message
   */
  public boolean collectAr(HttpServletRequest request, HttpServletResponse response, CookieReader cookieReader, IEndUserContext endUserContext,
                           ContainerRequestContext requestContext) throws Exception {
    new DAPResponseHandler(request, response, cookieReader, endUserContext, requestContext)
            .sendDAPResponse();
    return true;
  }

  // construct X-EBAY-C-TRACKING header for mcs to send ubi event
  // guid is mandatory, if guid is null, create a guid
  public String constructTrackingHeader(ContainerRequestContext requestContext, CookieReader cookieReader,
                                        String channelType) {
    String cookie = "";
    String rawGuid = cookieReader.getGuid(requestContext);
    if (!StringUtils.isEmpty(rawGuid))
      cookie += "guid=" + rawGuid.substring(0,Constants.GUID_LENGTH);
    else {
      try {
        cookie += "guid=" + new Guid().nextPaddedGUID();
      } catch (UnknownHostException e) {
        logger.warn("Create guid failure: ", e);
      }
      logger.warn("No guid");
      metrics.meter("NoGuid", 1, Field.of(Constants.CHANNEL_TYPE, channelType));
    }

    String rawCguid = cookieReader.getCguid(requestContext);
    if (!StringUtils.isEmpty(rawCguid))
      cookie += ",cguid=" + rawCguid.substring(0,Constants.CGUID_LENGTH);
    else {
      logger.warn("No cguid");
      metrics.meter("NoCguid", 1, Field.of(Constants.CHANNEL_TYPE, channelType));
    }

    return cookie;
  }

  /**
   * Parse rotation id from query mkrid
   */
  private long parseRotationId(MultiValueMap<String, String> parameters) {
    long rotationId = -1L;
    if (parameters.containsKey(Constants.MKRID) && parameters.get(Constants.MKRID).get(0) != null) {
      try {
        String rawRotationId = parameters.get(Constants.MKRID).get(0);
        rotationId = Long.valueOf(rawRotationId.replaceAll("-", ""));
      } catch (Exception e) {
        logger.warn(Errors.ERROR_INVALID_MKRID);
        metrics.meter("InvalidMkrid");
      }
    } else {
      logger.warn(Errors.ERROR_NO_MKRID);
      metrics.meter("NoMkrid");
    }

    return rotationId;
  }

}