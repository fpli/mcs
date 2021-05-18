/*
 * Copyright (c) 2021. eBay inc. All rights reserved.
 */

package com.ebay.app.raptor.chocolate.eventlistener.collector;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.app.raptor.chocolate.eventlistener.model.BaseEvent;
import com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil;
import com.ebay.app.raptor.chocolate.eventlistener.util.PageIdEnum;
import com.ebay.app.raptor.chocolate.gen.model.ROIEvent;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.raptor.auth.RaptorSecureContext;
import com.ebay.raptor.geo.context.UserPrefsCtx;
import com.ebay.raptor.kernel.util.RaptorConstants;
import com.ebay.tracking.api.IRequestScopeTracker;
import com.ebay.tracking.util.TrackerTagValueUtil;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import com.google.common.primitives.Longs;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;

import java.util.Map;
import java.util.UUID;

import static com.ebay.app.raptor.chocolate.constant.Constants.CHANNEL_ACTION;
import static com.ebay.app.raptor.chocolate.constant.Constants.CHANNEL_TYPE;
import static com.ebay.app.raptor.chocolate.eventlistener.util.CollectionServiceUtil.isLongNumeric;

@Component
@DependsOn("EventListenerService")
public class ROICollector {
  private static final Logger LOGGER = LoggerFactory.getLogger(ROICollector.class);

  private Metrics metrics;

  @PostConstruct
  public void postInit() {
    this.metrics = ESMetrics.getInstance();
  }

  /**
   * Set item id from ROIEvent
   * @param roiEvent roi event
   */
  public void setItemId(@NotNull ROIEvent roiEvent) {
    if(roiEvent.getItemId() == null) {
      roiEvent.setItemId("");
      LOGGER.warn("Error item id null");
      metrics.meter("ErrorNewROIParam", 1, Field.of(CHANNEL_ACTION, "New-ROI"), Field.of(CHANNEL_TYPE, "New-ROI"));
    } else {
      Long itemId = Longs.tryParse(roiEvent.getItemId());
      if (itemId == null || itemId < 0) {
        roiEvent.setItemId("");
        LOGGER.warn("Error itemId " + itemId);
        metrics.meter("ErrorNewROIParam", 1, Field.of(CHANNEL_ACTION, "New-ROI"), Field.of(CHANNEL_TYPE, "New-ROI"));
      }
    }
  }

  public void setTransTimestamp(@NotNull ROIEvent roiEvent) {
    if(roiEvent.getTransactionTimestamp() == null) {
      roiEvent.setTransactionTimestamp( Long.toString(System.currentTimeMillis()));
      LOGGER.warn("Error timestamp null");
      metrics.meter("ErrorNewROIParam", 1, Field.of(CHANNEL_ACTION, "New-ROI"), Field.of(CHANNEL_TYPE, "New-ROI"));
    }
    else {
      Long transTimestamp = Longs.tryParse(roiEvent.getTransactionTimestamp());
      if (transTimestamp == null || transTimestamp <= 0) {
        roiEvent.setTransactionTimestamp( Long.toString(System.currentTimeMillis()));
        LOGGER.warn("Error timestamp " + transTimestamp);
        metrics.meter("ErrorNewROIParam", 1, Field.of(CHANNEL_ACTION, "New-ROI"), Field.of(CHANNEL_TYPE, "New-ROI"));
      }
    }
  }

  public void setTransId(ROIEvent roiEvent) {
    if(roiEvent.getUniqueTransactionId() == null) {
      roiEvent.setUniqueTransactionId("");
      LOGGER.warn("Error transactionId null");
      metrics.meter("ErrorNewROIParam", 1, Field.of(CHANNEL_ACTION, "New-ROI"), Field.of(CHANNEL_TYPE, "New-ROI"));
    }
    Long transId = Longs.tryParse(roiEvent.getUniqueTransactionId());
    if(transId == null || transId < 0) {
      roiEvent.setUniqueTransactionId("");
      LOGGER.warn("Error transactionId " + transId);
      metrics.meter("ErrorNewROIParam", 1, Field.of(CHANNEL_ACTION, "New-ROI"), Field.of(CHANNEL_TYPE, "New-ROI"));
    }
  }

  /**
   * Add roi sjo tags
   * @param requestContext  request context
   * @param baseEvent       base event
   */
  public void trackUbi(ContainerRequestContext requestContext, BaseEvent baseEvent) {
    try {
      // Ubi tracking
      IRequestScopeTracker requestTracker =
          (IRequestScopeTracker) requestContext.getProperty(IRequestScopeTracker.NAME);

      // page id
      requestTracker.addTag(TrackerTagValueUtil.PageIdTag, PageIdEnum.ROI.getId(), Integer.class);

      // site ID is embedded in IRequestScopeTracker default commit tags

      // Item ID
      if(isLongNumeric(baseEvent.getRoiEvent().getItemId())) {
        requestTracker.addTag("itm", baseEvent.getRoiEvent().getItemId(), String.class);
      }

      // Transation Type
      if (!StringUtils.isEmpty(baseEvent.getRoiEvent().getTransType())) {
        requestTracker.addTag("tt", baseEvent.getRoiEvent().getTransType(), String.class);
      }

      // Transation ID
      if (isLongNumeric(baseEvent.getRoiEvent().getUniqueTransactionId())) {
        requestTracker.addTag("roi_bti", baseEvent.getRoiEvent().getUniqueTransactionId(), String.class);
      }

      // user ID
      if (isLongNumeric(baseEvent.getUid())) {
        requestTracker.addTag("userid", baseEvent.getUid(), String.class);
      }

      // Transaction Time
      if (isLongNumeric(baseEvent.getRoiEvent().getTransactionTimestamp())) {
        requestTracker.addTag("producereventts", Long.parseLong(baseEvent.getRoiEvent().getTransactionTimestamp()),
            Long.class);
      }
    } catch (Exception e) {
      LOGGER.warn("Error when tracking ubi for roi event", e);
      metrics.meter("ErrorWriteRoiEventToUBI");
    }
  }
}
