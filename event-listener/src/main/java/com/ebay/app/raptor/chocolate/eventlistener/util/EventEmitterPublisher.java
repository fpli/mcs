package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.platform.raptor.cosadaptor.exceptions.TokenCreationException;
import com.ebay.platform.raptor.cosadaptor.token.ISecureTokenManager;
import com.ebay.raptor.domain.request.api.DomainRequestData;
import com.ebay.raptorio.env.PlatformEnvProperties;
import com.ebay.raptorio.request.tracing.RequestTracingContext;
import com.ebay.traffic.dsptchtrk.emitter.Environment;
import com.ebay.traffic.dsptchtrk.emitter.EventEmitter;
import com.ebay.traffic.dsptchtrk.emitter.EventEmitterFactory;
import com.ebay.traffic.dsptchtrk.emitter.QueueFullException;
import com.ebay.traffic.elements.messagetracker.common.*;
import com.ebay.traffic.elements.messagetracker.dataobject.MessageEvent;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;

import javax.ws.rs.container.ContainerRequestContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jialili1 on 8/12/20
 */
public class EventEmitterPublisher {
  private static final Logger logger = LoggerFactory.getLogger(EventEmitterPublisher.class);
  private Metrics metrics = ESMetrics.getInstance();

  private EventEmitter emitter;
  private ISecureTokenManager tokenGenerator;

  public EventEmitterPublisher(PlatformEnvProperties platformEnvProperties, ISecureTokenManager tokenGenerator)
      throws Exception {
    this.emitter = EventEmitterFactory.getInstance(getEnv(platformEnvProperties));
    this.tokenGenerator = tokenGenerator;
  }

  /**
   * Construct and send events to message tracker
   */
  public void sendToMessageTracker(ContainerRequestContext requestContext, MultiValueMap<String, String> parameters,
                                    ChannelAction channelAction) throws TokenCreationException {
    MessageEvent messageEvent = new MessageEvent();

    RequestTracingContext tracingContext = (RequestTracingContext) requestContext.getProperty(RequestTracingContext.NAME);
    DomainRequestData domainRequest = (DomainRequestData) requestContext.getProperty(DomainRequestData.NAME);

    messageEvent.setDispatchId("");
    messageEvent.setRlogId(tracingContext.getRlogId());
    messageEvent.setService("Chocolate");
    messageEvent.setServiceHost(domainRequest.getHost());

    List<Map<String, String>> data = new ArrayList<>();
    Map<String, String> emailData = new HashMap<>();

    // interaction type
    if (ChannelAction.CLICK.equals(channelAction)) {
      emailData.put(MessageConstantsEnum.INTERACTION_TYPE.getValue(), InteractionTypeEnum.CLICK.getValue());
    } else if (ChannelAction.EMAIL_OPEN.equals(channelAction)) {
      emailData.put(MessageConstantsEnum.INTERACTION_TYPE.getValue(), InteractionTypeEnum.OPEN.getValue());
    }

    // channel
    emailData.put(MessageConstantsEnum.CHANNEL_NAME.getValue(), ChannelEnum.EMAIL.getValue());

    // site id
    emailData.put(MessageConstantsEnum.SITE_ID.getValue(), String.valueOf(domainRequest.getSiteId()));

    // user id
    String userId;
    if (parameters.containsKey("bu") && parameters.getFirst("bu") != null) {
      userId = parameters.getFirst("bu");
      emailData.put(MessageConstantsEnum.ENCRYPTED_USER_ID.getValue(), userId);
    }

    // tags from parameters
    addEmailTag(emailData, parameters, MessageConstantsEnum.SEGMENT.getValue(), "segname");
    addEmailTag(emailData, parameters, MessageConstantsEnum.RUN_DATE.getValue(), "crd");


    emailData.put(MessageConstantsEnum.STATUS.getValue(), EventTypeEnum.SENT.getValue());
    emailData.put(MessageConstantsEnum.TEMPLATE_ID.getValue(), null);

    data.add(emailData);
    messageEvent.setData(data);

    // spock token
    String authToken = tokenGenerator.getToken().getAccessToken();

    // send event
    try {
      emitter.emit(messageEvent, authToken);
    } catch(QueueFullException e) {
      logger.error("Event emitter sending error", e);
    }

  }

  /**
   * add email tags
   */
  private void addEmailTag(Map<String, String> emailData, MultiValueMap<String, String> parameters, String key,
                           String param) {
    if (parameters.containsKey(param) && parameters.getFirst(param) != null) {
      emailData.put(key, parameters.getFirst(param));
    }
  }

  /**
   * Get environment for event emitter
   */
  private static Environment getEnv(PlatformEnvProperties platformEnvProperties) throws Exception{
    String env = platformEnvProperties.getPlatformEnvironment().toLowerCase();
    logger.info("Platform Environment: {}", env);

    Environment environment;
    switch (env) {
      case "dev":
        environment = Environment.DEV;
        break;
      case "staging":
        environment = Environment.STAGING;
        break;
      case "pre-production":
        environment = Environment.PRE_PROD;
        break;
      case "production":
        environment = Environment.PROD;
        break;
      default:
        throw new Exception("No matched environment");
    }

    return environment;
  }

}
