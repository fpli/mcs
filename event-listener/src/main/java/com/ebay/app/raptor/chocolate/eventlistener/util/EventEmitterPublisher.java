package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.kernel.presentation.constants.PresentationConstants;
import com.ebay.platform.raptor.cosadaptor.exceptions.TokenCreationException;
import com.ebay.platform.raptor.cosadaptor.token.ISecureTokenManager;
import com.ebay.raptor.domain.request.api.DomainRequestData;
import com.ebay.raptor.geo.context.UserPrefsCtx;
import com.ebay.raptor.kernel.util.RaptorConstants;
import com.ebay.raptorio.request.tracing.RequestTracingContext;
import com.ebay.traffic.dsptchtrk.emitter.Environment;
import com.ebay.traffic.dsptchtrk.emitter.EventEmitter;
import com.ebay.traffic.dsptchtrk.emitter.EventEmitterFactory;
import com.ebay.traffic.dsptchtrk.emitter.QueueFullException;
import com.ebay.traffic.elements.messagetracker.common.*;
import com.ebay.traffic.elements.messagetracker.dataobject.MessageEvent;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import javax.ws.rs.container.ContainerRequestContext;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;

/**
 * Created by jialili1 on 8/12/20
 */
public class EventEmitterPublisher {
  private static final Logger logger = LoggerFactory.getLogger(EventEmitterPublisher.class);
  private Metrics metrics = ESMetrics.getInstance();

  private EventEmitter emitter;
  private ISecureTokenManager tokenGenerator;

  private static final String APP_NAME = "mktcollectionsvc";
  private static final String CLIENT_SCOPE = "https://api.ebay.com/oauth/scope/marketing@application";

  public EventEmitterPublisher(ISecureTokenManager tokenGenerator) throws Exception {
    this.emitter = EventEmitterFactory.getInstance(getEnv());
    this.tokenGenerator = tokenGenerator;
  }

  /**
   * Construct and send events to message tracker
   */
  public void publishEvent(ContainerRequestContext requestContext, MultiValueMap<String, String> parameters,
                           String targetUrl, ChannelType channelType, ChannelAction channelAction, Long snapshotId) {
    MessageEvent messageEvent = new MessageEvent();

    RequestTracingContext tracingContext = (RequestTracingContext) requestContext.getProperty(RequestTracingContext.NAME);
    DomainRequestData domainRequest = (DomainRequestData) requestContext.getProperty(DomainRequestData.NAME);

    messageEvent.setDispatchId(String.valueOf(snapshotId));
    messageEvent.setRlogId(tracingContext.getRlogId());
    messageEvent.setService("CHOCOLATE");
    messageEvent.setServiceHost(domainRequest.getHost());
    messageEvent.setUpdated(System.currentTimeMillis());

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

    // add other tags
    emailData = addTags(emailData, parameters, requestContext, targetUrl, channelType, channelAction);

    data.add(emailData);
    messageEvent.setData(data);

    // generate token for spock
    String authToken = "";
    try {
      authToken = tokenGenerator.getPublicAppToken(APP_NAME, CLIENT_SCOPE).getAccessToken();
    } catch (TokenCreationException e) {
      logger.error("Auth token generation for spock failed", e);
      metrics.meter("TokenGenerationFail");
    }

    // send event
    try {
      if (!StringUtils.isEmpty(authToken)) {
        emitter.emit(messageEvent, authToken);
        metrics.meter("EventEmitterPublishSuccess");
      }
    } catch(QueueFullException e) {
      logger.error("Event emitter sending error", e);
      metrics.meter("EventEmitterQueueFull");
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
  private static Environment getEnv() throws Exception{
    String env = ApplicationOptions.getInstance().getEnvironment();
    logger.info("Platform Environment: {}", env);

    Environment environment;
    switch (env) {
      case "dev":
        environment = Environment.DEV;
        break;
      case "qa":
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

  /**
   * Get application payload
   */
  private Map<String, String> addTags(Map<String, String> emailData, MultiValueMap<String, String> parameters,
                                                    ContainerRequestContext requestContext, String targetUrl,
                                                    ChannelType channelType, ChannelAction channelAction) {
    for (Map.Entry<String, String> entry : Constants.emailTagParamMap.entries()) {
      addEmailTag(emailData, parameters, entry.getKey(), entry.getValue());
    }

    // add tags in url param "sojTags" into applicationPayload
    addSojTags(emailData, parameters, channelType, channelAction);

    // add other tags
    if (ChannelAction.CLICK.equals(channelAction)) {
      try {
        emailData.put("url_mpre", URLEncoder.encode(targetUrl, "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        logger.warn("Tag url_mpre encoding failed", e);
        metrics.meter("UrlMpreEncodeError", 1, Field.of(Constants.CHANNEL_ACTION, channelAction.toString()),
            Field.of(Constants.CHANNEL_TYPE, channelType.toString()));
      }
    }
    // buyer access site id
    UserPrefsCtx userPrefsCtx = (UserPrefsCtx) requestContext.getProperty(RaptorConstants.USERPREFS_CONTEXT_KEY);
    emailData.put("bs", String.valueOf(userPrefsCtx.getGeoContext().getSiteId()));

    return deleteNullOrEmptyValue(emailData);
  }

  /**
   * Add tags in param sojTags
   */
  private void addSojTags(Map<String, String> emailData, MultiValueMap<String, String> parameters,
                          ChannelType channelType, ChannelAction channelAction) {
    if(parameters.containsKey(Constants.SOJ_TAGS) && parameters.get(Constants.SOJ_TAGS).get(0) != null) {
      String sojTags = parameters.get(Constants.SOJ_TAGS).get(0);
      try {
        sojTags = URLDecoder.decode(sojTags, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        logger.warn("Param sojTags is wrongly encoded", e);
        metrics.meter("ErrorEncodedSojTags", 1, Field.of(Constants.CHANNEL_ACTION, channelAction.toString()),
            Field.of(Constants.CHANNEL_TYPE, channelType.toString()));
      }
      if (!StringUtils.isEmpty(sojTags)) {
        StringTokenizer stToken = new StringTokenizer(sojTags, PresentationConstants.COMMA);
        while (stToken.hasMoreTokens()) {
          StringTokenizer sojNvp = new StringTokenizer(stToken.nextToken(), PresentationConstants.EQUALS);
          if (sojNvp.countTokens() == 2) {
            String sojTag = sojNvp.nextToken().trim();
            String urlParam = sojNvp.nextToken().trim();
            if (!StringUtils.isEmpty(urlParam) && !StringUtils.isEmpty(sojTag)) {
              addEmailTag(emailData, parameters, sojTag, urlParam);
            }
          }
        }
      }
    }
  }

  /**
   * Delete map entry with null or empty value
   */
  private Map<String, String> deleteNullOrEmptyValue(Map<String, String> map) {
    Set<Map.Entry<String, String>> entrySet = map.entrySet();
    Iterator<Map.Entry<String, String>> iterator = entrySet.iterator();

    while(iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      if (StringUtils.isEmpty(entry.getValue())) {
        iterator.remove();
      }
    }

    return map;
  }

}
