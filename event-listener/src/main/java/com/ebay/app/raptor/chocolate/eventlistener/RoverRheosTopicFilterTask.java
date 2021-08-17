package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.avro.*;
import com.ebay.app.raptor.chocolate.common.ShortSnapshotId;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import com.ebay.app.raptor.chocolate.constant.Constants;
import com.ebay.app.raptor.chocolate.eventlistener.util.BehaviorKafkaSink;
import com.ebay.app.raptor.chocolate.eventlistener.util.PageIdEnum;
import com.ebay.app.raptor.chocolate.eventlistener.util.PageNameEnum;
import com.ebay.app.raptor.chocolate.eventlistener.util.RheosConsumerWrapper;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.collections.CollectionUtils;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import org.apache.commons.lang3.StringUtils;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Rover rheos topic filter.
 *
 * We only filter
 *
 * 1. missing clicks in epn case.
 * 2. roi events
 *
 * @author xiangli4
 */
public class RoverRheosTopicFilterTask extends Thread {

  private static final String APPLICATION_PAYLOAD = "applicationPayload";
  private static final String CLIENT_DATA = "clientData";
  private static final String INCOMING = "Incoming";
  private static final String INCOMING_PAGE_ROVER = "IncomingPageRover";
  private static final String INCOMING_MISSING_CLICKS = "IncomingMissingClicks";
  private static final String INCOMING_PAGE_ROI = "IncomingPageRoi";
  private static final long ONE_HOUR = 1000 * 60 * 60;
  private static final Metrics metrics = ESMetrics.getInstance();

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RoverRheosTopicFilterTask.class);
  private static Pattern missingRoverClicksPattern = Pattern.compile("^\\/rover\\/.*\\/.*\\/1\\?.*rvrhostname=.*",
    Pattern.CASE_INSENSITIVE);
  private static final Utf8 empty = new Utf8("");
  private static final Utf8 zero = new Utf8("0");
  private static Long interval = 0L;
  private static RoverRheosTopicFilterTask task = null;
  private static Boolean runFlag = true;
  private static String ROVER_HOST = "https://rover.ebay.com";

  /**
   * Singleton
   *
   * @param interval
   */
  private RoverRheosTopicFilterTask(Long interval) {
    this.interval = interval;
  }

  public static RoverRheosTopicFilterTask getInstance() {
    return task;
  }

  /**
   * Init the job with interval
   *
   * @param interval: interval in milliseconds to consume data from Rheos
   */
  public static void init(long interval) {
    task = new RoverRheosTopicFilterTask(interval);
    logger.info("Filter Task initialized with interval: " + interval);
  }

  /**
   * Simple coalesce to set default value for input value
   * in case of Null or empty
   *
   * @param a input value
   * @param b default value
   * @return Utf8 format
   */
  public static Utf8 coalesce(Utf8 a, Utf8 b) {
    if (null == a || a.length() == 0) return b;
    else return a;
  }

  /**
   * terminate
   */
  public static void terminate() {
    runFlag = false;
    try {
      Thread.sleep(interval * 3);
      System.out.println("Filter task terminated");
    } catch (InterruptedException e) {
      logger.error(e.getMessage());
    }
  }

  /**
   * Main method
   */
  @Override
  public void run() {
    RheosConsumerWrapper rheosConsumer = RheosConsumerWrapper.getInstance();
    Producer<Long, ListenerMessage> producer;
    producer = KafkaSink.get();
    Long round = 0L;

    while (runFlag) {
      round++;
      if ((round % 10000) == 1) logger.warn(String.format("Round %d from rheos", round));
      try {
        processRecords(rheosConsumer, producer);
      } catch (Exception e) {
        logger.error("Something wrong:", e);
      }
    }
  }

  /**
   * Set common fields for listener message
   * @param record listener message
   * @param applicationPayload rheos application payload
   */
  public void setCommonFields(ListenerMessage record, HashMap<Utf8, Utf8> applicationPayload, GenericRecord genericRecord,
                              String schemaVersion) {
    // user id
    record.setUserId(Long.valueOf(getField(genericRecord, "userId", "0")));

    // guid, cguid
    record.setGuid(coalesce(applicationPayload.get(new Utf8("g")), empty).toString());
    record.setCguid(coalesce(applicationPayload.get(new Utf8("n")), empty).toString());

    Map<Utf8, Utf8> clientData = ((HashMap<Utf8, Utf8>) genericRecord.get(CLIENT_DATA));

    // remote ip ,user agent
    String forwardedFor;
    String agent;
    String referer;

    if ("2".equals(schemaVersion)) {
        forwardedFor = clientData.getOrDefault(new Utf8("ForwardedFor"), new Utf8("")).toString();
        agent = clientData.getOrDefault(new Utf8("Agent"), new Utf8("")).toString();
        referer = getField(genericRecord, "referrer", "");
    } else {
        forwardedFor = coalesce(applicationPayload.get(new Utf8("ForwardedFor")), empty).toString();
        agent = coalesce(applicationPayload.get(new Utf8("Agent")), empty).toString();
        referer = coalesce(applicationPayload.get(new Utf8("Referer")), empty).toString();
    }
    record.setRemoteIp(forwardedFor);
    record.setUserAgent(agent);
    record.setReferer(referer);
    String eventTimestamp = getField(genericRecord, "eventTimestamp", "");
    long timestamp = Long.parseLong(eventTimestamp);
    record.setTimestamp(timestamp);

    // language code
    record.setLangCd(coalesce(applicationPayload.get(new Utf8("ul")), empty).toString());

    // geography identifier
    record.setGeoId(Long.valueOf(coalesce(applicationPayload.get(new Utf8("uc")), zero).toString()));

    // site id
    record.setSiteId(Long.valueOf(getField(genericRecord, "siteId", "0")));

    record.setUdid(coalesce(applicationPayload.get(new Utf8("udid")), empty).toString());

    //TODO: landing page url
    // landing page url
    record.setLandingPageUrl("");

    record.setSnid("");
    record.setIsTracked(false);

    // Format record
    record.setRequestHeaders("");
    record.setResponseHeaders("");

    // Get snapshotId from request
    Long snapshotId = SnapshotId.getNext(ApplicationOptions.getInstance().getDriverId(), timestamp)
        .getRepresentation();
    record.setSnapshotId(snapshotId);
    ShortSnapshotId shortSnapshotId = new ShortSnapshotId(record.getSnapshotId());
    record.setShortSnapshotId(shortSnapshotId.getRepresentation());
  }

  /**
   * Filter missing clicks and publish to filter kafka
   *
   * @param rheosConsumer Rheos used to consume
   * @param producer      Kafka producer
   */
  @SuppressWarnings("unchecked")
  public void processRecords(RheosConsumerWrapper rheosConsumer, Producer<Long, ListenerMessage> producer) {

    ConsumerRecords<byte[], RheosEvent> consumerRecords;
    consumerRecords = rheosConsumer.getConsumer().poll(Duration.ofMillis(interval));
    for (ConsumerRecord<byte[], RheosEvent> consumerRecord : consumerRecords) {
      String topic = consumerRecord.topic();
      Headers headers = consumerRecord.headers();
      String schemaVersion = "";
      if (headers != null) {
        for (Header header : headers) {
          if ("schemaVersion".equals(header.key())) {
            schemaVersion = new String(header.value());
          }
        }
      }
      logger.info("schemaVersion is {}", schemaVersion);

      ESMetrics.getInstance().meter(INCOMING);
      GenericRecord genericRecord = rheosConsumer.getDecoder().decode(consumerRecord.value());
      HashMap<Utf8, Utf8> data = ((HashMap<Utf8, Utf8>) genericRecord.get(APPLICATION_PAYLOAD));
      int pageId;
      if (genericRecord.get("pageId") != null) {
        pageId = (int) genericRecord.get("pageId");
      } else {
        continue;
      }

      //EPN
      if (pageId == 3084) {
        ESMetrics.getInstance().meter(INCOMING_PAGE_ROVER);
        String kafkaTopic = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(ChannelType.EPN);
        HashMap<Utf8, Utf8> applicationPayload = ((HashMap<Utf8, Utf8>) genericRecord.get(APPLICATION_PAYLOAD));

        // get urlQueryString from 3 places
        String urlQueryString = coalesce(applicationPayload.get(new Utf8("urlQueryString")), empty).toString();
        if (StringUtils.isEmpty(urlQueryString)) {
          urlQueryString = getField(genericRecord, "urlQueryString", "");
          if (StringUtils.isEmpty(urlQueryString)) {
            HashMap<Utf8, Utf8> clientData = ((HashMap<Utf8, Utf8>) genericRecord.get(CLIENT_DATA));
            urlQueryString = coalesce(clientData.get(new Utf8("urlQueryString")), empty).toString();
            if (!(StringUtils.isEmpty(urlQueryString))) {
              ESMetrics.getInstance().meter("UrlQueryStringFromClientData");
            }
          } else {
            ESMetrics.getInstance().meter("UrlQueryStringFromRheosTag");
          }
        } else {
          ESMetrics.getInstance().meter("UrlQueryStringFromApplicationPayload");
        }

        Matcher roverSitesMatcher = missingRoverClicksPattern.matcher(urlQueryString.toLowerCase());
        // match the missing clicks type, forward to filter
        if (roverSitesMatcher.find()) {
          if(urlQueryString.contains("5338380161")) {
            logger.info("Incoming5338380161: " + urlQueryString);
            ESMetrics.getInstance().meter("Incoming5338380161");
          }
          ESMetrics.getInstance().meter(INCOMING_MISSING_CLICKS);
          ListenerMessage record = new ListenerMessage(0L, 0L, 0L, 0L, "", "", "", "", "", 0L, "", "", -1L, -1L, 0L, "",
            0L, 0L, "", "", "", ChannelAction.CLICK, ChannelType.DEFAULT, HttpMethod.GET, "", false);

          setCommonFields(record, applicationPayload, genericRecord, schemaVersion);

          String uri = ROVER_HOST + urlQueryString;
          record.setUri(uri);
          // Set the channel type + HTTP headers + channel action
          record.setChannelType(ChannelType.EPN);
          record.setHttpMethod(HttpMethod.GET);
          record.setChannelAction(ChannelAction.CLICK);

          // Set campaign id
          UriComponents uriComponents;
          uriComponents = UriComponentsBuilder.fromUriString(uri).build();

          //get lowercase parameter in case some publishers may put different case of characters
          MultiValueMap<String, String> lowerCaseParams = new LinkedMultiValueMap<>();
          MultiValueMap<String, String> params = uriComponents.getQueryParams();
          for (Map.Entry<String, List<String>> entry : params.entrySet()) {
            lowerCaseParams.put(entry.getKey().toLowerCase(), entry.getValue());
          }

          // source and destination rotation id are parsed later in epn nrt
          record.setSrcRotationId(-1L);
          record.setDstRotationId(-1L);

          long campaignId = -1L;
          try{
            List<String> list = lowerCaseParams.get("campid");
            if (list == null) {
              throw new IllegalArgumentException("no campid");
            }
            String first = list.get(0);
            if (first == null) {
              throw new IllegalArgumentException("no campid");
            }
            campaignId = Long.parseLong(first);
            if(campaignId == 5338380161l) {
              logger.info("Success5338380161: " + uri);
              ESMetrics.getInstance().meter("Success5338380161");
            }
          } catch (Exception e) {
            logger.error("Parse campaign id error");
          }
          record.setCampaignId(campaignId);
          record.setPublisherId(-1L);

          // get landingPageUrl from applicationPayload.url_mpre
          String landingPageUrl = coalesce(applicationPayload.get(new Utf8("url_mpre")), empty).toString();
          try {
            if(landingPageUrl.startsWith("https%3A%2F%2F") || landingPageUrl.startsWith("http%3A%2F%2F")) {
              landingPageUrl = URLDecoder.decode(landingPageUrl, "UTF-8");
            }
          } catch (Exception ex) {
            ESMetrics.getInstance().meter("DecodeEpnMobileClicksLandingPageUrlError");
            logger.warn("Decode EPN Mobile clicks landing page url error");
          }
          record.setLandingPageUrl(landingPageUrl);

          producer.send(new ProducerRecord<>(kafkaTopic, record.getSnapshotId(), record), KafkaSink.callback);
        }
      } else if (pageId == 3086) {
        ESMetrics.getInstance().meter(INCOMING_PAGE_ROI);
        String kafkaTopic = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(ChannelType.ROI);
        HashMap<Utf8, Utf8> applicationPayload = ((HashMap<Utf8, Utf8>) genericRecord.get(APPLICATION_PAYLOAD));
        String urlQueryString;
        if ("2".equals(schemaVersion)) {
          urlQueryString = getField(genericRecord, "urlQueryString", StringUtils.EMPTY);
        } else {
            urlQueryString = coalesce(applicationPayload.get(new Utf8("urlQueryString")), empty).toString();
        }
        try {
          urlQueryString = URLDecoder.decode(urlQueryString, "UTF-8");
        } catch (Exception ex) {
          ESMetrics.getInstance().meter("DecodeROIUrlError");
          logger.warn("Decode ROI url error");
        }

        ListenerMessage record = new ListenerMessage(0L, 0L, 0L, 0L, "", "", "", "", "", 0L, "", "", -1L, -1L, 0L, "",
            0L, 0L, "", "", "", ChannelAction.ROI, ChannelType.ROI, HttpMethod.GET, "", false);

        setCommonFields(record, applicationPayload, genericRecord, schemaVersion);

        // TODO: Remove this logic after release and everything stable
        // set short snapshot id to be from Rheos event so that when inserting into TD, it can be deduped by primary index
        String rvrIdStr = coalesce(applicationPayload.get(new Utf8("rvrid")), empty).toString();
        if (StringUtils.isNumeric(rvrIdStr)) {
          record.setShortSnapshotId(Long.valueOf(rvrIdStr));
        }

        String uri = ROVER_HOST + urlQueryString;
        record.setUri(uri);

        record.setHttpMethod(HttpMethod.GET);

        // source and destination rotation id parse for roi
        Long rotationId = urlQueryString.split("/").length > 3 ? Long.valueOf(urlQueryString.split("/")[3].split("\\?")[0].replace("-", "")) : 0l;
        record.setSrcRotationId(rotationId);
        record.setDstRotationId(rotationId);

        record.setCampaignId(-1L);
        record.setPublisherId(-1L);

        long currentTimestamp = System.currentTimeMillis();
        long eventTimestamp = record.getTimestamp();
        long rheosCreateTimestamp = consumerRecord.value().getEventCreateTimestamp();
        long rheosSentTimestamp = consumerRecord.value().getEventSentTimestamp();

        String metricMessageLatency = "MCSConsumePulsarMessageLatency";
        String metricRheosCreateLatency = "MCSConsumePulsarRheosCreateLatency";
        String metricRheosSentLatency = "MCSConsumePulsarRheosSentLatency";
        String metricMessageLatencyCritical = "MCSConsumePulsarMessageLatencyCritical";
        String metricRheosCreateLatencyCritical = "MCSConsumePulsarRheosCreateLatencyCritical";
        String metricRheosSentLatencyCritical = "MCSConsumePulsarRheosSentLatencyCritical";

        // record roi latency in mcs
        long latencyOfMessage = currentTimestamp - eventTimestamp;
        // rheos event create timestamp diff
        long latencyOfRheosCreateTimestamp = currentTimestamp - rheosCreateTimestamp;
        // rheos event sent timestamp diff
        long latencyOfRheosSentTimestamp = currentTimestamp - rheosSentTimestamp;
        // rheos internal timestamps
        String rheosInternalTimestamps = coalesce(applicationPayload.get(new Utf8("rheosTimestamps")), empty).toString();
        metrics.meter(metricMessageLatency, latencyOfMessage, Field.of("channelType",
            record.getChannelType().toString()));
        metrics.meter(metricRheosCreateLatency, latencyOfRheosCreateTimestamp, Field.of("channelType",
            record.getChannelType().toString()));
        metrics.meter(metricRheosSentLatency, latencyOfRheosSentTimestamp, Field.of("channelType",
            record.getChannelType().toString()));
        // if latency is larger than 1 hour log specifically to another metric
        String delayLogFormat = "snapshort_id=%d, short_snapshort_id=%d, current_ts=%d, event_ts=%d, rheos_create_ts=%d, rheos_sent_ts=%d, rheos_internal_ts=%s";
        if(latencyOfMessage > ONE_HOUR) {
          metrics.meter(metricMessageLatencyCritical, latencyOfMessage, Field.of("channelType",
              record.getChannelType().toString()));
          logger.warn(String.format(metricMessageLatencyCritical + ": " + delayLogFormat,
              record.getSnapshotId(),
              record.getShortSnapshotId(),
              currentTimestamp,
              eventTimestamp,
              rheosCreateTimestamp,
              rheosSentTimestamp,
              rheosInternalTimestamps));
        }
        if(latencyOfRheosCreateTimestamp > ONE_HOUR) {
          metrics.meter(metricRheosCreateLatencyCritical, latencyOfRheosCreateTimestamp, Field.of("channelType",
              record.getChannelType().toString()));
          logger.warn(String.format(metricRheosCreateLatencyCritical + ": " + delayLogFormat,
              record.getSnapshotId(),
              record.getShortSnapshotId(),
              currentTimestamp,
              eventTimestamp,
              rheosCreateTimestamp,
              rheosSentTimestamp,
              rheosInternalTimestamps));
        }
        if(latencyOfRheosSentTimestamp > ONE_HOUR) {
          metrics.meter(metricRheosSentLatencyCritical, latencyOfRheosSentTimestamp, Field.of("channelType",
              record.getChannelType().toString()));
          logger.warn(String.format(metricRheosSentLatencyCritical + ": " + delayLogFormat,
              record.getSnapshotId(),
              record.getShortSnapshotId(),
              currentTimestamp,
              eventTimestamp,
              rheosCreateTimestamp,
              rheosSentTimestamp,
              rheosInternalTimestamps));
        }
        producer.send(new ProducerRecord<>(kafkaTopic, record.getSnapshotId(), record), KafkaSink.callback);
      }
    }
  }

  protected Map<String, String> convertMap(Map<Utf8, Utf8> map) {
    Map<String, String> target = new HashMap<>();
    map.forEach((k, v) -> {
      target.put(String.valueOf(k), String.valueOf(v));
    });
    return target;
  }

  @SuppressWarnings("unchecked")
  protected ChannelIdEnum parseChannelType(GenericRecord genericRecord) {
    HashMap<Utf8, Utf8> applicationPayload = ((HashMap<Utf8, Utf8>) genericRecord.get(APPLICATION_PAYLOAD));
    if (applicationPayload.containsKey(new Utf8("chnl"))) {
      switch (String.valueOf(applicationPayload.get(new Utf8("chnl")))) {
        case "7":
          return ChannelIdEnum.SITE_EMAIL;
        case "8":
          return ChannelIdEnum.MRKT_EMAIL;
        case "26":
          return ChannelIdEnum.SITE_MESSAGE_CENTER;
        case "27":
          return ChannelIdEnum.MRKT_MESSAGE_CENTER;
        default:
          return null;
      }
    }

    Utf8 urlQueryString = (Utf8) genericRecord.get("urlQueryString");
    if (urlQueryString == null) {
      return null;
    }

    return parseChannelType(urlQueryString);
  }

  @Nullable
  protected ChannelIdEnum parseChannelType(Utf8 urlQueryString) {
    List<String> strings = URLEncodedUtils.parsePathSegments(String.valueOf(urlQueryString), StandardCharsets.UTF_8);
    if (CollectionUtils.isEmpty(strings)) {
      return null;
    }
    String lastElem = strings.get(strings.size() - 1);
    if (!lastElem.contains("?")) {
      return null;
    }
    return ChannelIdEnum.parse(lastElem.substring(0, lastElem.indexOf("?")));
  }


  /**
   * Get field if exist
   *
   * @param genericRecord generic record
   * @param key           key to get
   * @return
   */
  public String getField(GenericRecord genericRecord, String key, String defaultValue) {
    return genericRecord.get(key) == null ? defaultValue : genericRecord.get(key).toString();
  }
}
