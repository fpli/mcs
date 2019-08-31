package com.ebay.app.raptor.chocolate.eventlistener;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.HttpMethod;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.ShortSnapshotId;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.app.raptor.chocolate.eventlistener.util.RheosConsumerWrapper;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.monitoring.ESMetrics;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URLDecoder;
import java.util.HashMap;
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

  private static final String INCOMING = "Incoming";
  private static final String INCOMING_PAGE_ROVER = "IncomingPageRover";
  private static final String INCOMING_MISSING_CLICKS = "IncomingMissingClicks";
  private static final String INCOMING_PAGE_ROI = "IncomingPageRoi";


  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RoverRheosTopicFilterTask.class);
  private static Pattern missingRoverClicksPattern = Pattern.compile("^\\/rover\\/.*\\/.*\\/1\\?.*rvrhostname=.*",
    Pattern.CASE_INSENSITIVE);
  private static final Utf8 empty = new Utf8("");
  private static final Utf8 zero = new Utf8("0");
  private static Long interval = 0L;
  private static RoverRheosTopicFilterTask task = null;
  private static Boolean runFlag = true;

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
    String kafkaTopic;
    Long round = 0L;

    while (runFlag) {
      round++;
      if ((round % 10000) == 1) logger.warn(String.format("Round %d from rheos", round));
      try {
        processRecords(rheosConsumer, producer);
      } catch (Throwable e) {
        logger.error("Something wrong:", e);
      }
    }
  }

  /**
   * Set common fields for listener message
   * @param record listener message
   * @param applicationPayload rheos application payload
   */
  public void setCommonFields(ListenerMessage record, HashMap<Utf8, Utf8> applicationPayload, GenericRecord genericRecord) {
    // user id
    record.setUserId(Long.valueOf(getField(genericRecord, "userId", "0")));

    // guid, cguid
    record.setGuid(coalesce(applicationPayload.get(new Utf8("g")), empty).toString());
    record.setCguid(coalesce(applicationPayload.get(new Utf8("n")), empty).toString());

    // remote ip
    record.setRemoteIp(coalesce(applicationPayload.get(new Utf8("ForwardedFor")), empty).toString());

    // user agent
    record.setUserAgent(coalesce(applicationPayload.get(new Utf8("Agent")), empty).toString());


    // language code
    record.setLangCd(coalesce(applicationPayload.get(new Utf8("ul")), empty).toString());

    // geography identifier
    record.setGeoId(Long.valueOf(coalesce(applicationPayload.get(new Utf8("uc")), zero).toString()));

    // site id
    record.setSiteId(Long.valueOf(getField(genericRecord, "siteId", "0")));


    record.setUdid(coalesce(applicationPayload.get(new Utf8("udid")), empty).toString());

    //TODO: no referer in rheos
    // referer
    record.setReferer(coalesce(applicationPayload.get(new Utf8("Referer")), empty).toString());

    //TODO: landing page url
    // landing page url
    record.setLandingPageUrl("");

    record.setSnid("");
    record.setIsTracked(false);

    // Format record
    record.setRequestHeaders("");
    record.setResponseHeaders("");
    long timestamp = Long.valueOf(coalesce(applicationPayload.get(new Utf8("timestamp")), empty).toString());
    record.setTimestamp(timestamp);

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
  public void processRecords(RheosConsumerWrapper rheosConsumer, Producer<Long, ListenerMessage> producer) {

    ConsumerRecords<byte[], RheosEvent> consumerRecords;
    consumerRecords = rheosConsumer.getConsumer().poll(interval);
    for (ConsumerRecord<byte[], RheosEvent> consumerRecord : consumerRecords) {
      ESMetrics.getInstance().meter(INCOMING);
      GenericRecord genericRecord = rheosConsumer.getDecoder().decode(consumerRecord.value());
      HashMap<Utf8, Utf8> data = ((HashMap<Utf8, Utf8>) genericRecord.get(APPLICATION_PAYLOAD));
      int pageId = -1;
      if (genericRecord.get("pageId") != null) {
        pageId = (int) genericRecord.get("pageId");
      } else {
        continue;
      }

      if (pageId == 3084) {
        ESMetrics.getInstance().meter(INCOMING_PAGE_ROVER);
        String kafkaTopic = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(ChannelType.EPN);
        HashMap<Utf8, Utf8> applicationPayload = ((HashMap<Utf8, Utf8>) genericRecord.get(APPLICATION_PAYLOAD));
        String urlQueryString = coalesce(applicationPayload.get(new Utf8("urlQueryString")), empty).toString();
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

          setCommonFields(record, applicationPayload, genericRecord);

          String uri = "https://rover.ebay.com" + urlQueryString;
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
          for (String key: params.keySet()) {
            lowerCaseParams.put(key.toLowerCase(), params.get(key));
          }

          // source and destination rotation id are parsed later in epn nrt
          record.setSrcRotationId(-1L);
          record.setDstRotationId(-1L);

          long campaignId = -1L;
          try{
            campaignId = Long.valueOf(lowerCaseParams.get("campid").get(0));
            if(campaignId == 5338380161l) {
              logger.info("Success5338380161: " + uri);
              ESMetrics.getInstance().meter("Success5338380161");
            }
          } catch (Exception e) {
            logger.error("Parse campaign id error");
          }
          record.setCampaignId(campaignId);
          record.setPublisherId(-1L);

          producer.send(new ProducerRecord<>(kafkaTopic, record.getSnapshotId(), record), KafkaSink.callback);
        }
      } else if(pageId == 3086) {
        ESMetrics.getInstance().meter(INCOMING_PAGE_ROI);
        String kafkaTopic = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(ChannelType.ROI);
        HashMap<Utf8, Utf8> applicationPayload = ((HashMap<Utf8, Utf8>) genericRecord.get(APPLICATION_PAYLOAD));
        String urlQueryString = coalesce(applicationPayload.get(new Utf8("urlQueryString")), empty).toString();
        try {
          urlQueryString = URLDecoder.decode(urlQueryString, "UTF-8");
        } catch (Exception ex) {
          ESMetrics.getInstance().meter("DecodeROIUrlError");
          logger.warn("Decode ROI url error");
        }

        ListenerMessage record = new ListenerMessage(0L, 0L, 0L, 0L, "", "", "", "", "", 0L, "", "", -1L, -1L, 0L, "",
            0L, 0L, "", "", "", ChannelAction.ROI, ChannelType.ROI, HttpMethod.GET, "", false);

        setCommonFields(record, applicationPayload, genericRecord);

        String uri = "https://rover.ebay.com" + urlQueryString;
        record.setUri(uri);

        record.setHttpMethod(HttpMethod.GET);

        // source and destination rotation id parse for roi
        Long rotationId = urlQueryString.split("/").length > 3 ? Long.valueOf(urlQueryString.split("/")[3].split("\\?")[0].replace("-", "")) : 0l;
        record.setSrcRotationId(rotationId);
        record.setDstRotationId(rotationId);

        record.setCampaignId(-1L);
        record.setPublisherId(-1L);
        producer.send(new ProducerRecord<>(kafkaTopic, record.getSnapshotId(), record), KafkaSink.callback);
      }
    }
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
