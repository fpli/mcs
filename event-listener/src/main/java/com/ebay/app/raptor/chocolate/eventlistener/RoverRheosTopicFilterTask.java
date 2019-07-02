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
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Rover rheos topic filter.
 *
 * We only filter missing clicks in epn case.
 *
 * @author xiangli4
 */
public class RoverRheosTopicFilterTask extends Thread {

  protected static final String APPLICATION_PAYLOAD = "applicationPayload";

  protected static final String INCOMING = "Incoming";
  protected static final String INCOMING_PAGE_ROVER = "IncomingPageRover";
  protected static final String INCOMING_MISSING_CLICKS = "IncomingMissingClicks";

  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RoverRheosTopicFilterTask.class);
  private static Pattern missingRoverClicksPattern = Pattern.compile("^\\/rover\\/.*\\/.*\\/1\\?.*rvrhostname=.*",
    Pattern.CASE_INSENSITIVE);
  protected static final Utf8 empty = new Utf8("");
  protected static final Utf8 zero = new Utf8("0");

  protected static Map<String, String> defaultApplicationPayload = new HashMap<>();
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
    kafkaTopic = ApplicationOptions.getInstance().getSinkKafkaConfigs().get(ChannelType.EPN);
    Long round = 0L;

    while (runFlag) {
      round++;
      if ((round % 10000) == 1) logger.warn(String.format("Round %d from rheos", round));
      try {
        processRecords(rheosConsumer, producer, kafkaTopic);
      } catch (Throwable e) {
        logger.error("Something wrong:", e);
      }
    }
  }

  /**
   * Filter missing clicks and publish to filter kafka
   *
   * @param rheosConsumer Rheos used to consume
   * @param producer      Kafka producer
   */
  public void processRecords(RheosConsumerWrapper rheosConsumer, Producer<Long, ListenerMessage> producer, String
    kafkaTopic) {

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
        HashMap<Utf8, Utf8> applicationPayload = ((HashMap<Utf8, Utf8>) genericRecord.get(APPLICATION_PAYLOAD));
        String urlQueryString = coalesce(applicationPayload.get(new Utf8("urlQueryString")), empty).toString();
        Matcher roverSitesMatcher = missingRoverClicksPattern.matcher(urlQueryString.toLowerCase());
        // match the missing clicks type, forward to filter
        if (roverSitesMatcher.find()) {
          ESMetrics.getInstance().meter(INCOMING_MISSING_CLICKS);
          ListenerMessage record = new ListenerMessage(0L, 0L, 0L, 0L, "", "", "", "", "", 0L, "", "", -1L, -1L, 0L, "",
            0L, 0L, "", "", "", ChannelAction.CLICK, ChannelType.DEFAULT, HttpMethod.GET, "", false);

          // user id
          record.setUserId(Long.valueOf(getField(genericRecord, "userId", "0")));

          // guid, cguid
          record.setGuid(coalesce(applicationPayload.get(new Utf8("g")), empty).toString());
          record.setCguid(coalesce(applicationPayload.get(new Utf8("n")), empty).toString());

          // remote ip
          record.setRemoteIp(coalesce(applicationPayload.get(new Utf8("RemoteIP")), empty).toString());

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

          // source and destination rotation id
          record.setSrcRotationId(-1L);
          record.setDstRotationId(-1L);

          String uri = "https://rover.ebay.com" + urlQueryString;
          record.setUri(uri);
          // Set the channel type + HTTP headers + channel action
          record.setChannelType(ChannelType.EPN);
          record.setHttpMethod(HttpMethod.GET);
          record.setChannelAction(ChannelAction.CLICK);
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

          UriComponents uriComponents;
          uriComponents = UriComponentsBuilder.fromUriString(uri).build();
          long campaignId = -1L;
          try{
            campaignId = Long.valueOf(uriComponents.getQueryParams().get("campid").get(0));
          } catch (Exception e) {
            logger.error("Parse campaign id error");
          }
          record.setCampaignId(campaignId);
          record.setPublisherId(-1L);
          record.setSnid("");
          record.setIsTracked(false);
          producer.send(new ProducerRecord<>(kafkaTopic, record.getSnapshotId(), record), KafkaSink.callback);
        }
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
