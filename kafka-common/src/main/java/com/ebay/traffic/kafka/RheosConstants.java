package com.ebay.traffic.kafka;

/**
 * Created by yliu29 on 2/13/18.
 */
public class RheosConstants {

  public static final String RHEOS_SERVICES_URL_KEY = "rheos.services.urls";

  public static final String RHEOS_SERVICE_URL_DEFAULT = "http://rheos-services.stratus.ebay.com";

  public static final String RHEOS_SERVICE_URL_QA = "https://rheos-services.qa.ebay.com";

  public static final String RHEOS_BOOTSTRAP_SERVERS_DEFAULT =
          "rheos-kafka-proxy-1.stratus.phx.ebay.com:9092,rheos-kafka-proxy-2.stratus.phx.ebay.com:9092," +
          "rheos-kafka-proxy-3.stratus.phx.ebay.com:9092,rheos-kafka-proxy-4.stratus.phx.ebay.com:9092," +
          "rheos-kafka-proxy-5.stratus.phx.ebay.com:9092,rheos-kafka-proxy-1.stratus.lvs.ebay.com:9092," +
          "rheos-kafka-proxy-2.stratus.lvs.ebay.com:9092,rheos-kafka-proxy-3.stratus.lvs.ebay.com:9092," +
          "rheos-kafka-proxy-4.stratus.lvs.ebay.com:9092,rheos-kafka-proxy-5.stratus.lvs.ebay.com:9092";

  public static final String RHEOS_TOPIC_SCHEMA_KEY = "rheos.topic.schema";

  public static final String RHEOS_PRODUCER_KEY = "rheos.producer";
}
