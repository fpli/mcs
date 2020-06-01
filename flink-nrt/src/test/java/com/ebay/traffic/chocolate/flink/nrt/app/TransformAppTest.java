package com.ebay.traffic.chocolate.flink.nrt.app;

import org.apache.flink.configuration.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class TransformAppTest {
  private TrackingEventTransformApp transformApp;

  @Before
  public void setUp() throws Exception {
    transformApp = new TrackingEventTransformApp();
  }

  @Test
  public void getGlobalJobParameters() {
    Configuration configuration = new Configuration();
    configuration.setString("elasticsearch.url", "http://10.148.181.34:9200");
    configuration.setString("elasticsearch.index.prefix", "chocolate-metrics-");
    assertEquals(configuration, transformApp.getGlobalJobParameters());
  }

  @Test
  public void getConsumerTopics() {
    assertEquals(Arrays.asList(
                    "marketing.tracking.ssl.filtered-paid-search",
                    "marketing.tracking.ssl.filtered-roi",
                    "marketing.tracking.ssl.filtered-social-media",
                    "marketing.tracking.ssl.filtered-display"),
            transformApp.getConsumerTopics());
  }

  @Test
  public void getConsumerProperties() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "rhs-vsrvkiaa-kfk-lvs-1.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-2.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-3.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-4.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-5.rheos-streaming-qa.svc.32.tess.io:9092");
    properties.setProperty("group.id", "marketing-tracking-consumer-ssl-flink");
    properties.setProperty("session.timeout.ms", "10000");
    properties.setProperty("auto.offset.reset", "latest");
    properties.setProperty("rheos.services.urls", "https://rheos-services.qa.ebay.com");
    properties.setProperty("sasl.mechanism", "IAF");
    properties.setProperty("security.protocol", "SASL_SSL");
    properties.setProperty("sasl.jaas.config", "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\" iafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\" iafEnv=\"staging\";");
    properties.setProperty("ssl.endpoint.identification.algorithm", "");
    assertEquals(properties, transformApp.getConsumerProperties());
  }

  @Test
  public void getProducerTopic() {
    assertEquals("marketing.tracking.staging.flat", transformApp.getProducerTopic());
  }

  @Test
  public void getProducerProperties() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "rhs-gvrvkiaa-kfk-slc-1.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-2.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-3.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-4.rheos-streaming-qa.svc.33.tess.io:9092");
    properties.setProperty("rheos.services.urls", "https://rheos-services.qa.ebay.com");
    properties.setProperty("acks", "-1");
    properties.setProperty("buffer.memory", "33554432");
    properties.setProperty("retries", "3");
    properties.setProperty("batch.size", "16384");
    properties.setProperty("linger.ms", "20");
    properties.setProperty("max.block.ms", "120000");
    properties.setProperty("request.timeout.ms", "120000");
    properties.setProperty("compression.type", "snappy");
    properties.setProperty("rheos.producer", "marketing-tracking-producer");
    properties.setProperty("sasl.mechanism", "IAF");
    properties.setProperty("security.protocol", "SASL_PLAINTEXT");
    properties.setProperty("sasl.jaas.config", "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\" iafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\" iafEnv=\"staging\";");
    assertEquals(properties, transformApp.getProducerProperties());
  }

  @Test
  public void transform() {
  }
}