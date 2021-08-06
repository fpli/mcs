package com.ebay.traffic.chocolate.flink.nrt.util;

import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class PropertyMgrTest {
  PropertyMgr propertyMgr;

  @Before
  public void setUp() throws Exception {
    propertyMgr = PropertyMgr.getInstance();
  }

  @Test
  public void loadProperty() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "rhs-vsrvkiaa-kfk-lvs-1.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-2.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-3.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-4.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-5.rheos-streaming-qa.svc.32.tess.io:9092");
    properties.setProperty("group.id", "marketing-tracking-consumer-imk-trckng-event-transform");
    properties.setProperty("session.timeout.ms", "10000");
    properties.setProperty("auto.offset.reset", "earliest");
    properties.setProperty("sasl.mechanism", "IAF");
    properties.setProperty("security.protocol", "SASL_SSL");
    properties.setProperty("sasl.jaas.config", "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\" iafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\" iafEnv=\"staging\";");
    properties.setProperty("rheos.services.urls", "https://rheos-services.qa.ebay.com");
    properties.setProperty("ssl.endpoint.identification.algorithm", "");
    assertEquals(properties, propertyMgr.loadProperty("imk-trckng-event-transform-app.rheos.consumer.properties"));
  }

  @Test
  public void loadAllLines() {
    List<String> target = new ArrayList<>();
    target.add("bootstrap.servers=rhs-vsrvkiaa-kfk-lvs-1.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-2.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-3.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-4.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-5.rheos-streaming-qa.svc.32.tess.io:9092");
    target.add("group.id=marketing-tracking-consumer-imk-trckng-event-transform");
    target.add("session.timeout.ms=10000");
    target.add("auto.offset.reset=earliest");
    target.add("rheos.services.urls=https://rheos-services.qa.ebay.com");
    target.add("sasl.mechanism=IAF");
    target.add("security.protocol=SASL_SSL");
    target.add("sasl.jaas.config=io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\" iafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\" iafEnv=\"staging\";");
    target.add("ssl.endpoint.identification.algorithm=");
    target.add("flink.partition-discovery.interval-millis=60000");
    assertEquals(target, propertyMgr.loadAllLines("imk-trckng-event-transform-app.rheos.consumer.properties"));
  }

  @Test
  public void loadFile() {
    List<String> target = new ArrayList<>();
    target.add("bootstrap.servers=rhs-vsrvkiaa-kfk-lvs-1.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-2.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-3.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-4.rheos-streaming-qa.svc.32.tess.io:9092,rhs-vsrvkiaa-kfk-lvs-5.rheos-streaming-qa.svc.32.tess.io:9092");
    target.add("group.id=marketing-tracking-consumer-imk-trckng-event-transform");
    target.add("session.timeout.ms=10000");
    target.add("auto.offset.reset=earliest");
    target.add("rheos.services.urls=https://rheos-services.qa.ebay.com");
    target.add("sasl.mechanism=IAF");
    target.add("security.protocol=SASL_SSL");
    target.add("sasl.jaas.config=io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\" iafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\" iafEnv=\"staging\";");
    target.add("ssl.endpoint.identification.algorithm=");
    target.add("flink.partition-discovery.interval-millis=60000");
    assertEquals(StringUtils.join(target, "\n"), propertyMgr.loadFile("imk-trckng-event-transform-app.rheos.consumer.properties"));
  }
}