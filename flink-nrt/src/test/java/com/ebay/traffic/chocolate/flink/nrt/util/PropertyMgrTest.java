package com.ebay.traffic.chocolate.flink.nrt.util;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class PropertyMgrTest {
  PropertyMgr propertyMgr;

  @Before
  public void setUp() throws Exception {
    propertyMgr = PropertyMgr.getInstance();
  }

  @Test
  public void loadProperty() {
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "rhs-gvrvkiaa-kfk-slc-1.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-2.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-3.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-4.rheos-streaming-qa.svc.33.tess.io:9092");
    properties.setProperty("group.id", "marketing-tracking-consumer-ubi-event-sink");
    properties.setProperty("session.timeout.ms", "10000");
    properties.setProperty("auto.offset.reset", "earliest");
    properties.setProperty("receive.buffer.bytes", "65536");
    properties.setProperty("request.timeout.ms", "305000");
    properties.setProperty("sasl.mechanism", "IAF");
    properties.setProperty("security.protocol", "SASL_PLAINTEXT");
    properties.setProperty("sasl.jaas.config", "io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\" iafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\" iafEnv=\"staging\";");
    properties.setProperty("rheos.services.urls", "https://rheos-services.qa.ebay.com");
    assertEquals(properties, propertyMgr.loadProperty("ubi-event-sink-app.rheos.consumer.properties"));
  }

  @Test
  public void loadAllLines() {
    List<String> target = new ArrayList<>();
    target.add("bootstrap.servers=rhs-gvrvkiaa-kfk-slc-1.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-2.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-3.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-4.rheos-streaming-qa.svc.33.tess.io:9092");
    target.add("group.id=marketing-tracking-consumer-ubi-event-sink");
    target.add("session.timeout.ms=10000");
    target.add("auto.offset.reset=earliest");
    target.add("receive.buffer.bytes=65536");
    target.add("request.timeout.ms=305000");
    target.add("sasl.mechanism=IAF");
    target.add("security.protocol=SASL_PLAINTEXT");
    target.add("sasl.jaas.config=io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\" iafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\" iafEnv=\"staging\";");
    target.add("rheos.services.urls=https://rheos-services.qa.ebay.com");
    assertEquals(target, propertyMgr.loadAllLines("ubi-event-sink-app.rheos.consumer.properties"));
  }

  @Test
  public void loadFile() {
    List<String> target = new ArrayList<>();
    target.add("bootstrap.servers=rhs-gvrvkiaa-kfk-slc-1.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-2.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-3.rheos-streaming-qa.svc.33.tess.io:9092,rhs-gvrvkiaa-kfk-slc-4.rheos-streaming-qa.svc.33.tess.io:9092");
    target.add("group.id=marketing-tracking-consumer-ubi-event-sink");
    target.add("session.timeout.ms=10000");
    target.add("auto.offset.reset=earliest");
    target.add("receive.buffer.bytes=65536");
    target.add("request.timeout.ms=305000");
    target.add("sasl.mechanism=IAF");
    target.add("security.protocol=SASL_PLAINTEXT");
    target.add("sasl.jaas.config=io.ebay.rheos.kafka.security.iaf.IAFLoginModule required iafConsumerId=\"urn:ebay-marketplace-consumerid:0a2563dc-390f-4b78-8648-68c01e248639\" iafSecret=\"f660cc36-bf60-4528-befc-bb2fc203a960\" iafEnv=\"staging\";");
    target.add("rheos.services.urls=https://rheos-services.qa.ebay.com");
    assertEquals(StringUtils.join(target, "\n"), propertyMgr.loadFile("ubi-event-sink-app.rheos.consumer.properties"));
  }
}