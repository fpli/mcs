package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Wrapper class for RheosConsumer to consume what we miss from LB
 *
 * @author xiangli4
 */
public class RheosConsumerWrapper {
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RheosConsumerWrapper.class);

  private Consumer<byte[], RheosEvent> consumer;
  private GenericRecordDomainDataDecoder decoder;
  private static RheosConsumerWrapper rheosConsumer;

  /**
   * Singleton
   */
  private RheosConsumerWrapper() {
    this.consumer = null;
    this.decoder = null;
  }

  /**
   * Init the RheosConsumerWrapper
   *
   * @param rheosProperties rheos kafka consumer properties
   */
  synchronized public static void init(Properties rheosProperties) {
    rheosConsumer = new RheosConsumerWrapper();
    rheosConsumer.consumer = new KafkaConsumer<>(rheosProperties);
    rheosConsumer.consumer.subscribe(Arrays.asList(ApplicationOptions.getInstance().getConsumeRheosTopic().split(",")));
    logger.info("Rheos Topic: " + ApplicationOptions.getInstance().getConsumeRheosTopic());
    Map<String, Object> config = new HashMap<>();
    config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, ApplicationOptions.getInstance().getConsumeRheosKafkaServiceUrl());
    rheosConsumer.decoder = new GenericRecordDomainDataDecoder(config);
    logger.info("RheosConsumer initialized");
  }

  /**
   * Terminate the consumer gracefully
   */
  public static synchronized void terminate() {
    rheosConsumer.consumer.close();
    rheosConsumer = null;
    logger.info("RheosConsumer terminated");
  }

  public static RheosConsumerWrapper getInstance() {
    return rheosConsumer;
  }

  public Consumer<byte[], RheosEvent> getConsumer() {
    return consumer;
  }

  public GenericRecordDomainDataDecoder getDecoder() {
    return decoder;
  }
}
