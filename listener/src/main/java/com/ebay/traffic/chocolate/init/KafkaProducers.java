package com.ebay.traffic.chocolate.init;

import com.ebay.traffic.chocolate.listener.util.ChannelIdEnum;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Lu huichun
 *
 * Class for providing a map of different Channel to KafkaProducerWrappers, this Class
 * is supporting for multiple topics for different channels
 */

public class KafkaProducers {

    private static final Logger logger = Logger.getLogger(KafkaProducers.class);

    private static volatile KafkaProducers INSTANCE;
    private volatile Map<ChannelIdEnum, KafkaProducerWrapper> channelProducerMap;
    /**Constructor for KafkaProducers*/
    private KafkaProducers() {
        ListenerOptions options = ListenerOptions.getInstance();
        channelProducerMap = new HashMap<>();
        try {
            for (ChannelIdEnum channel : options.getKafkaChannelTopicMap().keySet()) {
                channelProducerMap.put(channel,
                        new KafkaProducerWrapper(options.getKafkaChannelTopicMap().get(channel),
                                new KafkaProducer<>(options.getKafkaProperties())));
            }
        } catch (IOException e){
            logger.error("Error reading Kafka config files");
        }
    }
    /**Constructor for KafkaProducers for unit tests*/
    private KafkaProducers(ListenerOptions options) {
      channelProducerMap = new HashMap<>();
      try {
        for (ChannelIdEnum channel : options.getKafkaChannelTopicMap().keySet()) {
          channelProducerMap.put(channel,
              new KafkaProducerWrapper(options.getKafkaChannelTopicMap().get(channel),
                  new KafkaProducer<>(options.getKafkaProperties())));
        }
      } catch (IOException e){
        logger.error("Error reading Kafka config files");
      }
    }

    /**
     * Init method for KafkaProducers
     *
     */
    public static void init(){
        Validate.isTrue(INSTANCE == null, "Instance should be initialized only once");
        INSTANCE = new KafkaProducers();
    }

  /**
   * Init method for KafkaProducers for unit tests
   *
   */
  public static void init(ListenerOptions options){
    Validate.isTrue(INSTANCE == null, "Instance should be initialized only once");
    INSTANCE = new KafkaProducers(options);
  }

    /**
     * Flush and close all channel's producer
     */
    public void close() {
        Validate.isTrue(INSTANCE != null, "Instance should be init first");
        for (ChannelIdEnum channel : channelProducerMap.keySet()) {
            channelProducerMap.get(channel).close();
        }
        channelProducerMap.clear();
    }

    /**
     * Instance close
     */
    public static void closeAll() {
        INSTANCE.close();
        INSTANCE = null;
    }

    /**
     * Get the instance of KafkaProducers
     * @return  kafkaProducers
     */
    public static KafkaProducers getInstance() {
        if (INSTANCE == null) {
            synchronized (KafkaProducers.class) {
                if (INSTANCE == null)
                    init();
            }
        }
        return INSTANCE;
    }

    /**
     * Get the map of channel and producer
     * @return channelProducerMap
     */
    public Map<ChannelIdEnum, KafkaProducerWrapper> getChannelProducerMap(){
        return channelProducerMap;
    }

    /**
     * Get the kafka producer by channel
     * @param channel
     * @return kafka producer
     */
    public KafkaProducerWrapper getKafkaProducer(ChannelIdEnum channel){
        return channelProducerMap.get(channel);
    }

}
