package com.ebay.traffic.chocolate.common;

import com.ebay.app.raptor.chocolate.avro.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.util.*;

/**
 * Created by yliu29 on 2/28/18.
 */
public class TestHelper {

  private static final Runtime sys = Runtime.getRuntime();

  /**
   * Create a temp directory for test
   * The temp directory will be deleted when test finished.
   *
   * The temp directory is a random directory, e.g. chocolate-ad1sdxzcvasdfxx
   *
   * @param root       the root path for the temp directory
   * @param namePrefix the prefix for the random temp file
   * @return java.io.File of the temp directory
   */
  public static File createTempDir(String root, String namePrefix) throws IOException {
    final File dir = createDirectory(root, namePrefix);
    sys.addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          deleteRecursively(dir);
        } catch (Exception e) {
          //
        }
      }
    });
    return dir;
  }

  /**
   * Create a temp directory for test
   * The temp directory will be deleted when test finished.
   *
   * The temp directory is a random directory, e.g. chocolate-ad1sdxzcvasdfxx
   *
   * @return java.io.File of the temp directory
   */
  public static File createTempDir() throws IOException {
    return createTempDir(System.getProperty("java.io.tmpdir"), "dir");
  }

  /**
   * Create a temp directory for test
   * The temp directory will be deleted when test finished.
   *
   * The temp directory is a random directory, e.g. chocolate-ad1sdxzcvasdfxx
   *
   * @param namePrefix the prefix for the random temp file
   * @return java.io.File of the temp directory
   */
  public static File createTempDir(String namePrefix) throws IOException {
    return createTempDir(System.getProperty("java.io.tmpdir"), namePrefix);
  }

  /**
   * @return random port
   */
  public static int getRandomPort() {
    int port = 0;
    try {
      ServerSocket s = new ServerSocket(0);
      port = s.getLocalPort();
      s.close();
    } catch (IOException e) {
    }
    return port;
  }

  private static File createDirectory(String root, String namePrefix) throws IOException {
    int attempts = 0;
    int maxAttempts = 10;
    File dir = null;
    while (dir == null) {
      attempts++;
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory under "
                + root + " after " + maxAttempts + " attemps!");
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID().toString());
        if (dir.exists() || !dir.mkdirs()) {
          dir = null;
        }
      } catch (SecurityException e) {
        dir = null;
      }

    }
    return dir.getCanonicalFile();
  }

  private static void deleteRecursively(File file) throws IOException {
    if (file != null) {
      try {
        if (file.isDirectory()) {
          IOException exception = null;
          for (File child : file.listFiles()) {
            try {
              deleteRecursively(child);
            } catch (IOException e) {
              exception = e;
            }
          }
          if (exception != null) {
            throw exception;
          }
        }
      } finally {
        if (!file.delete()) {
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath());
          }
        }
      }

    }
  }

  /**
   * Create filter message
   *
   * @param snapshotId the snapshot ID
   * @param publisherId the publisher ID
   * @param campaignId the campaign ID
   * @return filter message
   */
  public static FilterMessage newFilterMessage(long snapshotId,
                                               long publisherId,
                                               long campaignId) {
    return newFilterMessage(ChannelType.EPN, ChannelAction.CLICK,
            snapshotId, publisherId, campaignId);
  }

  /**
   * Create filter message
   *
   * @param snapshotId the snapshot ID
   * @param publisherId the publisher ID
   * @param campaignId the campaign ID
   * @param timestamp the timestamp
   * @return filter message
   */
  public static FilterMessage newFilterMessage(long snapshotId,
                                               long publisherId,
                                               long campaignId,
                                               long timestamp) {
    return newFilterMessage(ChannelType.EPN, ChannelAction.CLICK,
            snapshotId, publisherId, campaignId, timestamp);
  }

  /**
   * Create filter message
   *
   * @param channelType channel type
   * @param channelAction channel action
   * @param snapshotId the snapshot ID
   * @param publisherId the publisher ID
   * @param campaignId the campaign ID
   * @return filter message
   */
  public static FilterMessage newFilterMessage(ChannelType channelType,
                                               ChannelAction channelAction,
                                               long snapshotId,
                                               long publisherId,
                                               long campaignId) {
    return newFilterMessage(channelType, channelAction, snapshotId,
            publisherId, campaignId, System.currentTimeMillis());
  }

  /**
   * Create filter message
   *
   * @param channelType channel type
   * @param channelAction channel action
   * @param snapshotId the snapshot ID
   * @param publisherId the publisher ID
   * @param campaignId the campaign ID
   * @param timestamp the timestamp
   * @return filter message
   */
  public static FilterMessage newFilterMessage(ChannelType channelType,
                                               ChannelAction channelAction,
                                               long snapshotId,
                                               long publisherId,
                                               long campaignId,
                                               long timestamp) {
    FilterMessage message = new FilterMessage();
    message.setSnapshotId(snapshotId);
    message.setTimestamp(timestamp);
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setRequestHeaders("");
    message.setUri("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338195018&item=263365814851");
    message.setResponseHeaders("");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setRtRuleFlags(0L);
    message.setNrtRuleFlags(0L);
    message.setSnid("");
    return message;
  }

  /**
   * Create filter message
   *
   * @param channelType channel type
   * @param channelAction channel action
   * @param snapshotId the snapshot ID
   * @param publisherId the publisher ID
   * @param campaignId the campaign ID
   * @param timestamp the timestamp
   * @param ip the ip addresss
   * @return filter message
   */
  public static FilterMessage newFilterMessage(ChannelType channelType,
                                               ChannelAction channelAction,
                                               long snapshotId,
                                               long publisherId,
                                               long campaignId,
                                               long timestamp,
                                               String ip) {
  FilterMessage message = new FilterMessage();
    message.setSnapshotId(snapshotId);
    message.setTimestamp(timestamp);
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setRequestHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524|X-eBay-Client-IP: " + ip +
      "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive");
    message.setUri("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338195018&item=263365814851");
    message.setResponseHeaders("");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setRtRuleFlags(0L);
    message.setNrtRuleFlags(0L);
    message.setSnid("");
    return message;
}

  /**
   * Create listener message
   *
   * @param snapshotId the snapshot ID
   * @param publisherId the publisher ID
   * @param campaignId the campaign ID
   * @return listener message
   */
  public static ListenerMessage newListenerMessage(long snapshotId,
                                                   long publisherId,
                                                   long campaignId) {
    return newListenerMessage(ChannelType.EPN, ChannelAction.CLICK,
            snapshotId, publisherId, campaignId);
  }

  /**
   * Create listener message
   *
   * @param channelType channel type
   * @param channelAction channel action
   * @param snapshotId the snapshot ID
   * @param publisherId the publisher ID
   * @param campaignId the campaign ID
   * @return listener message
   */
  public static ListenerMessage newListenerMessage(ChannelType channelType,
                                            ChannelAction channelAction,
                                            long snapshotId,
                                            long publisherId,
                                            long campaignId) {
    ListenerMessage message = new ListenerMessage();
    message.setSnapshotId(snapshotId);
    message.setTimestamp(System.currentTimeMillis());
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setRequestHeaders("");
    message.setUri("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338195018&item=263365814851");
    message.setResponseHeaders("");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setSnid("");
    return message;
  }

  /**
   * Load properties from file
   *
   * @param fileName the properties file
   * @return properties
   * @throws IOException
   */
  public static Properties loadProperties(String fileName) throws IOException {
    Properties prop = new Properties();
    try (InputStream in = TestHelper.class.getClassLoader().getResourceAsStream(fileName)) {
      prop.load(in);
    }
    return prop;
  }

  /**
   * Poll number of records from Kafka.
   *
   * @param consumer the kafka consumer
   * @param topics the kafka topics to subscribe
   * @param number the number of records to poll
   * @param timeout timeout before return
   * @return record map
   */
  public static <K, V> Map<K, V> pollFromKafkaTopic(
          Consumer<K, V> consumer, List<String> topics, int number, long timeout) {

    Map<K, V> result = new HashMap<>();

    consumer.subscribe(topics);

    int count = 0;
    long start = System.currentTimeMillis();
    long end = start;
    while (count < number && (end - start < timeout)) {
      ConsumerRecords<K, V> consumerRecords = consumer.poll(100);
      Iterator<ConsumerRecord<K, V>> iterator = consumerRecords.iterator();

      while (iterator.hasNext()) {
        ConsumerRecord<K, V> record = iterator.next();
        result.put(record.key(), record.value());
        count++;
      }
      end = System.currentTimeMillis();
    }

    return result;
  }
}
