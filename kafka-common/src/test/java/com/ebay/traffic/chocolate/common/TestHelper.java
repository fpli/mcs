package com.ebay.traffic.chocolate.common;

import com.ebay.app.raptor.chocolate.avro.*;
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV1;
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
   * @param snapshotId the snapshot ID
   * @param shortSnapshotId the short snapshot ID
   * @param publisherId the publisher ID
   * @param campaignId the campaign ID
   * @param timestamp the timestamp
   * @return filter message
   */
  public static FilterMessage newFilterMessage(long snapshotId,
                                               long shortSnapshotId,
                                               long publisherId,
                                               long campaignId,
                                               long timestamp) {
    return newFilterMessage(ChannelType.EPN, ChannelAction.CLICK,
        snapshotId, shortSnapshotId, publisherId, campaignId, timestamp);
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
    message.setShortSnapshotId(123L);
    message.setTimestamp(timestamp);
    message.setUserId(1L);
    message.setCguid("");
    message.setGuid("");
    message.setRemoteIp("127.0.0.1");
    message.setLangCd("");
    message.setUserAgent("");
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("");
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
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
   * @param shortSnapshotId the short snapshot ID
   * @param publisherId the publisher ID
   * @param campaignId the campaign ID
   * @param timestamp the timestamp
   * @return filter message
   */
  public static FilterMessage newFilterMessage(ChannelType channelType,
                                               ChannelAction channelAction,
                                               long snapshotId,
                                               long shortSnapshotId,
                                               long publisherId,
                                               long campaignId,
                                               long timestamp) {
    FilterMessage message = new FilterMessage();
    message.setSnapshotId(snapshotId);
    message.setShortSnapshotId(shortSnapshotId);
    message.setTimestamp(timestamp);
    message.setUserId(1L);
    message.setCguid("");
    message.setGuid("");
    message.setRemoteIp("127.0.0.1");
    message.setLangCd("");
    message.setUserAgent("");
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("");
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
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
    message.setShortSnapshotId(123L);
    message.setTimestamp(timestamp);
    message.setUserId(1L);
    message.setCguid("");
    message.setGuid("");
    message.setRemoteIp(ip);
    message.setLangCd("");
    message.setUserAgent("");
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("");
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
    message.setRequestHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524|X-eBay-Client-IP: " + ip +
      "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive");
    message.setUri("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338195018&item=263365814851");
    message.setResponseHeaders("");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setRtRuleFlags(0L);
    message.setNrtRuleFlags(0L);
    message.setSnid("testsnid");
    return message;
  }

  /**
   * Create filter message
   *
   * @param channelType channel type
   * @param channelAction channel action
   * @param snapshotId the snapshot ID
   * @param timestamp the timestamp
   * @param ip the ip addresss
   * @param userAgent the user agent
   * @return filter message
   */
  public static FilterMessage newFilterMessage(ChannelType channelType,
                                               ChannelAction channelAction,
                                               long snapshotId,
                                               long timestamp,
                                               String ip,
                                               String userAgent) {
    FilterMessage message = new FilterMessage();
    message.setSnapshotId(snapshotId);
    message.setShortSnapshotId(123L);
    message.setTimestamp(timestamp);
    message.setUserId(1L);
    message.setCguid("");
    message.setGuid("");
    message.setRemoteIp(ip);
    message.setLangCd("");
    message.setUserAgent(userAgent);
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("");
    message.setCampaignId(1L);
    message.setPublisherId(1L);
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
    message.setRequestHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524|X-eBay-Client-IP: " + ip +
        "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive");
    message.setUri("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338195018&item=263365814851");
    message.setResponseHeaders("");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setRtRuleFlags(0L);
    message.setNrtRuleFlags(0L);
    message.setSnid("testsnid");
    return message;
  }

  /**
   * Create filter message v1
   *
   * @param channelType channel type
   * @param channelAction channel action
   * @param snapshotId the snapshot ID
   * @param publisherId the publisher ID
   * @param campaignId the campaign ID
   * @param timestamp the timestamp
   * @param cguid the cguid in response cookie
   * @return filter message
   */
  public static FilterMessageV1 newFilterMessageV1(ChannelType channelType,
                                                   ChannelAction channelAction,
                                                   long snapshotId,
                                                   long publisherId,
                                                   long campaignId,
                                                   String cguid,
                                                   long timestamp) {
    FilterMessageV1 message = new FilterMessageV1();
    message.setSnapshotId(snapshotId);
    message.setTimestamp(timestamp);
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setRequestHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524|" +
            "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive");
    message.setUri("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338195018&item=263365814851");
    message.setResponseHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524|Set-Cookie: cguid/" + cguid +
            "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setRtRuleFlags(0L);
    message.setNrtRuleFlags(0L);
    message.setSnid("snidtest");
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
   * @param cguid the cguid in response cookie
   * @return filter message
   */
  public static FilterMessage newFilterMessage(ChannelType channelType,
                                               ChannelAction channelAction,
                                               long snapshotId,
                                               long publisherId,
                                               long campaignId,
                                               String cguid,
                                               long timestamp) {
    FilterMessage message = new FilterMessage();
    message.setSnapshotId(snapshotId);
    message.setShortSnapshotId(snapshotId);
    message.setTimestamp(timestamp);
    message.setUserId(1L);
    message.setCguid(cguid);
    message.setGuid("");
    message.setRemoteIp("127.0.0.1");
    message.setLangCd("");
    message.setUserAgent("");
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("http://www.translate.google.bid");
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setRequestHeaders("Referer:http://translate.google.com|X-Purpose:preview|Accept:text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8|Accept-Encoding:gzip, deflate, sdch|Accept-Language:en-US,en;q=0.8|Cookie:ebay=%5Esbf%3D%23%5E; nonsession=CgADLAAFY825/NQDKACBiWWj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDjrjVIf; dp1=bbl/USen-US5cb5ce77^; s=CgAD4ACBY9Lj3NzZjYmQ5ZWExNWIwYTkzZDEyODMxODMzZmZmMWMxMDhRBcIc; npii=btguid/92d9dfe51670a93d12831833fff1c1085ad49dd7^trm/svid%3D1136038334911271815ad49dd7^cguid/47a11c671620a93c91006917fffa2a915d116016^|Proxy-Connection:keep-alive|Upgrade-Insecure-Requests:1|X-EBAY-CLIENT-IP:10.108.159.177|User-Agent:Shuang-UP.Browser-baiduspider-ebaywinphocore");
    message.setUri("http://rover.qa.ebay.com/rover/1/5282-53481-19255-0/1?icep_ff3=9&toolid=12345&customid=24342&icep_uq=flashlight&icep_sellerId=&icep_ex_kw=&icep_sortBy=12&icep_catId=&icep_minPrice=&icep_maxPrice=&ipn=psmain&icep_vectorid=229508&kwid=902099&mtid=824&kw=lg&item=&uq=2&ext=4365&satitle=23435&FF2=https%3A%2F%2Fm.gsmarena.com%2Fsamsung_galaxy_s9-8966.php&ff3=");
    message.setResponseHeaders("Cache-Control:private,no-cache,no-store|location:https://rover.ebay.com.sg/rover/1/3423-53474-19255-0/1?lgeo=1&toolid=10044&customid=&item=253635703200&ff3=2&campid=5336871800&mpre=https%3A%2F%2Fwww.ebay.com.sg&cguid=279af33e1690a88ba1141e17df25744e&rvrrefts=279af3661690a88ba1132b79ffe2070c&dashenId=6505984835508625408&dashenCnt=0");
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.GET);
    message.setRtRuleFlags(0L);
    message.setNrtRuleFlags(0L);
    message.setSnid("snidtest");
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
   * @param cguid the cguid in request cookie
   * @return filter message
   */
  public static FilterMessage newFilterMessage(ChannelType channelType,
                                               ChannelAction channelAction,
                                               long snapshotId,
                                               long publisherId,
                                               String cguid,
                                               long campaignId,
                                               long timestamp) {
    FilterMessage message = new FilterMessage();
    message.setSnapshotId(snapshotId);
    message.setShortSnapshotId(123L);
    message.setTimestamp(timestamp);
    message.setUserId(1L);
    message.setCguid(cguid);
    message.setGuid("");
    message.setRemoteIp("127.0.0.1");
    message.setLangCd("");
    message.setUserAgent("");
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("");
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
    message.setRequestHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524|Cookie: cguid/" + cguid +
        "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive");
    message.setUri("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338195018&item=263365814851");
    message.setResponseHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524" +
        "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setRtRuleFlags(0L);
    message.setNrtRuleFlags(0L);
    message.setSnid("snidtest");
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
   * @param cguid_req the cguid in request cookie
   * @param cguid_res the cguid in response cookie
   * @return filter message
   */
  public static FilterMessage newFilterMessage(ChannelType channelType,
                                               ChannelAction channelAction,
                                               long snapshotId,
                                               long publisherId,
                                               long campaignId,
                                               String cguid_req,
                                               String cguid_res,
                                               long timestamp
                                               ) {
    FilterMessage message = new FilterMessage();
    message.setSnapshotId(snapshotId);
    message.setShortSnapshotId(123L);
    message.setTimestamp(timestamp);
    message.setUserId(1L);
    message.setCguid(cguid_res);
    message.setGuid("");
    message.setRemoteIp("127.0.0.1");
    message.setLangCd("");
    message.setUserAgent("");
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("");
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
    message.setRequestHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524|Cookie: cguid/" + cguid_req +
        "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive");
    message.setUri("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338195018&item=263365814851");
    message.setResponseHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524|Set-Cookie: cguid/" + cguid_res +
        "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setRtRuleFlags(0L);
    message.setNrtRuleFlags(0L);
    message.setSnid("snidtest");
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
   * @param cguid the CGUID in cookie
   * @return filter message
   */
  public static FilterMessage newFilterMessage(ChannelType channelType,
                                               ChannelAction channelAction,
                                               long snapshotId,
                                               long publisherId,
                                               long campaignId,
                                               long timestamp,
                                               String ip,
                                               String cguid) {
    FilterMessage message = new FilterMessage();
    message.setSnapshotId(snapshotId);
    message.setShortSnapshotId(123L);
    message.setTimestamp(timestamp);
    message.setUserId(1L);
    message.setCguid(cguid);
    message.setGuid("");
    message.setRemoteIp(ip);
    message.setLangCd("");
    message.setUserAgent("");
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("");
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
    message.setRequestHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524|X-eBay-Client-IP: " + ip +
      "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive");
    message.setUri("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338195018&item=263365814851");
    message.setResponseHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524|X-eBay-Client-IP: " + ip + "|Cookie: cguid/" + cguid +
      "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setRtRuleFlags(0L);
    message.setNrtRuleFlags(0L);
    message.setSnid("snidtest");
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
   * @param cguid the CGUID in cookie
   * @param userAgent the user agent in the request header
   * @return filter message
   */
  public static FilterMessage newFilterMessage(ChannelType channelType,
                                               ChannelAction channelAction,
                                               long snapshotId,
                                               long publisherId,
                                               long campaignId,
                                               long timestamp,
                                               String ip,
                                               String cguid,
                                               String userAgent) {
    FilterMessage message = new FilterMessage();
    message.setSnapshotId(snapshotId);
    message.setShortSnapshotId(123L);
    message.setTimestamp(timestamp);
    message.setUserId(1L);
    message.setCguid(cguid);
    message.setGuid("");
    message.setRemoteIp(ip);
    message.setLangCd("");
    message.setUserAgent(userAgent);
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("");
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
    message.setRequestHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524|X-eBay-Client-IP: " + ip +
        "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive");
    message.setUri("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338195018&item=263365814851");
    message.setResponseHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524|X-eBay-Client-IP: " + ip + "|Cookie: cguid/" + cguid +
        "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setRtRuleFlags(0L);
    message.setNrtRuleFlags(0L);
    message.setSnid("snidtest");
    return message;
  }

  /**
   * Create filter message
   *
   * @param channelType channel type
   * @param channelAction channel action
   * @param snapshotId the snapshot ID
   * @param publisherId the publisher ID
   * @param timestamp the timestamp
   * @param snid the snid
   * @return filter message
   */
  public static FilterMessage newFilterMessage(ChannelType channelType,
                                               ChannelAction channelAction,
                                               long snapshotId,
                                               long publisherId,
                                               long timestamp,
                                               String snid) {
    FilterMessage message = new FilterMessage();
    message.setSnapshotId(snapshotId);
    message.setShortSnapshotId(123L);
    message.setTimestamp(timestamp);
    message.setUserId(1L);
    message.setCguid("");
    message.setGuid("");
    message.setRemoteIp("127.0.0.1");
    message.setLangCd("");
    message.setUserAgent("");
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("");
    message.setCampaignId(1l);
    message.setPublisherId(publisherId);
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
    message.setRequestHeaders("");
    message.setUri("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338195018&item=263365814851");
    message.setResponseHeaders("");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setRtRuleFlags(0L);
    message.setNrtRuleFlags(0L);
    message.setSnid(snid);
    return message;
  }
  
  public static FilterMessage newFilterMessage(ChannelType channelType,
                                               ChannelAction channelAction,
                                               long snapshotId,
                                               long publisherId,
                                               long campaignId,
                                               long timestamp,
                                               long rtRuleFlags,
                                               long nrtRuleFlags,
                                               boolean isMobi) {
    FilterMessage message = new FilterMessage();
    message.setSnapshotId(snapshotId);
    message.setShortSnapshotId(123L);
    message.setTimestamp(timestamp);
    message.setUserId(1L);
    message.setCguid("");
    message.setGuid("");
    message.setRemoteIp("127.0.0.1");
    message.setLangCd("");
    message.setUserAgent("");
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("");
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
    message.setRequestHeaders(isMobi ? "Accept: application/json|User-Agent: Mobi" : "");
    message.setUri("");
    message.setResponseHeaders("");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setRtRuleFlags(rtRuleFlags);
    message.setNrtRuleFlags(nrtRuleFlags);
    message.setSnid("");
    return message;
  }

  public static FilterMessage newFilterMessage(long snapshotId,
                                               long timestamp,
                                               ChannelType channelType,
                                               ChannelAction channelAction,
                                               long rtRuleFlags,
                                               long nrtRuleFlags) {
    FilterMessage message = new FilterMessage();
    message.setSnapshotId(snapshotId);
    message.setShortSnapshotId(123L);
    message.setTimestamp(timestamp);
    message.setUserId(1L);
    message.setCguid("");
    message.setGuid("");
    message.setRemoteIp("127.0.0.1");
    message.setLangCd("");
    message.setUserAgent("");
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("");
    message.setCampaignId(1L);
    message.setPublisherId(11L);
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
    message.setRequestHeaders("");
    message.setUri("");
    message.setResponseHeaders("");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setRtRuleFlags(rtRuleFlags);
    message.setNrtRuleFlags(nrtRuleFlags);
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
    message.setShortSnapshotId(123L);
    message.setTimestamp(System.currentTimeMillis());
    message.setUserId(1L);
    message.setCguid("");
    message.setGuid("");
    message.setRemoteIp("127.0.0.1");
    message.setLangCd("");
    message.setUserAgent("");
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("");
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
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
   * Create listener message
   *
   * @param snapshotId the snapshot ID
   * @param shortSnapshotId the short snapshot ID
   * @param publisherId the publisher ID
   * @param campaignId the campaign ID
   * @param timestamp the timestamp
   * @return filter message
   */
  public static ListenerMessage newListenerMessage(long snapshotId,
                                               long shortSnapshotId,
                                               long publisherId,
                                               long campaignId,
                                               long timestamp) {
    return newListenerMessage(ChannelType.EPN, ChannelAction.CLICK,
            snapshotId, shortSnapshotId, publisherId, campaignId, timestamp);
  }

  /**
   * Create listener message
   *
   * @param channelType channel type
   * @param channelAction channel action
   * @param snapshotId the snapshot ID
   * @param shortSnapshotId the short snapshot ID
   * @param publisherId the publisher ID
   * @param campaignId the campaign ID
   * @param timestamp the timestamp
   * @return listener message
   */
  public static ListenerMessage newListenerMessage(ChannelType channelType,
                                               ChannelAction channelAction,
                                               long snapshotId,
                                               long shortSnapshotId,
                                               long publisherId,
                                               long campaignId,
                                               long timestamp) {
    ListenerMessage message = new ListenerMessage();
    message.setSnapshotId(snapshotId);
    message.setShortSnapshotId(shortSnapshotId);
    message.setTimestamp(timestamp);
    message.setUserId(1L);
    message.setCguid("");
    message.setGuid("");
    message.setRemoteIp("127.0.0.1");
    message.setLangCd("");
    message.setUserAgent("");
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("");
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
    message.setRequestHeaders("");
    message.setUri("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338195018&item=263365814851");
    message.setResponseHeaders("");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setSnid("");
    return message;
  }

  public static ListenerMessage newROIMessage(ChannelType channelType,
                                                   ChannelAction channelAction,
                                                   long snapshotId,
                                                   long publisherId,
                                                   long campaignId) {
    ListenerMessage message = new ListenerMessage();
    message.setSnapshotId(snapshotId);
    message.setShortSnapshotId(123L);
    message.setTimestamp(System.currentTimeMillis());
    message.setUserId(1L);
    message.setCguid("");
    message.setGuid("");
    message.setRemoteIp("127.0.0.1");
    message.setLangCd("");
    message.setUserAgent("");
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("");
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
    message.setRequestHeaders("");
    message.setUri("http://mktcollectionsvc.vip.qa.ebay.com/marketingtracking/v1/roi?tranType=&uniqueTransactionId=324357529&itemId=52357723598250&transactionTimestamp=1581427339000&nroi=1");
    message.setResponseHeaders("");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setSnid("");
    return message;
  }

  public static ListenerMessage roverROIMessage(ChannelType channelType,
                                              ChannelAction channelAction,
                                              long snapshotId,
                                              long publisherId,
                                              long campaignId) {
    ListenerMessage message = new ListenerMessage();
    message.setSnapshotId(snapshotId);
    message.setShortSnapshotId(123L);
    message.setTimestamp(System.currentTimeMillis());
    message.setUserId(1L);
    message.setCguid("");
    message.setGuid("");
    message.setRemoteIp("127.0.0.1");
    message.setLangCd("");
    message.setUserAgent("");
    message.setGeoId(1L);
    message.setUdid("");
    message.setReferer("");
    message.setCampaignId(campaignId);
    message.setPublisherId(publisherId);
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
    message.setRequestHeaders("");
    message.setUri("https://rover.ebay.com/roverroi/1/711-518-1801-10?mpuid=2082834080;324077230744;2176548533011;&siteId=0&BIN-Store=1&ff1=ss" +
        "&ff2=CHECKOUT|26764&tranType=BIN-Store&ff3=100015329801208&rcb=0");
    message.setResponseHeaders("");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setSnid("");
    return message;
  }

  public static BehaviorMessage newBehaviorMessage(String snapshotId) {
    Map<String, String> applicationPayload = new HashMap<>();
    Map<String, String> clientData = new HashMap<>();
    List<Map<String, String>> data = new ArrayList<>();

    BehaviorMessage message = new BehaviorMessage();
    message.setGuid("");
    message.setAdguid("adguid");
    message.setEventTimestamp(System.currentTimeMillis());
    message.setPageId(3962);
    message.setSessionId(snapshotId);
    message.setSnapshotId(snapshotId);
    message.setSeqNum("1");
    message.setUrlQueryString("/marketingtracking/v1/impression?mkevt=4&mkcid=7&mkpid=0&sojTags=bu=bu&bu=43551630917" +
        "&emsid=e11051.m44.l1139&euid=c527526a795a414cb4ad11bfaba21b5d&ext=56623");
    message.setRemoteIP("127.0.0.1");
    message.setChannelType(ChannelType.SITE_EMAIL.toString());
    message.setChannelAction(ChannelAction.EMAIL_OPEN.toString());
    message.setDispatchId("dispatchId");
    message.setApplicationPayload(applicationPayload);
    message.setClientData(clientData);
    message.setData(data);

    return message;
  }

  public static UnifiedTrackingMessage newUnifiedTrackingMessage(String eventId) {
    Map<String, String> payload = new HashMap<>();

    UnifiedTrackingMessage message = new UnifiedTrackingMessage();
    message.setEventId(eventId);
    message.setProducerEventId("123");
    message.setEventTs(System.currentTimeMillis());
    message.setProducerEventTs(System.currentTimeMillis());
    message.setPayload(payload);

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
