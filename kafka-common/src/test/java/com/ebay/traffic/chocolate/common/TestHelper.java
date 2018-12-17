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
    message.setRequestHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524" +
      "|Accept: application/json|User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) " +
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive" +
        "|X-eBay-Client-IP: 10.239.10.10" +
        "|Referer: http://www.polo6rfreunde.de/index.php/Dereferer/?ref=aHR0cCUzQS8vcm92ZXIuZWJheS5jb20vcm92ZXIvMS83MDctNTM0Nzct" +
        "|accept-language: zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7" +
        "|cookie: paas.active.org=EBAY; altus.not_first=true; paas.environment=production; cssg=765619471660ac802342ae39fffffb2d;" +
        "paas.user=huiclu; AMCVS_A71B5B5B54F607AB0A4C98A2%40AdobeOrg=1;" +
        "AMCV_A71B5B5B54F607AB0A4C98A2%40AdobeOrg=-" +
        "1758798782%7CMCIDTS%7C17829%7CMCMID%7C67754572831508364883533405785999125926%7CMCAAMLH-1540969649%7C9%7CMCAAMB" +
        "-1540969650%7C6G1ynYcLPuiQxYZrsz_pkqfLG9yMXBpb2zX5dvJdYQJzPXImdj0y%7CMCCIDH%7C-456987536%7CMCOPTOUT" +
        "-1540372050s%7CNONE%7CMCAID%7CNONE; aam_uuid=67766820294013938453532496876785767451;" +
        "ns1=BAQAAAWZ6LAkKAAaAANgATF2xTbtjNzJ8NjAxXjE1Mzk1ODQwNjEzNzZeXjFeM3wyfDY1fDV8NHw3XjFeMl40XjNeMTJeMTJe" +
        "Ml4xXjFeMF4xXjBeMV42NDQyNDU5MDc1YkrrEaQBrH0FDNX/QmKkLD/E0+Q*; dp1=bu1p/QEBfX0BAX19AQA**5db14dbb^bl/US5f" +
        "92813b^pbf/%238000400000005db14dbb^; s=CgAD4ACBb0Wu7NzY1NjE5NDcxNjYwYWM4MDIzNDJhZTM5ZmZmZmZiMmTFKrqK;" +
        "npii=btguid/765619471660ac802342ae39fffffb2d5db14dc2^cguid/" + cguid);
    message.setUri("http://rover.ebay.com/rover/1/1346-53200-19255-0/4?ff1=2&toolid=10039&campid=5338195018" +
        "&uq=A%26B%3DC&customid=page&ff2=2&ff3=2&ff16=2&ctx=n&cb_kw=autograph%2C+signed" +
        "&cb_cat=176985,176984&cb_ex_kw=Lego&cb_ex_cat=hah&fb_used=1&ad_format=123&ad_content_type=9012&load_time=11&lgeo=1&FirstName=Foo+A%26B%3DC&LastName=Bar");
    message.setResponseHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524|Cookie: cguid/" + cguid +
      "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive"
    + "|location: http://cgi.ebay.fr/ws/eBayISAPI.dll?ViewItem&item=dwddwdwdz&ixed=1");
    message.setSiteId(1L);
    message.setLandingPageUrl("");
    message.setSrcRotationId(1L);
    message.setDstRotationId(2L);
    message.setRequestHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524|" +
      "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive");
    message.setUri("http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10039&campid=5338195018&item=263365814851");
    message.setResponseHeaders("test_case_tag: filterAutomation|test_case_id: 1505705453524|Set-Cookie: cguid/" + cguid +
      "|Accept: application/json|User-Agent: Mozilla+Compatible|Host: rover.qa.ebay.com|Proxy-Connection: keep-alive");
    message.setChannelAction(channelAction);
    message.setChannelType(channelType);
    message.setHttpMethod(HttpMethod.POST);
    message.setRtRuleFlags(1025L);
    message.setNrtRuleFlags(1024L);
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
