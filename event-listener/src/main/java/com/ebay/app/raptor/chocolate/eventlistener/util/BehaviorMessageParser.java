package com.ebay.app.raptor.chocolate.eventlistener.util;

import com.ebay.app.raptor.chocolate.avro.ChannelAction;
import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.avro.BehaviorMessage;
import com.ebay.app.raptor.chocolate.common.SnapshotId;
import com.ebay.app.raptor.chocolate.eventlistener.ApplicationOptions;
import com.ebay.app.raptor.chocolate.eventlistener.constant.Constants;
import com.ebay.kernel.context.ServerContext;
import com.ebay.kernel.presentation.constants.PresentationConstants;
import com.ebay.platform.dds.api.DeviceInfoProvider;
import com.ebay.platform.raptor.ddsmodels.DDSResponse;
import com.ebay.platform.raptor.ddsmodels.DeviceInfo;
import com.ebay.platform.raptor.ddsmodels.UserAgentInfo;
import com.ebay.raptor.domain.request.api.DomainRequestData;
import com.ebay.raptorio.request.tracing.RequestTracingContext;
import com.ebay.tracking.common.util.UrlProcessHelper;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import com.ebay.traffic.monitoring.Metrics;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by jialili1 on 7/29/20
 */
public class BehaviorMessageParser {
  private static final Logger logger = LoggerFactory.getLogger(BehaviorMessageParser.class);
  private Metrics metrics = ESMetrics.getInstance();

  private static BehaviorMessageParser INSTANCE;

  private Map<String, String> applicationPayload = new HashMap<>();
  private Map<String, String> clientData = new HashMap<>();
  private List<Map<String, String>> data = new ArrayList<>();

  /**
   * tag - param map
   */
  public static final ImmutableMap<String, String> tagParamMap = new ImmutableMap.Builder<String, String>()
      .put("chnl", "")
      .put("euid", "euid")
      .put("emid", "bu")
      .put("bu", "bu")
      .put("crd", "crd")
      .put("segname", "segname")
      .put("exe", "exe")
      .put("ext", "ext")
      .put("es", "es")
      .put("ec", "ec")
      .put("nqc", "nqt")
      .put("emsid", "emsid")
      .put("sid", "sid")
      .put("rpp_cid", "rpp_cid")
      .put("rank", "rank")
      .put("cs", "cs")
      .put("placement-type", "placement-type")
      .put("adcamppu", "pu")
      .put("cbtrack", "cbtrack")
      .build();

  /**
   * Singleton class
   */
  private BehaviorMessageParser() { }

  /**
   * Initialize singleton instance
   */
  public static synchronized void init() {
    if (INSTANCE == null) {
      INSTANCE = new BehaviorMessageParser();
    }
  }

  /**
   * Return singleton instance
   */
  public static BehaviorMessageParser getInstance() {
    return INSTANCE;
  }

  public BehaviorMessage parse(final HttpServletRequest request, ContainerRequestContext requestContext,
                                 MultiValueMap<String, String> parameters, UserAgentInfo agentInfo, String uri,
                                 Long startTime, final ChannelType channelType, final ChannelAction channelAction,
                                 int pageId, String pageName, int rdt) {
    // set default value
    BehaviorMessage record = new BehaviorMessage("", "", 0L, null, 0, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, applicationPayload, null, clientData, "", "", "", data);

    RequestTracingContext tracingContext = (RequestTracingContext) requestContext.getProperty(RequestTracingContext.NAME);
    DomainRequestData domainRequest = (DomainRequestData) requestContext.getProperty(DomainRequestData.NAME);

    // adguid
    String adguid = "";
    Cookie[] cookies = request.getCookies();
    for (Cookie cookie : cookies) {
      if (Constants.ADGUID.equalsIgnoreCase(cookie.getName())) {
        adguid = cookie.getValue();
      }
    }
    record.setAdguid(adguid);

    // source id
    record.setSid(parseTagFromParams(parameters, Constants.SOURCE_ID));

    // eventTimestamp
    record.setEventTimestamp(startTime);

    // page info
    record.setPageId(pageId);
    record.setPageName(pageName);

    // event family and action
    record.setEventFamily(Constants.EVENT_FAMILY_CRM);
    record.setEventAction(Constants.EVENT_ACTION);

    // snapshotId
    Long snapshotId = SnapshotId.getNext(ApplicationOptions.getInstance().getDriverId()).getRepresentation();
    record.setSnapshotId(String.valueOf(snapshotId));

    // fake session info
    record.setSessionId(String.valueOf(snapshotId));
    record.setSeqNum("1");

    // agent info
    record.setAgentInfo(agentInfo.getUserAgentRawData());

    // app info
    record.setAppId(CollectionServiceUtil.getAppIdFromUserAgent(agentInfo));
    if (agentInfo.getAppInfo() != null) {
      record.setAppVersion(agentInfo.getAppInfo().getAppVersion());
    }

    // url query string
    record.setUrlQueryString(UrlProcessHelper.getMaskedUrl(uri, domainRequest.isSecure(), false));

    // applicationPayload
    for (Map.Entry<String, String> entry : tagParamMap.entrySet()) {
      if (parameters.containsKey(entry.getValue()) && parameters.getFirst(entry.getValue()) != null) {
        applicationPayload.put(entry.getKey(), parseTagFromParams(parameters, entry.getValue()));
      }
    }
    // add tags in url param "sojTags" into applicationPayload
    if(parameters.containsKey(Constants.SOJ_TAGS) && parameters.get(Constants.SOJ_TAGS).get(0) != null) {
      String sojTags = parameters.get(Constants.SOJ_TAGS).get(0);
      try {
        sojTags = URLDecoder.decode(sojTags, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        logger.warn("Param sojTags is wrongly encoded", e);
        metrics.meter("ErrorEncodedSojTags", 1, Field.of(Constants.CHANNEL_ACTION, channelAction.toString()),
            Field.of(Constants.CHANNEL_TYPE, channelType.toString()));
      }
      if (!StringUtils.isEmpty(sojTags)) {
        StringTokenizer stToken = new StringTokenizer(sojTags, PresentationConstants.COMMA);
        while (stToken.hasMoreTokens()) {
          StringTokenizer sojNvp = new StringTokenizer(stToken.nextToken(), PresentationConstants.EQUALS);
          if (sojNvp.countTokens() == 2) {
            String sojTag = sojNvp.nextToken().trim();
            String urlParam = sojNvp.nextToken().trim();
            if (!StringUtils.isEmpty(urlParam) && !StringUtils.isEmpty(sojTag)) {
              applicationPayload.put(sojTag, parseTagFromParams(parameters, urlParam));
            }
          }
        }
      }
    }
    // add other tags
    applicationPayload.put("Agent", agentInfo.getUserAgentRawData());
    applicationPayload.put("Payload", UrlProcessHelper.getMaskedUrl(uri, domainRequest.isSecure(), false));

    record.setApplicationPayload(applicationPayload);

    // device info
//    DeviceInfoProvider deviceInfoProvider = new DeviceInfoProvider(false);
//    DeviceInfo deviceInfo = deviceInfoProvider.get(userAgent);

    DDSResponse deviceInfo = agentInfo.getDeviceInfo();
    String deviceFamily;
    if (deviceInfo.isTablet()) {
      deviceFamily = "Tablet";
    } else if (deviceInfo.isTouchScreen()) {
      deviceFamily = "TouchScreen";
    } else if (deviceInfo.isDesktop()) {
      deviceFamily = "Desktop";
    } else if (deviceInfo.isMobile()) {
      deviceFamily = "Mobile";
    } else {
      deviceFamily = "Other";
    }
    record.setDeviceFamily(deviceFamily);
    record.setDeviceType(deviceInfo.getOsName());
    record.setBrowserVersion(deviceInfo.getBrowserVersion());
    record.setBrowserFamily(deviceInfo.getBrowser());
    record.setOsVersion(deviceInfo.getDeviceOSVersion());
    record.setOsFamily(deviceInfo.getDeviceOS());

    // cobrand
    record.setCobrand(String.valueOf(domainRequest.getCoBrandId()));

    // rlogid
    record.setRlogid(tracingContext.getRlogId());

    // client data
    clientData.put("ForwardedFor", domainRequest.getXForwardedFor());
    clientData.put("Script", domainRequest.getServletPath());
    clientData.put("Server", domainRequest.getHost());
    record.setWebServer(domainRequest.getHost());
    InetAddress netAddress = getInetAddress();
    if (netAddress != null) {
      clientData.put("TMachine", netAddress.getHostAddress());
    }
    clientData.put("TName", domainRequest.getCommandName());
    String dnsRegion = ServerContext.getDnsRegion();
    if (dnsRegion == null) {
      dnsRegion = "";
    } else {
      dnsRegion = dnsRegion.trim();
    }
    clientData.put("colo", dnsRegion);
    clientData.put("Agent", domainRequest.getUserAgent());
    clientData.put("RemoteIP", domainRequest.getClientIp());
    record.setRemoteIP(domainRequest.getClientIp());
    clientData.put("ContentLength", String.valueOf(domainRequest.getContentLength()));
    clientData.put("nodeId", tracingContext.getNodeId());
    clientData.put("requestGuid", tracingContext.getRequestGuid());
    clientData.put("Referrer", UrlProcessHelper.getMaskedUrl(domainRequest.getReferrerUrl(), false, true));
    if (domainRequest.getReferrerUrl() != null) {
      record.setRefererHash(String.valueOf(domainRequest.getReferrerUrl().hashCode()));
    }
    clientData.put("AcceptEncoding", domainRequest.getAcceptEncoding());
    record.setClientData(clientData);

    //site id
    record.setSiteId(String.valueOf(domainRequest.getSiteId()));

    //rdt
    record.setRdt(rdt);

    // channel type and action
    record.setChannelType(channelType.toString());
    record.setChannelAction(channelAction.toString());

    System.out.println(record);

    return record;
  }

  /**
   * Parse tag from url query string and add to sojourner
   */
  private static String parseTagFromParams(MultiValueMap<String, String> parameters, String param) {
    if (parameters.containsKey(param) && parameters.get(param).get(0) != null) {
      return parameters.getFirst(param);
    }

    return "";
  }

  /**
   * Get the address of the local host
   */
  public static InetAddress getInetAddress() {
    try {
      return InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      logger.error(e.getMessage(), e);
    }
    return null;
  }

}
