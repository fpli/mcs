package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.app.raptor.chocolate.adservice.ApplicationOptions;
import com.ebay.app.raptor.chocolate.adservice.constant.Constants;
import com.ebay.app.raptor.chocolate.adservice.constant.LBSConstants;
import com.ebay.app.raptor.chocolate.adservice.constant.StringConstants;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSClient;
import com.ebay.app.raptor.chocolate.adservice.lbs.LBSQueryResult;
import com.ebay.jaxrs.client.EndpointUri;
import com.ebay.jaxrs.client.GingerClientBuilder;
import com.ebay.jaxrs.client.config.ConfigurationBuilder;
import com.ebay.kernel.context.RuntimeContext;
import com.ebay.kernel.domain.lookup.biz.LookupBoHelperCfg;
import com.ebay.kernel.patternmatch.dawg.Dawg;
import com.ebay.kernel.patternmatch.dawg.DawgDictionary;
import com.ebay.kernel.resource.ResolverFactory;
import com.ebay.platform.raptor.cosadaptor.context.IEndUserContext;
import com.ebay.raptor.device.fingerprint.client.api.DeviceFingerPrintClient;
import com.ebay.raptor.device.fingerprint.client.api.DeviceFingerPrintClientFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.http.Header;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.client.Client;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Response;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author Zhiyuan Wang
 * @since 2019/9/24
 */
public class DAPResponseHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(DAPResponseHandler.class);
  private static final String CONFIG_SUBFOLDER = "config/";

  private static final DawgDictionary userAgentBotDawgDictionary;
  private static final DawgDictionary ipBotDawgDictionary;

  private static final String MGVALUEREASON = "mgvaluereason";
  private static final String MGVALUE = "mgvalue";
  private static final String DEFAULT_MGVALUE = "0";

  private static final String DAP_USER_AGENT_ROBOT_FILE = "dap_user_agent_robot.txt";
  private static final String DAP_IP_ROBOT_FILE = "dap_ip_robot.txt";

  static {
    List<String> userAgentBotList = null;
    List<String> ipBotList = null;

    try {
      userAgentBotList = Files.readAllLines(Paths.get(RuntimeContext.getConfigRoot().getFile() + CONFIG_SUBFOLDER + DAP_USER_AGENT_ROBOT_FILE));
      ipBotList = Files.readAllLines(Paths.get(RuntimeContext.getConfigRoot().getFile() + CONFIG_SUBFOLDER + DAP_IP_ROBOT_FILE));
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
    }

    userAgentBotDawgDictionary = new DawgDictionary(Objects.requireNonNull(userAgentBotList).toArray(new String[0]), true);
    ipBotDawgDictionary = new DawgDictionary(Objects.requireNonNull(ipBotList).toArray(new String[0]), true);
  }

  private DAPResponseHandler() {
  }

  // TODO remove token
  private static final String TOKEN = "Bearer v^1.1#i^1#r^0#I^3#f^0#p^1#t^H4sIAAAAAAAAAO1YbWwURRjuXq8lpbQaY0Q+Yo5F0ED2Y3b37vZW7vSgnD0o7cG1fFQR53bn6HJ3u5edvbYHhtQSSUiMQUkgFZSCiRHiF8GaKiQYscGiUf9IDCIlMUYIiSAKGI3B3etRrpWApUVq4v25zMw77zzv87zvzOywbaVlszZWb7xcQYxzdLaxbQ6CAOVsWWnJ7Mpix+SSIrbAgOhse7DN2V58eg6GqWRaWoJwWtcwcrWmkhqWcp1+MmNokg6xiiUNphCWTFmKBhfVSBzNSmlDN3VZT5KucJWfBByA0As8AoSyLFp92lWP9bqf9Igi5N1eEFc4twdxnDWOcQaFNWxCzfSTHAt8FGApjq8HHokTJV6gvbynkXQtRQZWdc0yoVkykAMr5eYaBUhvDBRijAzTckIGwsFQtC4YrppfWz+HKfAVyLMQNaGZwYNb83QFuZbCZAbdeBmcs5aiGVlGGJNMoH+FwU6l4FUwtwA/R7SXZWXe5xa9XkWRFS8YFSpDupGC5o1x2D2qQsVzphLSTNXM3oxRi43YGiSb+Vat5SJc5bL/FmdgUo2ryPCT8+cGVwQjETJgr45iMEuloJFAZjoJZUTJVg5lUshQFYnlBSALskx5BB+ihBjgKTHm81Ey4n0cUgQhxvryIPpXykswBMU8XVNUm1DsqtXNuciKCA3lDRTwZhnVaXVGMG7aaAfs3IX88o224P0KZ8wmzdYcpSySXLnmzdUZmG2ahhrLmGjAw9CBHH1+EqbTqkIOHczlaT61WrGfbDLNtMQwLS0tdq3TLTytG6sZjmUBs3xRTVRuQilIDtirhROub0ypuVBkZM3CqmRm0xaWViuPLQDaajIAgOjzgDzvg2EFhvb+raMgZmZwtYxW9cSA14uQwiqczCnu2KhsRIF8AjM3y1/eHed4MY4oxeOLU4IvHqdibsVDgThCLEKxmOwT/y+i4ZVBFMkGModRB/9CDTQ1V7Usx4gXUhqjhITlIfC4IKBQzYKFDYnqtaCloXphOMzNRktmi/5/WinXD17W0yiiJ1U5O4gBu9bvOAsAmxFomNkRRYjtCMeYutZ8bDmAaZW2C42W9RSjQ2ubZ3JomRjE6DFre7ZkgXaFjIiAYDodTqUyJowlUXg4+/2Y2+uvG55q3ZTGVEyWmP2qqkr/FYfOSUvjZpk2ENYzhnW7o+vsU71eTyDN2gdNQ08mkbF0ZEzYQt+Kvnat30Y+RvE4uTVehnvP+W/kvZxUrfRaNdYiu+Nqq3CMneXA+moVBREAYURxzcvpXZ8d2WHmfPbA6EdYrWMTKbfhxs4MflkIFOV+oJ3oYtuJfQ6CYBl2BpjOTistbnAWT5iMVRPRKozTWF2tWZ/MBqITKJuGquEoJdT1nL624C2jcyV7/8BrRlkxKC942mCnXhspAXdNrAA+wHI88HAiLzSy06+NOsF9znvZ56Z+Tz1Q0de18OEXpW1//rR+s1TGVgwYEURJkbOdKMKb6ha/Vbtn/zG+fNL56nsu/fG+49vWH8AZR9mmqiu/Vm7o7Dl4qNLhn/X5TsJf6fktdfpiR8/T3mXlxc43xk/86kJ8/O5Pdu88ESqasUDtdjSWPj9FTxzp6t0XffeItqar+czPWt/mrSd+rDzoUD7svnJ5w5ojJ6c0v3Ry6jfbH3ry8KHIuZ4tKw+/wvWeqv9079ervjj+2vRTX+7p7k34Ij1Ht0dLd33GnkVPne3om/DI/itPfLS18vx7v1/6Dm7b8gG9a0UjUfdLmbt75qIDO2pCHcdemHD8QqI2sg5PnEmIL3dWNExraH1z5tHxoGiH+9VY3zvjzkx69JneEnFd4vVl0l6w7GPfRf7c3W/3y/cX+R6h7mUSAAA=";

  /**
   * @param response the HTTP servlet response
   */
  public static void sendDAPResponse(HttpServletRequest request, final HttpServletResponse response,
                                     CookieReader cookieReader, IEndUserContext endUserContext,
                                     ContainerRequestContext requestContext)
          throws URISyntaxException, IOException {

    String cguid = cookieReader.getCguid(requestContext).substring(0, Constants.CGUID_LENGTH);
    String guid = cookieReader.getGuid(requestContext).substring(0, Constants.GUID_LENGTH);

    URIBuilder uriBuilder = new URIBuilder()
            .setScheme(ApplicationOptions.getInstance().getDapClientProperties().getProperty("uri.schema"))
            .setHost(ApplicationOptions.getInstance().getDapClientProperties().getProperty("uri.host"))
            .setPath(ApplicationOptions.getInstance().getDapClientProperties().getProperty("uri.path"));

    setRequestParameters(uriBuilder, request);
    setRvrId(uriBuilder, System.currentTimeMillis());
    setReferrer(uriBuilder, request);
    setGeoInfo(uriBuilder, request, endUserContext);
    setCguid(uriBuilder, cguid);
    setGuid(uriBuilder, guid);
    setUdid(uriBuilder, endUserContext);
    setMgInfo(uriBuilder, cguid, endUserContext);
    setUserInfo(uriBuilder, cguid);
    callDAPResponse(uriBuilder, response);
  }

  private static void setMgInfo(URIBuilder uriBuilder, String cguid, IEndUserContext endUserContext) {
    boolean isBot = isBotByIp(endUserContext.getIPAddress()) || isBotByUserAgent(endUserContext.getUserAgent());
    if (isBot) {
      uriBuilder.setParameter(MGVALUE, DEFAULT_MGVALUE);
      uriBuilder.setParameter(MGVALUEREASON, String.valueOf(MgvalueReason.BOT.getId()));
      return;
    }
    ImmutablePair<String, MgvalueReason> mgvalueAndMgvalueReason = getMgvalueAndMgvalueReason(cguid);
    uriBuilder.setParameter(MGVALUE, mgvalueAndMgvalueReason.left);
    uriBuilder.setParameter(MGVALUEREASON, String.valueOf(mgvalueAndMgvalueReason.right.getId()));

    if (!mgvalueAndMgvalueReason.left.equals(DEFAULT_MGVALUE)) {
      return;
    }
    String mid = getMid();
    if (StringUtils.isEmpty(mid)) {
      // replace mgvaluereason
      uriBuilder.setParameter(MGVALUEREASON, String.valueOf(MgvalueReason.TRUST_CALL_ERROR.getId()));
      return;
    }
    uriBuilder.setParameter(Constants.MID, mid);
  }

  private static ImmutablePair<String, MgvalueReason> getMgvalueAndMgvalueReason(String cguid) {
    cguid = "49bb481e14d0af4285d7b4b7fffffff6";
    String mgvalue = null;
    MgvalueReason mgvalueReason = null;
    Configuration config = ConfigurationBuilder.newConfig("idlinksvc.mktCollectionClient", "urn:ebay-marketplace-consumerid:bf59aef8-25de-4140-acc5-2d7ddc290ecb");
    Client mktClient = GingerClientBuilder.newClient(config);
    String endpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);

    try (Response ress = mktClient.target(endpoint).path("/idlink")
            .queryParam("id", cguid)
            .queryParam("type", Constants.CGUID)
            .request()
            .header("Authorization", "Bearer v^1.1#i^1#r^0#f^0#I^3#p^1#t^H4sIAAAAAAAAAO1Yf2wTVRxvt24E5jAqCg6M9YAYXa733vXa3p20UOh+1ME2aDcHCsv1+rqd6901917ZuqgZUwZRgzFqYgRj+R0MxiBqCAFRIwkJLiAkhCgKMUSICYnTGOIf/rjryugm8mtDZmL/ad573/d93+/n8/289+6BntKJj/bV9l0st08oyvaAniK7HZaBiaUllZOLiypKbKDAwJ7tmdXj6C0+PwdLajIlLkE4pWsYObvUpIbFXKefShuaqEtYwaImqQiLRBYjwUULRdYFxJShE13Wk5QzHPJT0MvzPuiLcT7kAZLH7NQuuYzqfiomIyhAifO6WR74kGyOY5xGYQ0TSSN+igVQoCGgWW8U8CIURJZ38R7PMsrZjAys6Jpp4gJUIBetmJtrFIR69UgljJFBTCdUIBysjjQEw6Gq+ugcpsBXIA9DhEgkjYe3Fuhx5GyWkml09WVwzlqMpGUZYUwxgcEVhjsVg5eCuYnwc0jzXt4rQ4nlOJjwxRNoTKCs1g1VIlePw+pR4nQiZyoijSgkcy1ETTRiTyOZ5Fv1potwyGn9LU5LSSWhIMNPVc0PLg02NlIBa3UUkzK0KhkdiKSSkoxo2ayhtIoMJS4CNwdlTpZpLycgmotBN83HBIGWkVtgUZzjYkDIBzG4Up6CEVEs0LW4YgGKnfU6mY/MjNBI3EABbqZRg9ZgBBPEinbIji/EFy6zCB9kOE3aNYtzpJogOXPNa7MzNJsQQ4mlCRryMHIgB5+fklIpJU6NHMzVab60urCfaickJTJMZ6el9U5Xp9ulG20MCwBkWhYtjMjtSJWoIXulYMI/GNNKLhXZrDvTXiSZlBlLl1nHZgBaGxWAkBe8MI/78LACI3v/1lGQMzNcLWOlnrhHkjkvK3DA2osSnrFQTyBfwMy16tftSbBuPoHouFdI0JyQSNAxT9xLwwRCAKFYTBb4/0V0YzKIINlA5Pp18G9ooH1lqLMFIzenaky8mmuphjUch6oXPl7X1FHbDTubauvCYbYSLank/derlCsnL+sp1KgnFTlTiICl9duPAsSkUTJIZlQZYivDccauOR+bDqSU4rKE5pJ1ldElc5tnctEyMQmjeeb2bNIiWQoZFQDBVCqsqmkixZIofAP7/fjb66+YnmLelMZVTiaZg6wq8cErjitHrQuvlF0GwnraMG93rgbrVI/qHUgz90Fi6MkkMppHh4RF9E3wa2n9VuIxhsfJzeFyg/ec/0jdy0nFLK/W8ZbZbWdbkcbZWQ49Phb4fIKXH1VeC3J8RzOjOswcq/bdggxrdUxQ/Bbc2JnhTwsBW+4He+0fgV77riK7HTBgNpwJHiotbnIU31GBFYJcipRwYaVNMz+ZDeTqQJmUpBhFpXblOVbvLnjMyC4H04aeMyYWw7KCtw0w4/JICbxzajkUIGC9gIcCyy8DMy+POuB9jinBpx7blWaLjn6wZW/2+Gu7W9d/epoH5UNGdnuJzdFrt/Udyl4sq1v06ieO5d51bQPddQNfHVnvPrSXk8/OZbp+fPYY0bOpkK1iTnfX+ZM/nAyXDmz4bef+bQNbd2cerDh68c0l2uYD98+YfM8jE4q//DwzfdsbW/q2T+nf2qw+I++899ThSS8E1nxdE/rstO3nrIi2/3782+mtd8mNmw7Sje+dudu+7ru1VR9+P7vlyIWpKPTTixsHZi19e0+USUafn7zxVy00CdWHXp++v3R2pH/uy5XipnNffPN+ZFNk9YVX1q7+ZeOBto4dJxafPfdkzYoH3O9sX32i5uNp8zavW6E8/O7pSeXqYXKsaRf/0r4/d+zpWL/liZNryoqyrWfe+mNFT/9Bz4X5pzbsW2Xb2T9I318QCXYkZhIAAA==")
            .get()) {
      switch (ress.getStatus()) {
        case HttpStatus.SC_NOT_FOUND:
          mgvalue = DEFAULT_MGVALUE;
          mgvalueReason = MgvalueReason.NEW_USER;
          break;
        case HttpStatus.SC_OK:
          String msg = ress.readEntity(String.class);
          JSONObject obj = new JSONObject(msg);
          JSONArray type = (JSONArray) obj.get("identityLinking");
          int size = type.length();
          for (int j = 0; j < size; j++) {
            JSONObject first = (JSONObject) type.get(j);
            String value = first.getString("type");
            if (value.equalsIgnoreCase("MGVALUE")) {
              JSONArray arr = first.getJSONArray("ids");
              // fetch the last inserted mgvalue
              mgvalue = arr.get(arr.length() - 1).toString();
              mgvalueReason = MgvalueReason.SUCCESS;
              break;
            }
          }
          if (mgvalue == null) {
            mgvalue = DEFAULT_MGVALUE;
            mgvalueReason = MgvalueReason.NEW_USER;
          }
          break;
        default:
          mgvalue = DEFAULT_MGVALUE;
          mgvalueReason = MgvalueReason.IDLINK_CALL_ERROR;
      }
    } catch(Exception exception) {
      mgvalue = DEFAULT_MGVALUE;
      mgvalueReason = MgvalueReason.IDLINK_CALL_ERROR;
    }

    return new ImmutablePair<>(mgvalue, mgvalueReason);
  }

  // TODO sync with dap
  private static void setMid(URIBuilder uriBuilder) {
    String mid = getMid();
    if (!StringUtils.isEmpty(mid)) {
      uriBuilder.setParameter(Constants.MID, mid);
    }
  }

  private static String getMid() {
    String mId = null;
    LookupBoHelperCfg.initialize();
    try {
      DeviceFingerPrintClient dfpClient = DeviceFingerPrintClientFactory.getDeviceFingerPrintClient();
      mId = dfpClient.getMid();
    } catch (Exception e) {
      LOGGER.info(e.getMessage());
    }
    return mId;
  }

  private static void setCguid(URIBuilder uriBuilder, String cguid) {
    uriBuilder.setParameter(Constants.CGUID, cguid);
  }

  private static void setGuid(URIBuilder uriBuilder, String guid) {
    uriBuilder.setParameter(Constants.GUID, guid);
  }

  // TODO format user profile
  private static void setUserInfo(URIBuilder uriBuilder, String cguid) {
    Configuration config = ConfigurationBuilder.newConfig("bullseyesvc.mktCollectionClient", "urn:ebay-marketplace-consumerid:bf59aef8-25de-4140-acc5-2d7ddc290ecb");
    Client mktClient = GingerClientBuilder.newClient(config);
    String endpoint = (String) mktClient.getConfiguration().getProperty(EndpointUri.KEY);

    try (Response ress = mktClient.target(endpoint).path("/timeline")
            .queryParam("modelid","911")
            .queryParam("cguid", "de22859016c0ada3c18276d3ef68d372")
            .queryParam("attrs", "EU_LOYALTY_SEGMENT,LastItemsViewed2,LastItemsWatched2,LastItemsBidOrBin,LastItemsLost2,LastItemsPurchased2,LastItemsBidOn2,LastQueriesUsed,CouponData,MaritalStatus,NumChildren,EstIncome,Gender,Occupation,Age,LeftNegativeFeedBack,AdChoice,GDPRConsent,HasUserFiledINR,HasUserContactedCS,LastCategoriesAccessed_Agg,MainCategories")
            .request()
            .header("Authorization", "Bearer v^1.1#i^1#p^1#r^0#f^0#I^3#t^H4sIAAAAAAAAAO1Ye2wURRjvvQoVK5ooLzEei5hi3d2Zvb3jduEOjz7oldIWeiUtgnR3b65de7d72ZnjOBqaUhXjg0CMoIkBqiaIzwQiIFGaEIKQENREBU1I0D/EosZAQiJGCO5ej3KtyKugNfH+uczMN9983+/3/WZmB3QVFj2ypmrNb8W2UfaeLtBlt9ngGFBU6Cq9y2Gf5CoAeQa2nq6Hupzdjr5ZWErEk+JChJO6hpF7RSKuYTHbGaBShibqElaxqEkJhEWiiA2h+TUixwAxaehEV/Q45Q6XByjJ4+NnAL9vBgSIE3xes1e75DOiByjohYKs+KSYh0eSLwbNcYxTKKxhImkkQHEACjQENMdHgE/keNELGD8UFlPuRcjAqq6ZJgyggtlwxexcIy/Wq4cqYYwMYjqhguFQZUNdKFxeURuZxeb5CuZwaCASSeHBrTI9ityLpHgKXX0ZnLUWG1KKgjCm2GD/CoOdiqFLwdxE+FmoYwpAUeT3KrKHlyVFviVQVupGQiJXj8PqUaN0LGsqIo2oJHMtRE005CeRQnKtWtNFuNxt/S1ISXE1piIjQFXMCTWH6uupoLU6kqUMnZCMdkSScUlBtGLWUCqBDDUqAg8PFV5RaB8vIJqXoYf2y4JAK8gjcCjK8zIQckH0r5SjYEgUZboWVS1AsbtWJ3OQmREaihvMw800qtPqjFCMWNEO2Pny8fUutgjvZzhF2jSLc5QwQXJnm9dmZ2A2IYYqpwga8DB0IAufKblkUo1SQwezdZorrRU4QLURkhRZNm1pPZ1m0h5GN1pZDgDINs2vaVDaUEKiBuzVyxP+zphWs6koyJyFVZFkkmYsK8w6NgPQWqkghH7BB3O4Dw4rOLT3Lx15ObOD1XKr1OMxS4gHwMcDr9ePrCyGr55groDZa9WvxxvjPP4YoqM+IUbzQixGy96oj4YxhABCsqwI/v9FdGMyaECKgch16+Af0UDb8vJ0E0YePqGx0Uq+qRLO5XlUWVM9r7G9aiVMN1bNC4e5UrSw1B+4XqVcOXlFT6J6Pa4qmTwELK2PABQgJvWSQTLDyhBbGY4wds352HQgJVXGEhqj6AlWl8xtns1Gy8oSRo+Z27NJi2QpZFgAhJLJcCKRIpIcR+Hr3+9H4F5/xfRU86Y0onIyyexnVY32X3GYLLUMXq4wBsJ6yjBvd0yddapH9HakmfsgMfR4HBmLhoeERfSN82tp/bbicQuPk5vD5cbuOf+Vulfiqlley0ZaZv8626o0ws5y6J1hsu3nBDCsvMqyfEcywznMnKv33o4Mq3RMUPQ23NjZwW8LwYLsD3bbdoJu23a7zQZYMA1OBVMKHY1Ox52TsEoQo0oxBqutmvnJbCCmHWWSkmrYC21qJ6evzHvN6FkKJgy8ZxQ54Ji8xw0w+fKIC44dXwwFCDge+DjeCxaDqZdHnXCc895DLrl3VdOG0zsP/7w7Y99aHelNvweKB4xsNleBs9tWMKH2UEtJn8MdJh/5Xa5TDxtrm/1FZS/83vH1+M6ZdTtO7z5fsbFUemP/Ezte/IOufrT++DObg+s2fx85PuqTiyWzv734afsvvcwG+9tn724euyvyyrl9px5nXz+89K1vNtq+2rLvs5pp5fMa+w50LHvWrSw7d//ovp7WA2e2bp7yXGfJiZ9OJh54eU/H9NNLp0W6D2+Z0Hlw+8lG549LvphYktzVUqg575h+5ssf7tt0wt2yv+1gbPL6dRd3vP88d6z7naDj1Z4LPjj7/NELzfyHZUfHdTAT2777dfQqvKR3+dj1c7adWrD26QPbJ53d1lT08T3VS4o/ePClPcee0va+1se9u/rzI2/OPNLCwk2pucUl/fT9CV8UJhhnEgAA")
            .get()) {

    } catch(Exception exception) {

    }
  }

  private static void setUdid(URIBuilder uriBuilder, IEndUserContext endUserContext) {
    uriBuilder.setParameter(Constants.UNIQUE_DEVICE_ID, endUserContext.getDeviceId());
  }

  // TODO send rvr id to MCS
  private static void setRvrId(URIBuilder uriBuilder, long startTime) {
    uriBuilder.setParameter(Constants.RVR_ID, String.valueOf(getRvrId(startTime)));
  }

  /**
   * Append all request parameter to url
   * @param uriBuilder dap request url
   * @param request client request
   */
  private static void setRequestParameters(URIBuilder uriBuilder, HttpServletRequest request) {
    Map<String, String[]> params = request.getParameterMap();
    MultiValueMap<String, String> parameters = new LinkedMultiValueMap<>();
    for (Map.Entry<String, String[]> param : params.entrySet()) {
      String[] values = param.getValue();
      for (String value: values) {
        parameters.add(param.getKey(), value);
      }
    }

    parameters.remove(Constants.IPN);
    parameters.remove(Constants.MPT);

    parameters.forEach((name, values) -> {
      if (name.toUpperCase().startsWith(Constants.ICEP_PREFIX)) {
        values.forEach(value -> uriBuilder.setParameter(name.substring(Constants.ICEP_PREFIX.length()), value));
      } else {
        values.forEach(value -> uriBuilder.setParameter(name, value));
      }
    });
  }

  // TODO copy isBot from ImkDapDump
  private static boolean getIsBot() {
    return false;
  }

  private static void callDAPResponse(URIBuilder uriBuilder, HttpServletResponse response) throws URISyntaxException, IOException {
    HttpContext context = HttpClientContext.create();
    HttpGet httpget = new HttpGet(uriBuilder.build());

    try (CloseableHttpClient httpclient = HttpClients.createDefault();
         CloseableHttpResponse dapResponse = httpclient.execute(httpget, context);
         OutputStream os = response.getOutputStream()) {
      for (Header header : dapResponse.getAllHeaders()) {
        response.setHeader(header.getName(), header.getValue());
      }
      byte[] bytes = EntityUtils.toByteArray(dapResponse.getEntity());
      os.write(bytes);
    }
  }

  private static void setReferrer(URIBuilder uriBuilder, HttpServletRequest request) {
    String referrer = request.getHeader(Constants.REFERER);
    if (referrer == null) {
      return;
    }
    uriBuilder.setParameter(Constants.REF_URL, referrer);
    String referrerDomain = getHostFromUrl(referrer);
    if (referrerDomain == null) {
      return;
    }
    uriBuilder.setParameter(Constants.REF_DOMAIN, referrerDomain);
  }

  private static String getCountryFromBrowserLocale(HttpServletRequest request) {
    String countryCode;

    String acceptLangs;
    String[] acceptLangsArray = null;
    String localeName;

    acceptLangs = request.getHeader(Constants.HTTP_ACCEPT_LANGUAGE);

    if (!StringUtils.isEmpty(acceptLangs)) {
      acceptLangsArray = acceptLangs.split(StringConstants.COMMA);
    }

    if (acceptLangsArray != null && acceptLangsArray.length > 0) {
      localeName = acceptLangsArray[0];
      Locale locale = convertLocaleNameToLocale(localeName);
      if (locale != null) {
        countryCode = locale.getCountry();
        if (isValidCountryCode(countryCode)) {
          return countryCode;
        }
      }
    }
    return null;
  }

  private static boolean isValidCountryCode(String countryCode) {
    if (countryCode == null) {
      return false;
    }
    return countryCode.length() == Constants.ISO_COUNTRY_CODE_LENGTH;
  }

  private static Locale convertLocaleNameToLocale(String localeName) {
    String[] localeNamePieces;
    String langCode;
    String countryCode = StringConstants.EMPTY;

    if (localeName != null && localeName.trim().length() > 0) {
      localeNamePieces = localeName.split(StringConstants.HYPHEN);
      langCode = localeNamePieces[0];
      if (localeNamePieces.length > 1) {
        countryCode = localeNamePieces[1];
      }
      return new Locale(langCode, countryCode);
    }
    return null;
  }

  private static String getHostFromUrl(String url) {
    try {
      URL u = new URL(url);
      return u.getHost().toLowerCase();
    } catch (Exception e) {
      return null;
    }
  }

  // TODO
  private static long getRvrId(long startTime) {
    return 2056960579986L;
  }

  private static void setGeoInfo(URIBuilder uriBuilder, HttpServletRequest request, IEndUserContext endUserContext) {
    Map<String, String> lbsParameters = getLBSParameters(endUserContext.getIPAddress());

    String countryFromBrowserLocale = getCountryFromBrowserLocale(request);
    if (!StringUtils.isEmpty(countryFromBrowserLocale)) {
      uriBuilder.setParameter(LBSConstants.GEO_COUNTRY_CODE, countryFromBrowserLocale);
      lbsParameters.remove(LBSConstants.GEO_COUNTRY_CODE);
    }
    lbsParameters.forEach(uriBuilder::setParameter);
  }

  // TODO remove hacked ip
  private static Map<String, String> getLBSParameters(String ip) {
    Map<String, String> map = new HashMap<>();
    ip = "155.94.176.242";

    LBSQueryResult lbsResponse = LBSClient.getInstance().getLBSInfo(ip, TOKEN);
    if (lbsResponse == null) {
      return map;
    }
    map.put(LBSConstants.GEO_COUNTRY_CODE, lbsResponse.getIsoCountryCode2());
    map.put(LBSConstants.GEO_DMA, lbsResponse.getStateCode());
    map.put(LBSConstants.GEO_CITY, lbsResponse.getCity());
    map.put(LBSConstants.GEO_ZIP_CODE, lbsResponse.getPostalCode());
    map.put(LBSConstants.GEO_LATITUDE, String.valueOf(lbsResponse.getLatitude()));
    map.put(LBSConstants.GEO_LONGITUDE, String.valueOf(lbsResponse.getLongitude()));
    map.put(LBSConstants.GEO_METRO_CODE, lbsResponse.getMetroCode());
    map.put(LBSConstants.GEO_AREA_CODE, lbsResponse.getAreaCodes());
    return map;
  }

  /**
   * Check if this request is bot with userAgent
   * @param userAgent alias for user agent
   * @return is bot or not
   */
  private static boolean isBotByUserAgent(String userAgent) {
    return isBot(userAgent, userAgentBotDawgDictionary);
  }

  /**
   * Check if this request is bot with ip
   * @param ip alias for remote ip
   * @return is bot or not
   */
  private static boolean isBotByIp(String ip) {
    return isBot(ip, ipBotDawgDictionary);
  }

  private static boolean isBot(String info, DawgDictionary dawgDictionary) {
    if (StringUtils.isEmpty(info)) {
      return false;
    }
    Dawg dawg = new Dawg(dawgDictionary);
    Map result = dawg.findAllWords(info.toLowerCase(), false);
    return !result.isEmpty();
  }
}
