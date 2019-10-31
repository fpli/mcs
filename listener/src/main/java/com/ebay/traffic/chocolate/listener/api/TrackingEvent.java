package com.ebay.traffic.chocolate.listener.api;

import com.ebay.app.raptor.chocolate.avro.ChannelType;
import com.ebay.app.raptor.chocolate.constant.ChannelActionEnum;
import com.ebay.app.raptor.chocolate.constant.ChannelIdEnum;
import org.apache.commons.lang3.Validate;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * @author airogers
 *
 */
public class TrackingEvent {

    private int version, channelID;
    private String collectionID;
    private ChannelActionEnum action;
    private Map<String, Object> payload;
    private ChannelType channel;

    /* some helpful class constants to be used inside and outside the class
     * these are all strings that are used in the API
     */
    public static final char CLICK = 'c';
    public static final char IMPRESSION = 'i';
    public static final char VIEWABLE = 'v';

    public static final String PAGE = "page";
    public static final String ITEM = "item";
    public static final String PRODUCT = "product";

    public static final String[] EBAY_HOSTS = {
            "ebay.com",
            "ebay.co.uk",
            "ebay.com.au",
            "ebay.de",
            "ebay.fr",
            "ebay.it",
            "ebay.es",
            "ebay.at",
            "ebay.be",
            "ebay.ca",
            "ebay.cn",
            "ebay.com.hk",
            "ebay.in",
            "ebay.ie",
            "ebay.co.jp",
            "ebay.com.my",
            "ebay.nl",
            "ebay.ph",
            "ebay.pl",
            "ebay.com.sg",
            "ebay.ch",
            "ebay.co.th",
            "ebay.vn"
    };
    public static final String DEFAULT_DESTINATION = "http://www.ebay.com";

    public static final int CURRENT_VERSION = 1;

    public TrackingEvent(URL url, Map<String, String[]> params) throws NumberFormatException {
      payload = new HashMap<>();
      parsePath(url.getPath());
      try {
        validateParams(params);
      } catch (UnsupportedEncodingException e){
        // ignored - see javadoc for parsePayload()
      }
    }

    private void validateParams(Map<String, String[]> params) throws UnsupportedEncodingException, NumberFormatException{
      if (params.isEmpty()) return;
      Object val;
      for (Map.Entry<String, String[]> entry : params.entrySet()) {
        if (entry.getKey().equals("item") || entry.getKey().equals("product")) {
          val = Long.parseLong(entry.getValue()[0]);
        } else
          val = entry.getValue()[0];
        payload.put(entry.getKey(), val);
      }
    }

    /* hard coded 1x1 gif pixel, for impression serving */
    public static byte[] pixel = { 0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x1, 0x0, 0x1, 0x0, (byte) 0x80, 0x0, 0x0, (byte)  0xff, (byte)  0xff,  (byte) 0xff, 0x0, 0x0, 0x0, 0x2c, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x1, 0x0, 0x0, 0x2, 0x2, 0x44, 0x1, 0x0, 0x3b };

    /**
     * Parses the various pieces of the URL path
     * @param path from the URL
     */
    private void parsePath(String path) {
        String[] parts = path.split("/");
        setVersionAndType(parts[1]); // parts[0] will be empty string because path starts with a "/"
        setChannelAndCollection(parts[2]);
    }

    /**
     * Parse, validate and set channel ID and Collection ID
     * @param linkID from the URL
     */
    private void setChannelAndCollection(String linkID) {
        Validate.matchesPattern(linkID, "^\\d+-\\w+$");

        String[] parts = linkID.split("-");
        channelID = Integer.valueOf(parts[0]);
        collectionID = parts[1];
        channel = getChannelType(String.valueOf(channelID));
    }

    private ChannelType getChannelType(String channel) {
      ChannelIdEnum channelId = ChannelIdEnum.parse(channel);
      return channelId.getLogicalChannel().getAvro();
    }

    /**
     * Parse, validate and set version and event type
     * @param versionAndType - two character string with format <version number><event type>
     */
    private void setVersionAndType(String versionAndType) {
        Validate.matchesPattern(versionAndType, "^\\d\\w$");

        version = Character.getNumericValue( versionAndType.charAt(0) );
        Validate.isTrue(version <= CURRENT_VERSION);

        char type = versionAndType.charAt(1);
        Validate.matchesPattern(String.valueOf(type), "[civ]");
        switch (type) {
        case 'c': 
            action = ChannelActionEnum.CLICK;
            break;
        case 'i': 
            action = ChannelActionEnum.IMPRESSION;
            break;
        case 'v': 
            action = ChannelActionEnum.VIMP;
            break;
        default: 
            Validate.isTrue(false, "Unknown action encountered - " + type);
        }
    }

    /**
     * Depending on the request type, this will either redirect or send a 1x1 gif
     * Theoretically, this could be an abstract method and there could be subclasses
     * of TrackingEvent that override this. However, there are currently only two
     * possible responses, so for simplicity they are both implemented here. If we
     * ever add a third response, this should be refactored.
     * @param response response to the client
     * @throws IOException in case of any errors
     */
    void respond(HttpServletResponse response) throws IOException {
        setCommonHeaders(response);
        if (ChannelActionEnum.CLICK.equals(action)) {
            redirect(response);
        }
        else {
            respondWithPixel(response);
        }
    }

    // TODO incorporate cguid library
    private void setCommonHeaders(HttpServletResponse response) {
        //Aidan ya goof
        //Cookie cookie = new Cookie("npii", "fixme with some guids please");
        //response.addCookie(cookie);
    }

    private void respondWithPixel(HttpServletResponse response) throws IOException {
        response.setContentType("image/gif");
        // Set no cache header to avoid cache on browser like Chrome
        response.setHeader("Pragma","no-cache");
        response.setContentLength(pixel.length);
        OutputStream out = response.getOutputStream();
        out.write(pixel);
        out.close();
    }

    private void redirect(HttpServletResponse response) throws IOException {
      URL url = new URL((String)payload.get(PAGE));
        // Use lowercase to avoid case sensitive
        String host = url.getHost().toLowerCase();
        String destination = response.encodeRedirectURL(DEFAULT_DESTINATION);

        if (host.contains("ebay")) {
            for (String validHost : EBAY_HOSTS) {
                // Ensure the host is end with the domains in the list
                if (host.endsWith(validHost)) {
                    destination = response.encodeRedirectURL(url.toString());
                }
            }
        }
        response.sendRedirect(destination);
    }

    public int getVersion() {
        return version;
    }

    public ChannelActionEnum getAction() {
        return action;
    }

    public int getChannelID() {
        return channelID;
    }

    public String getCollectionID() {
        return collectionID;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

  public ChannelType getChannel() {
    return channel;
  }
}