package com.ebay.app.raptor.chocolate.avro;

import com.ebay.app.raptor.chocolate.avro.versions.UnifiedTrackingRheosMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.IOException;
import java.util.Map;

/**
 * Created by jialili1 on 11/5/20
 */
public class UnifiedTrackingMessage extends UnifiedTrackingRheosMessage {

  // Avro reader (threadsafe, therefore static)
  private final static DatumReader<UnifiedTrackingMessage> reader = new SpecificDatumReader<>(
      getClassSchema());

  public UnifiedTrackingMessage() {
  }

  public UnifiedTrackingMessage(String eventId, String producerEventId, Long eventTs, Long producerEventTs, String rlogId,
                                String trackingId, Long userId, String publicUserId, Long encryptedUserId, String guid,
                                String idfa, String gadid, String deviceId, String channelType, String actionType,
                                String partner, String campaignId, String rotationId, Integer siteId, String url,
                                String referer, String userAgent, String deviceFamily, String deviceType,
                                String browserFamily, String browserVersion, String osFamily, String osVersion,
                                String appVersion, String appId, String service, String server, String remoteIp,
                                Integer pageId, Integer geoId, Boolean isBot, Map<String,String> payload) {
    super(eventId, producerEventId, eventTs, producerEventTs, rlogId, trackingId, userId, publicUserId, encryptedUserId,
        guid, idfa, gadid, deviceId, channelType, actionType, partner, campaignId, rotationId, siteId, url, referer,
        userAgent, deviceFamily, deviceType, browserFamily, browserVersion, osFamily, osVersion, appVersion, appId,
        service, server, remoteIp, pageId, geoId, isBot, payload);
  }

  public static UnifiedTrackingMessage decodeRheos(Schema rheosHeaderSchema,
                                                   byte[] data) throws IOException {
    DatumReader<GenericRecord> rheosHeaderReader = new GenericDatumReader<>(
        rheosHeaderSchema);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    // skips the rheos header
    rheosHeaderReader.read(null, decoder);

    return decode(decoder);
  }

  public static <D extends Decoder> UnifiedTrackingMessage decode(D decoder) throws IOException{

    UnifiedTrackingMessage datum = new UnifiedTrackingMessage();
    datum = reader.read(datum, decoder);

    return datum;
  }
}
