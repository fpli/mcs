package com.ebay.app.raptor.chocolate.avro;

import com.ebay.app.raptor.chocolate.avro.versions.UnifiedTrackingMessageV0;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Created by jialili1 on 11/5/20
 */
public class UnifiedTrackingMessage extends UnifiedTrackingMessageV0 {

  // Avro reader (threadsafe, therefore static)
  private final static DatumReader<UnifiedTrackingMessage> reader = new SpecificDatumReader<>(
      getClassSchema());

  // Avro writer (threadsafe, therefore static)
  private final static DatumWriter<UnifiedTrackingMessage> writer = new SpecificDatumWriter<>(
      getClassSchema());

  public UnifiedTrackingMessage() {
  }

  public UnifiedTrackingMessage(String eventId, String producerEventId, Long eventTs, Long producerEventTs, String rlogId,
                                String trackingId, Long userId, String publicUserId, Long encryptedUserId, String guid,
                                String idfa, String gadid, String deviceId, String channelType, String actionType,
                                String partnerId, String campaignId, String rotationId, Integer siteId, String url,
                                String referer, String userAgent, String deviceFamily, String deviceType,
                                String browserFamily, String browserVersion, String osFamily, String osVersion,
                                String appVersion, String service, String server, String remoteIp, Integer pageId,
                                Integer geoId, Boolean isBot, Map<String,String> payload) {
    super(eventId, producerEventId, eventTs, producerEventTs, rlogId, trackingId, userId, publicUserId, encryptedUserId,
        guid, idfa, gadid, deviceId, channelType, actionType, partnerId, campaignId, rotationId, siteId, url, referer,
        userAgent, deviceFamily, deviceType, browserFamily, browserVersion, osFamily, osVersion, appVersion, service,
        server, remoteIp, pageId, geoId, isBot, payload);
  }

  public static UnifiedTrackingMessage readFromJSON(String json) throws IOException {
    JsonDecoder decoder = DecoderFactory.get().jsonDecoder(getClassSchema(), json);

    return decode(decoder);
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

  public String writeToJSON() throws IOException {
    return new String(writeToBytes());
  }

  public byte[] writeToBytes() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(getClassSchema(), out);
    this.writer.write(this, encoder);
    encoder.flush();
    return out.toByteArray();
  }
}
