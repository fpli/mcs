package com.ebay.app.raptor.chocolate.avro;

import com.ebay.app.raptor.chocolate.avro.versions.BehaviorMessageV0;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by jialili1 on 8/3/20
 */
public class BehaviorMessage extends BehaviorMessageV0 {

  // Avro reader (threadsafe, therefore static)
  private final static DatumReader<BehaviorMessage> reader = new SpecificDatumReader<>(
      getClassSchema());

  // Avro writer (threadsafe, therefore static)
  private final static DatumWriter<BehaviorMessage> writer = new SpecificDatumWriter<>(
      getClassSchema());

  public BehaviorMessage() {
  }

  public BehaviorMessage(String guid, String adguid, Long eventTimestamp, String sid, Integer pageId, String pageName,
                         String eventFamily, String eventAction, String userId, String siteId, String sessionId,
                         String snapshotId, String seqNum, Integer rdt, String refererHash, String urlQueryString,
                         String webServer, Integer bot, String clientIP, String remoteIP, String agentInfo,
                         String appId, String appVersion, String osVersion, String cobrand, String deviceFamily,
                         String deviceType, String browserVersion, String browserFamily, String osFamily,
                         String enrichedOsVersion, Map<String,String> applicationPayload, String rlogid,
                         Map<String, String> clientData, String channelAction, String channelType, String dispatchId,
                         List<Map<String,String>> data) {
    super(guid, adguid, eventTimestamp, sid, pageId, pageName, eventFamily, eventAction, userId, siteId, sessionId,
        snapshotId, seqNum, rdt, refererHash, urlQueryString, webServer, bot, clientIP, remoteIP, agentInfo, appId,
        appVersion, osVersion, cobrand, deviceFamily, deviceType, browserVersion, browserFamily, osFamily,
        enrichedOsVersion, applicationPayload, rlogid, clientData, channelAction, channelType, dispatchId, data);
  }

  public static BehaviorMessage readFromJSON(String json) throws IOException {
    JsonDecoder decoder = DecoderFactory.get().jsonDecoder(getClassSchema(), json);

    return decode(decoder);
  }

  public static BehaviorMessage decodeRheos(Schema rheosHeaderSchema,
                                            byte[] data) throws IOException {
    DatumReader<GenericRecord> rheosHeaderReader = new GenericDatumReader<>(
        rheosHeaderSchema);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    // skips the rheos header
    rheosHeaderReader.read(null, decoder);

    return decode(decoder);
  }

  public static <D extends Decoder> BehaviorMessage decode(D decoder) throws IOException{

    BehaviorMessage datum = new BehaviorMessage();
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
