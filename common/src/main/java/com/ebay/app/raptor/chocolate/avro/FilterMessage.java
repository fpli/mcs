package com.ebay.app.raptor.chocolate.avro;

import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV1;
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV2;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class FilterMessage extends FilterMessageV2 {
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static Schema getV1Schema() {
    return FilterMessageV1.getClassSchema();
  }

  // Avro reader (threadsafe, therefore static)
  private final static DatumReader<FilterMessage> reader = new SpecificDatumReader<>(
      getClassSchema());

  // Avro reader that reads previous version of schema (threadsafe, therefore static)
  private final static DatumReader<FilterMessageV1> readerV1 = new SpecificDatumReader<>(
      getV1Schema(), FilterMessageV1.getClassSchema());

  // Avro writer (threadsafe, therefore static)
  private final static DatumWriter<FilterMessage> writer = new SpecificDatumWriter<>(
      getClassSchema());

  public FilterMessage() {
  }

  public FilterMessage(Long snapshot_id, Long short_snapshot_id, Long timestamp, Long user_id, String guid,
                       String cguid, String remote_ip, String lang_cd, String user_agent, Long geo_id, String udid,
                       String referer, Long publisher_id, Long campaign_id, Long site_id, String landing_page_url,
                       Long src_rotation_id, Long dst_rotation_id, String request_headers, String uri,
                       String response_headers, Long rt_rule_flags, Long nrt_rule_flags,
                       ChannelAction channel_action, ChannelType channel_type, HttpMethod http_method,
                       String snid, Boolean is_tracked) {
    super(snapshot_id, short_snapshot_id, timestamp, user_id, guid, cguid, remote_ip, lang_cd, user_agent, geo_id, udid,
        referer, publisher_id, campaign_id, site_id, landing_page_url, src_rotation_id, dst_rotation_id, request_headers,
        uri, response_headers, rt_rule_flags, nrt_rule_flags, channel_action, channel_type, http_method, snid, is_tracked);
  }

  public static FilterMessage readFromJSON(String json) throws IOException {
    JsonDecoder decoder;
    FilterMessage datum = new FilterMessage();
    try {
      decoder = DecoderFactory.get().jsonDecoder(getClassSchema(), json);
      datum = reader.read(datum, decoder);
      return datum;
    } catch (AvroRuntimeException e) {
      // Nothing to do, need to try the upgrading reader first
    }

    // fallback to read V1
    decoder = DecoderFactory.get().jsonDecoder(getV1Schema(), json);
    FilterMessageV1 datumV1 = new FilterMessageV1();
    datumV1 = readerV1.read(datumV1, decoder);
    datum = new FilterMessage(datumV1.getSnapshotId(), -1L, datumV1.getTimestamp(),
        -1L, "", "", "", "", "", -1L, "", "",
        datumV1.getPublisherId(), datumV1.getCampaignId(),
        -1L, "",  -1L, -1L,
        datumV1.getRequestHeaders(), datumV1.getUri(), datumV1.getResponseHeaders(),
        datumV1.getRtRuleFlags(), datumV1.getNrtRuleFlags(), datumV1.getChannelAction(), datumV1.getChannelType(),
        datumV1.getHttpMethod(), datumV1.getSnid(), datumV1.getIsTracked());
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
