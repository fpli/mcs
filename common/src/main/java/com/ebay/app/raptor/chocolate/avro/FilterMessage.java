package com.ebay.app.raptor.chocolate.avro;

import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV2;
import com.ebay.app.raptor.chocolate.avro.versions.FilterMessageV3;
import com.ebay.app.raptor.chocolate.common.ShortSnapshotId;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class FilterMessage extends FilterMessageV3 {
  public static Schema getClassSchema() {
    return SCHEMA$;
  }

  private static Schema getV2Schema() {
    return FilterMessageV2.getClassSchema();
  }

  // Avro reader (threadsafe, therefore static)
  private final static DatumReader<FilterMessage> reader = new SpecificDatumReader<>(
      getClassSchema());

  // Avro reader that reads previous version of schema (threadsafe, therefore static)
  private final static DatumReader<FilterMessageV2> readerV2 = new SpecificDatumReader<>(
      getV2Schema(), FilterMessageV2.getClassSchema());

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
    JsonDecoder decoder = DecoderFactory.get().jsonDecoder(getClassSchema(), json);
    JsonDecoder decoderV2 = DecoderFactory.get().jsonDecoder(getV2Schema(), json);

    return decode(decoder, decoderV2);
  }

  public static FilterMessage decodeRheos(Schema rheosHeaderSchema,
                                          byte[] data) throws IOException {
    DatumReader<GenericRecord> rheosHeaderReader = new GenericDatumReader<>(
        rheosHeaderSchema);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    // skips the rheos header
    rheosHeaderReader.read(null, decoder);

    BinaryDecoder decoderV2 = DecoderFactory.get().binaryDecoder(data, null);
    // skips the rheos header
    rheosHeaderReader.read(null, decoderV2);

    return decode(decoder, decoderV2);
  }

  public static <D extends Decoder> FilterMessage decode(D decoder, D decoderV2) throws IOException {

    FilterMessage datum = new FilterMessage();
    try {
      datum = reader.read(datum, decoder);
      return datum;

    } catch (Exception e) {
      // Nothing to do, need to try the upgrading reader first
    }

    // fallback to read V2
    FilterMessageV2 datumV2 = new FilterMessageV2();
    datumV2 = readerV2.read(datumV2, decoderV2);
    ShortSnapshotId shortSnapshotId = new ShortSnapshotId(datumV2.getSnapshotId().longValue());
    datum = new FilterMessage(datumV2.getSnapshotId(), shortSnapshotId.getRepresentation(), datumV2.getTimestamp(),
        -1L, "", "", "", "", "", -1L, "", "",
        datumV2.getPublisherId(), datumV2.getCampaignId(),
        -1L, "", -1L, -1L,
        datumV2.getRequestHeaders(), datumV2.getUri(), datumV2.getResponseHeaders(),
        datumV2.getRtRuleFlags(), datumV2.getNrtRuleFlags(), datumV2.getChannelAction(), datumV2.getChannelType(),
        datumV2.getHttpMethod(), datumV2.getSnid(), datumV2.getIsTracked());
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
