package com.ebay.app.raptor.chocolate.avro;

import com.ebay.app.raptor.chocolate.avro.versions.ListenerMessageV4;
import com.ebay.app.raptor.chocolate.avro.versions.ListenerMessageV3;
import com.ebay.app.raptor.chocolate.common.ShortSnapshotId;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ListenerMessage extends ListenerMessageV4 {
  private static Schema getV3Schema() {
    return ListenerMessageV3.getClassSchema();
  }

  // Avro reader (threadsafe, therefore static)
  private final static DatumReader<ListenerMessage> reader = new SpecificDatumReader<>(
      getClassSchema());

  // Avro reader that reads previous version of schema (threadsafe, therefore static)
  private final static DatumReader<ListenerMessageV3> readerV3 = new SpecificDatumReader<>(
      getV3Schema(), ListenerMessageV3.getClassSchema());

  // Avro writer (threadsafe, therefore static)
  private final static DatumWriter<ListenerMessage> writer = new SpecificDatumWriter<>(
      getClassSchema());

  public ListenerMessage() {
  }

  public ListenerMessage(Long snapshot_id, Long short_snapshot_id, Long timestamp, Long user_id, String guid,
                         String cguid, String remote_ip, String lang_cd, String user_agent, Long geo_id, String udid,
                         String referer, Long publisher_id, Long campaign_id, Long site_id, String landing_page_url,
                         Long src_rotation_id, Long dst_rotation_id, String request_headers, String uri,
                         String response_headers, ChannelAction channel_action, ChannelType channel_type,
                         HttpMethod http_method, String snid, Boolean is_tracked) {
    super(snapshot_id, short_snapshot_id, timestamp, user_id, guid, cguid, remote_ip, lang_cd, user_agent, geo_id, udid,
        referer, publisher_id, campaign_id, site_id, landing_page_url, src_rotation_id, dst_rotation_id, request_headers,
        uri, response_headers, channel_action, channel_type, http_method, snid, is_tracked);
  }

  public static ListenerMessage readFromJSON(String json) throws IOException {
    JsonDecoder decoder = DecoderFactory.get().jsonDecoder(getClassSchema(), json);
    JsonDecoder decoderV3 = DecoderFactory.get().jsonDecoder(getV3Schema(), json);

    return decode(decoder, decoderV3);
  }

  public static ListenerMessage decodeRheos(Schema rheosHeaderSchema,
                                            byte[] data) throws IOException {
    DatumReader<GenericRecord> rheosHeaderReader = new GenericDatumReader<>(
        rheosHeaderSchema);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    // skips the rheos header
    rheosHeaderReader.read(null, decoder);

    BinaryDecoder decoderV3 = DecoderFactory.get().binaryDecoder(data, null);
    // skips the rheos header
    rheosHeaderReader.read(null, decoderV3);

    return decode(decoder, decoderV3);
  }

  public static <D extends Decoder> ListenerMessage decode(D decoder, D decoderV3) throws IOException {

    ListenerMessage datum = new ListenerMessage();
    try {
      datum = reader.read(datum, decoder);
      return datum;
    } catch (Exception e) {
      // Nothing to do, need to try the upgrading reader first
    }

    // fallback to read V3
    ListenerMessageV3 datumV3 = new ListenerMessageV3();
    datumV3 = readerV3.read(datumV3, decoderV3);
    ShortSnapshotId shortSnapshotId = new ShortSnapshotId(datumV3.getSnapshotId().longValue());
    datum = new ListenerMessage(datumV3.getSnapshotId(), shortSnapshotId.getRepresentation(), datumV3.getTimestamp(),
        -1L, "", "", "", "", "", -1L, "", "",
        datumV3.getPublisherId(), datumV3.getCampaignId(),
        -1L, "", -1L, -1L,
        datumV3.getRequestHeaders(), datumV3.getUri(), datumV3.getResponseHeaders(),
        datumV3.getChannelAction(), datumV3.getChannelType(),
        datumV3.getHttpMethod(), datumV3.getSnid(), datumV3.getIsTracked());
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
