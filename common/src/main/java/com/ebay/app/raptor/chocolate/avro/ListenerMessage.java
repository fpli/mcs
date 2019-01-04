package com.ebay.app.raptor.chocolate.avro;

import com.ebay.app.raptor.chocolate.avro.versions.ListenerMessageV1;
import com.ebay.app.raptor.chocolate.avro.versions.ListenerMessageV2;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ListenerMessage extends ListenerMessageV2 {
  private static Schema getV1Schema() {
    return ListenerMessageV1.getClassSchema();
  }

  // Avro reader (threadsafe, therefore static)
  private final static DatumReader<ListenerMessage> reader = new SpecificDatumReader<>(
          getClassSchema());

  // Avro reader that reads previous version of schema (threadsafe, therefore static)
  private final static DatumReader<ListenerMessageV1> readerV1 = new SpecificDatumReader<>(
          getV1Schema(), ListenerMessageV1.getClassSchema());

  // Avro writer (threadsafe, therefore static)
  private final static DatumWriter<ListenerMessage> writer = new SpecificDatumWriter<>(
          getClassSchema());

  public ListenerMessage() {
  }

  public ListenerMessage(Long snapshot_id, Long timestamp, String user_id, String guid,
                         String cguid, String remote_ip, String referer, Long publisher_id,
                         Long campaign_id, String request_headers, String uri, String response_headers,
                         ChannelAction channel_action, ChannelType channel_type, HttpMethod http_method,
                         String snid, Boolean is_tracked) {
    super(snapshot_id, timestamp, user_id, guid, cguid, remote_ip, referer, publisher_id,
            campaign_id, request_headers, uri, response_headers, channel_action, channel_type,
            http_method, snid, is_tracked);
  }

  public static ListenerMessage readFromJSON(String json) throws IOException {
    JsonDecoder decoder;
    ListenerMessage datum = new ListenerMessage();
    try {
      decoder = DecoderFactory.get().jsonDecoder(getClassSchema(), json);
      datum = reader.read(datum, decoder);
      return datum;
    } catch (AvroRuntimeException e) {
      // Nothing to do, need to try the upgrading reader first
    }

    // fallback to read V1
    decoder = DecoderFactory.get().jsonDecoder(getV1Schema(), json);
    ListenerMessageV1 datumV1 = new ListenerMessageV1();
    datumV1 = readerV1.read(datumV1, decoder);
    datum = new ListenerMessage(datumV1.getSnapshotId(), datumV1.getTimestamp(), "", "", "", "", "",
            datumV1.getPublisherId(), datumV1.getCampaignId(), datumV1.getRequestHeaders(), datumV1.getUri(),
            datumV1.getResponseHeaders(), datumV1.getChannelAction(), datumV1.getChannelType(),
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
