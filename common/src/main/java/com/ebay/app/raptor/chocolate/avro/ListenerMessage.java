package com.ebay.app.raptor.chocolate.avro;

import com.ebay.app.raptor.chocolate.avro.versions.ListenerMessageV0;
import com.ebay.app.raptor.chocolate.avro.versions.ListenerMessageV1;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ListenerMessage extends ListenerMessageV1 {
  private static Schema getOldSchema() {
    return ListenerMessageV0.getClassSchema();
  }

  // Avro reader (threadsafe, therefore static)
  private final static DatumReader<ListenerMessage> reader = new SpecificDatumReader<>(
          getClassSchema());

  // Avro reader that reads previous version of schema (threadsafe, therefore static)
  private final static DatumReader<ListenerMessage> readerUpgrade = new SpecificDatumReader<>(
          getOldSchema(), getClassSchema());

  // Avro writer (threadsafe, therefore static)
  private final static DatumWriter<ListenerMessage> writer = new SpecificDatumWriter<>(
          getClassSchema());

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

    decoder = DecoderFactory.get().jsonDecoder(getOldSchema(), json);
    datum = readerUpgrade.read(datum, decoder);
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
