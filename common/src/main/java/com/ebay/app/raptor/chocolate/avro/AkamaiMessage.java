package com.ebay.app.raptor.chocolate.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.IOException;

/**
 * Created by jialili1 on 9/7/22
 */
public class AkamaiMessage extends AkamaiRheosMessage {
  // Avro reader (threadsafe, therefore static)
  private final static DatumReader<AkamaiMessage> reader = new SpecificDatumReader<>(
      getClassSchema());

  public AkamaiMessage() {
  }

  public AkamaiMessage(String version, String ewUsageInfo, String ewExecutionInfo, String country, String city,
                       String state, String cacheStatus, String customField, String turnAroundTimeMSec,
                       String transferTimeMSec, String cliIP, String statusCode, String reqHost, String reqMethod,
                       String bytes, String tlsVersion, String UA, String queryStr, String rspContentLen,
                       String rspContentType, String reqPath, String reqPort, String proto, String reqTimeSec,
                       String cp, String reqId, String tlsOverheadTimeMSec, String objSize, String uncompressedSize,
                       String overheadBytes, String totalBytes, String accLang, String cookie, String range,
                       String referer, String xForwardedFor, String maxAgeSec, String reqEndTimeMSec,
                       String errorCode, String dnsLookupTimeMSec, String billingRegion, String edgeIP,
                       String securityRules, String serverCountry, String streamId) {
    super(version, ewUsageInfo, ewExecutionInfo, country, city, state, cacheStatus, customField, turnAroundTimeMSec,
        transferTimeMSec, cliIP, statusCode, reqHost, reqMethod, bytes, tlsVersion, UA, queryStr, rspContentLen,
        rspContentType, reqPath, reqPort, proto, reqTimeSec, cp, reqId, tlsOverheadTimeMSec, objSize, uncompressedSize,
        overheadBytes, totalBytes, accLang, cookie, range, referer, xForwardedFor, maxAgeSec, reqEndTimeMSec,
        errorCode, dnsLookupTimeMSec, billingRegion, edgeIP, securityRules, serverCountry, streamId);
  }

  public static AkamaiMessage decodeRheos(Schema rheosHeaderSchema,
                                                   byte[] data) throws IOException {
    DatumReader<GenericRecord> rheosHeaderReader = new GenericDatumReader<>(
        rheosHeaderSchema);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    // skips the rheos header
    rheosHeaderReader.read(null, decoder);

    return decode(decoder);
  }

  public static <D extends Decoder> AkamaiMessage decode(D decoder) throws IOException{

    AkamaiMessage datum = new AkamaiMessage();
    datum = reader.read(datum, decoder);

    return datum;
  }
}
