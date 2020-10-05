package com.ebay.traffic.chocolate.flink.nrt.app;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;

import java.io.IOException;

class CustomSource extends FromElementsFunction<String> implements ResultTypeQueryable<String> {
  public CustomSource(TypeSerializer<String> serializer, String... elements) throws IOException {
    super(serializer, elements);
  }

  public CustomSource(TypeSerializer<String> serializer, Iterable<String> elements) throws IOException {
    super(serializer, elements);
  }

  @Override
  public TypeInformation<String> getProducedType() {
    return Types.STRING;
  }
}
