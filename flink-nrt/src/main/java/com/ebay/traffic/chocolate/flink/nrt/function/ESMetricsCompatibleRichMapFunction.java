package com.ebay.traffic.chocolate.flink.nrt.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public abstract class ESMetricsCompatibleRichMapFunction<IN, OUT> extends RichMapFunction<IN, OUT> {

    @Override
    public void open(Configuration parameters) throws Exception {
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public abstract OUT map(IN value) throws Exception;
}
