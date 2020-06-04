package com.ebay.traffic.chocolate.flink.nrt.function;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.monitoring.ESMetrics;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Properties;

public abstract class ESMetricsCompatibleRichMapFunction<IN, OUT> extends RichMapFunction<IN, OUT> {

    @Override
    public void open(Configuration parameters) throws Exception {
//        Properties properties = PropertyMgr.getInstance()
//                .loadProperty(PropertyConstants.APPLICATION_PROPERTIES);
//        ESMetrics.init(properties.getProperty(PropertyConstants.ELASTICSEARCH_INDEX_PREFIX),
//                properties.getProperty(PropertyConstants.ELASTICSEARCH_URL));
    }

    @Override
    public void close() throws Exception {
//        ESMetrics.getInstance().close();
    }

    @Override
    public abstract OUT map(IN value) throws Exception;
}
