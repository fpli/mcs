package com.ebay.traffic.chocolate.flink.nrt.function;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.Properties;

public abstract class SherlockioMetricsCompatibleRichFlatMapFunction<IN, OUT> extends RichFlatMapFunction<IN, OUT> {
    @Override
    public void open(Configuration parameters) throws Exception {
        Properties properties = PropertyMgr.getInstance()
                .loadProperty(PropertyConstants.APPLICATION_PROPERTIES);
        SherlockioMetrics.init(properties.getProperty(PropertyConstants.SHERLOCKIO_NAMESPACE), "UnifiedTrackingBotTransformApp",
                properties.getProperty(PropertyConstants.SHERLOCKIO_ENDPOINT), properties.getProperty(PropertyConstants.SHERLOCKIO_USER));
    }

    @Override
    public void close() throws Exception {
        SherlockioMetrics.getInstance().close();
    }

    @Override
    public abstract void flatMap(IN var1, Collector<OUT> var2) throws Exception;
}
