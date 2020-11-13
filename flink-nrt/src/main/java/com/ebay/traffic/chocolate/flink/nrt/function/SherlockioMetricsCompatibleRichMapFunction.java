package com.ebay.traffic.chocolate.flink.nrt.function;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.util.PropertyMgr;
import com.ebay.traffic.sherlockio.pushgateway.SherlockioMetrics;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Properties;

public abstract class SherlockioMetricsCompatibleRichMapFunction <IN, OUT> extends RichMapFunction<IN, OUT> {

    @Override
    public void open(Configuration parameters) throws Exception {
        Properties properties = PropertyMgr.getInstance()
                .loadProperty(PropertyConstants.APPLICATION_PROPERTIES);
        SherlockioMetrics.init(properties.getProperty(PropertyConstants.SHERLOCKIO_NAMESPACE),
                properties.getProperty(PropertyConstants.SHERLOCKIO_ENDPOINT), properties.getProperty(PropertyConstants.SHERLOCKIO_USER));
    }

    @Override
    public void close() throws Exception {
        SherlockioMetrics.getInstance().close();
    }

    @Override
    public abstract OUT map(IN value) throws Exception;

}
