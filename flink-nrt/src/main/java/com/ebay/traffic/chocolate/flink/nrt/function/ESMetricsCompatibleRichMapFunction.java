package com.ebay.traffic.chocolate.flink.nrt.function;

import com.ebay.traffic.chocolate.flink.nrt.constant.PropertyConstants;
import com.ebay.traffic.chocolate.flink.nrt.constant.StringConstants;
import com.ebay.traffic.monitoring.ESMetrics;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public abstract class ESMetricsCompatibleRichMapFunction<IN, OUT> extends RichMapFunction<IN, OUT> {

    @Override
    public void open(Configuration parameters) throws Exception {
        ExecutionConfig.GlobalJobParameters globalParams = getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        Configuration globConf = (Configuration) globalParams;
        ESMetrics.init(globConf.getString(PropertyConstants.ELASTICSEARCH_INDEX_PREFIX, StringConstants.EMPTY), globConf.getString(PropertyConstants.ELASTICSEARCH_URL, StringConstants.EMPTY));
    }

    @Override
    public void close() throws Exception {
        ESMetrics.getInstance().close();
    }

    @Override
    public abstract OUT map(IN value) throws Exception;
}
