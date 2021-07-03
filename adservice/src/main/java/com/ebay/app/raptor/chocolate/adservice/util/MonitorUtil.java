package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;

public final class MonitorUtil {

    public static void debug(Logger logger, String key) {
        logger.debug(key);
    }

    public static void debug(Logger logger, String key, String info) {
        logger.debug(key, info);
    };

    public static void info(String key) {
        ESMetrics.getInstance().meter(key);
        Metrics.counter(key).increment();
    }
    @SafeVarargs
    public static void info(String key, long value,Field<String, Object>... fields) {
        ESMetrics.getInstance().meter(key, value,fields);
        Metrics.counter(key,fieldsToTags(fields)).increment(value);
    }

    public static void warn(String key) {
        ESMetrics.getInstance().meter(key);
        Metrics.counter(key).increment();
    }

    public static void error(String key) {
        ESMetrics.getInstance().meter(key);
        Metrics.counter(key).increment();
    }
    public static void latency(String key) {
        ESMetrics.getInstance().mean(key);
        Metrics.counter(key).increment();
    }
    @SafeVarargs
    public static void latency(String key, long value,Field<String, Object>... fields) {
        ESMetrics.getInstance().mean(key, value,fields);
        Metrics.counter(key,fieldsToTags(fields)).increment(value);
    }
    @SafeVarargs
    public static String[] fieldsToTags(Field<String, Object>... fields){
        int length=fields.length;
        String[] additional=new String[2*length];
        for(int i=0;i<length;i++){
            additional[2*i]=fields[i].getKey();
            additional[2*i+1]=fields[i].getValue().toString();
        }
        return additional;
    }
}
