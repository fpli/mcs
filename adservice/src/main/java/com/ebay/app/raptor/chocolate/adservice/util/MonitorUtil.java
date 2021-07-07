package com.ebay.app.raptor.chocolate.adservice.util;

import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Field;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;

public final class MonitorUtil {
    static com.ebay.traffic.monitoring.Metrics esMetrics;

    public static void setEsMetrics(com.ebay.traffic.monitoring.Metrics metrics){
        esMetrics=metrics;
    }
    public static void info(String key) {
        if(esMetrics==null){
            esMetrics= ESMetrics.getInstance();
        }
        esMetrics.meter(key);
        Metrics.counter(key).increment();
    }
    @SafeVarargs
    public static void info(String key, long value,Field<String, Object>... fields) {
        if(esMetrics==null){
            esMetrics= ESMetrics.getInstance();
        }
        esMetrics.meter(key, value,fields);
        Metrics.counter(key,fieldsToTags(fields)).increment(value);
    }

    public static void counter(String key, long value,String... tags) {
        Metrics.counter(key,tags).increment(value);
    }

    public static void warn(String key) {
        if(esMetrics==null){
            esMetrics= ESMetrics.getInstance();
        }
        esMetrics.meter(key);
        Metrics.counter(key).increment();
    }
    @SafeVarargs
    public static void latency(String key, long value,Field<String, Object>... fields) {
        if(esMetrics==null){
            esMetrics= ESMetrics.getInstance();
        }
        esMetrics.mean(key, value,fields);
        Metrics.counter(key,fieldsToTags(fields)).increment(value);
    }
    @SafeVarargs
    public static String[] fieldsToTags(Field<String, Object>... fields){
        int length=fields.length;
        String[] additional=new String[2*length];
        for(int i=0;i<length;i++){
            additional[2*i]=fields[i].getKey();
            additional[2*i+1]=fields[i].getValue()==null?"NULL":fields[i].getValue().toString();
        }
        return additional;
    }
}
