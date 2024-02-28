package com.ebay.traffic.chocolate.html.metric;

import com.ebay.traffic.chocolate.config.MetricConfig;
import com.ebay.traffic.chocolate.pojo.metric.monitor.FlinkProject;

import static com.ebay.traffic.chocolate.html.metric.ConsumerLagHtmlParse.parseConsumerLagHtml;
import static com.ebay.traffic.chocolate.html.metric.FlinkMonitorHtmlParse.parseFlinkMonitorHtml;

public class MetricHtmlParse {
    
    private static final MetricConfig metricConfig = MetricConfig.getInstance();
    
    public static String getMetricHtml() {
        StringBuilder html = new StringBuilder();
        if(metricConfig.getFlinkProjectList() != null && metricConfig.getFlinkProjectList().size() > 0) {
            for (FlinkProject flinkProject : metricConfig.getFlinkProjectList()) {
                html.append(flinkProject.getProjectName()).append(" flink job state(").append(flinkProject.getNamespace()).append(")");
                html.append("<br>");
                html.append(parseFlinkMonitorHtml(flinkProject));
                html.append(flinkProject.getProjectName()).append(" consumer lag state");
                html.append("<br>");
                html.append(parseConsumerLagHtml(flinkProject));
                html.append("<br>");
            }
        }
        return html.toString();
    }
    

    
}
