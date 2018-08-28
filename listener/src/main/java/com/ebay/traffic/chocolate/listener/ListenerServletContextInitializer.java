package com.ebay.traffic.chocolate.listener;

import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.traffic.chocolate.init.JettyServerConfiguration;
import com.ebay.traffic.chocolate.listener.api.TrackingServlet;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import com.ebay.traffic.chocolate.listener.util.MessageObjectParser;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import org.apache.log4j.Logger;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.context.annotation.Configuration;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRegistration;

@Configuration
@AutoConfigureAfter(JettyServerConfiguration.class)
public class ListenerServletContextInitializer implements ServletContextInitializer {

    private static final Logger logger = Logger.getLogger(ListenerServletContextInitializer.class);
    
    @Override
    public void onStartup(ServletContext servletContext) throws ServletException {
        registerServlet(servletContext);
    }

    private void registerServlet(ServletContext servletContext) {

        logger.info("start registerServlet");
        ListenerOptions options = ListenerOptions.getInstance();

        // Add tracking servlet 
        logger.info("start trackingServlet");
        ServletRegistration.Dynamic trackingServlet = servletContext.addServlet("ListenerTrackingServlet",
                new TrackingServlet(MetricsClient.getInstance(), ESMetrics.getInstance(), MessageObjectParser.getInstance()));
        trackingServlet.addMapping("/1v/*", "/1c/*", "/1i/*");
        trackingServlet.setLoadOnStartup(2);

        // Add primary Listener servlet 
        logger.info("start serviceServlet");
        ServletRegistration.Dynamic serviceServlet = servletContext.addServlet("ListenerProxyServlet",
                new ListenerProxyServlet());
        
        // doing /* instead will create a messay situation with AcknowledgerServlet.
        // copy rover pages from https://github.corp.ebay.com/RaptorTracking/rover/blob/master/roverweb/src/main/webapp/META-INF/raptor_app.xml
        // ePN only care about the first two pages
        serviceServlet.addMapping("/rover/*",
                "/roverimp/*",
                "/roverns/*",
                "/roverroi/*",
                "/rovertr/*",
                "/roverclk/*",
                "/roveropen/*",
                "/ar/*",
                "/roversync/*",
                "/roverbd/*",
                "/roverexit/*",
                "/idmap/*",
                "/pdssetcommand/*",
                "/rvr/*",
                "/roverlog/*",
                "/roverext/*",
                "/getid/*",
                "/mapid/*"
        );
        serviceServlet.setInitParameter(options.PROXY, options.getProxy());
        serviceServlet.setInitParameter(options.OUTPUT_HTTP_PORT, Integer.toString(options.getOutputHttpPort()));
        serviceServlet.setInitParameter(options.OUTPUT_HTTPS_PORT, Integer.toString(options.getOutputHttpsPort()));
        serviceServlet.setInitParameter(options.INPUT_HTTP_PORT, Integer.toString(options.getInputHttpPort()));
        serviceServlet.setInitParameter(options.INPUT_HTTPS_PORT, Integer.toString(options.getInputHttpsPort()));
        // Use preserveHost to keep Host Header in the proxy request
        serviceServlet.setInitParameter("preserveHost", "true");
        serviceServlet.setAsyncSupported(true);
        serviceServlet.setLoadOnStartup(3);
        
        logger.info("all loaded!");
    }
}