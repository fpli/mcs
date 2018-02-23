package com.ebay.traffic.chocolate.listener.api;

import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.traffic.chocolate.kafka.KafkaSink;
import com.ebay.traffic.chocolate.listener.util.ChannelIdEnum;
import com.ebay.traffic.chocolate.listener.util.ListenerOptions;
import com.ebay.traffic.chocolate.listener.util.MessageObjectParser;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.MalformedURLException;
import java.net.URL;

public class TrackingServlet extends HttpServlet {
    private static final long serialVersionUID = 3995857060272114801L;
    private static final Logger logger = Logger.getLogger(TrackingServlet.class);

    private static final String SNID_PATTERN = "snid";

    private Producer<Long, ListenerMessage> producer;

    private String kafkaTopic;
    
    /** Metrics client instance */
    private MetricsClient metrics;
    
    /** Message object parser instance */
    private MessageObjectParser parser;
    
    public TrackingServlet(final MetricsClient metrics, final MessageObjectParser parser) {
        this.metrics = metrics;
        this.parser = parser;
    }

    @Override
    public void init() {
        ListenerOptions options = ListenerOptions.getInstance();
        kafkaTopic = options.getKafkaChannelTopic(ChannelIdEnum.EPN);
        producer = KafkaSink.get();

        if (parser == null)
            parser = MessageObjectParser.getInstance();
        if (metrics == null)
            metrics = MetricsClient.getInstance();
    }

    /**
     * Processes the event and sends a response
     * @param request the HTTP request from the client
     * @param response the Listener's response, based on request URL
     * @throws MalformedURLException
     */
    public void doGet(HttpServletRequest request, HttpServletResponse response) throws MalformedURLException {
        try {
            metrics.meter("VimpCount");
            TrackingEvent event = new TrackingEvent(new URL(request.getRequestURL().toString()), request.getQueryString());
            
            // Make the response
            event.respond(response);
            
            // Derive the campaign ID
            Long campaignId;
            try {
                campaignId = Long.parseLong(event.getCollectionID());
            } catch (NumberFormatException e) {
                logger.error("Error in parsing collection as campaign=" + event.getCollectionID() + " for url=" + request.getRequestURL());
                return;
            }
            
            // Derive the channel ID and its logical channel counterpart
            ChannelIdEnum id = ChannelIdEnum.parse(Integer.toString(event.getChannelID()));
            if (id == null) {
                logger.error("Error in parsing channel id for url=" + request.getRequestURL());
                return;
            }

            String snid = null;
            if (event.getPayload() != null && event.getPayload().containsKey(SNID_PATTERN)) {
                snid = event.getPayload().get(SNID_PATTERN).toString();
            }
            
            // Derive the Kafka message object
            ListenerMessage message = parser.parseHeader(request, response, System.currentTimeMillis(), campaignId, 
                    id.getLogicalChannel(), event.getAction(), snid);
            if (message == null) {
                logger.error("Could not create Avro message for url=" + request.getRequestURL());
                return;
            }

            producer.send(new ProducerRecord<>(kafkaTopic, message.getSnapshotId(), message), KafkaSink.callback);

            metrics.meter("VimpSuccess");

        } catch (Exception e) {
            logger.error("Couldn't respond to tracking event for " + request.getRequestURL(), e);
        }
    }
}