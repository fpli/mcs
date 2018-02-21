package com.ebay.app.raptor.chocolate.filter;

import com.ebay.app.raptor.chocolate.avro.FilterMessage;
import com.ebay.app.raptor.chocolate.avro.ListenerMessage;
import com.ebay.app.raptor.chocolate.common.MetricsClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Wraps KafkaConsumer and KafkaProducer for ease  of unit testing
 * Created by spugach on 3/7/17.
 */
public class KafkaWrapper {
    // Logging instance.
    private static final Logger logger = Logger.getLogger(KafkaWrapper.class);

    private String outTopic;

    private KafkaConsumer<Long, String> consumer = null;
    private KafkaProducer<Long, String> producer = null;

    public void init(String inTopic, String outTopic) throws IOException {
        Properties kafkaConfig = ApplicationOptions.getInstance().getKafkaConfig();
        consumer = new KafkaConsumer<>(kafkaConfig);
        producer = new KafkaProducer<>(kafkaConfig);
        consumer.subscribe(Arrays.asList(inTopic));
        this.outTopic = outTopic;
    }

    public List<ListenerMessage> poll() {
        List<ListenerMessage> result = null;

        ConsumerRecords<Long, String> messages = this.consumer.poll(0);
        for (ConsumerRecord<Long, String> message : messages) {
            try
            {
                ListenerMessage datum = ListenerMessage.readFromJSON(message.value());

                if (datum != null) {
                    if (null == result) {
                        result  = new ArrayList<>();
                    }

                    result.add(datum);
                }
            }
            catch (IOException e)
            {
                logger.error("Caught exception reading datum", e);
                MetricsClient.getInstance().meter("messageParseFailure");
            }
        }

        return result;
    }

    public void send(FilterMessage message) throws IOException {
        String json = message.writeToJSON();
        this.producer.send(new ProducerRecord<>(outTopic, message.getCampaignId().longValue(), json));
    }

    public void flush() {
        this.producer.flush();
    }

    public void close() {
        if (this.consumer != null) {
            this.consumer.close();
        }

        if (this.producer != null) {
            this.producer.close();
        }
    }
}
