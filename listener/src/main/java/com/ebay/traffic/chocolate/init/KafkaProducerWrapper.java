package com.ebay.traffic.chocolate.init;

import com.ebay.app.raptor.chocolate.common.MetricsClient;
import com.ebay.cratchit.server.Replayable;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * @author kanliu
 *
 * Class for sending messages to the Kafka queue. Its primary function is to
 * wrap the underlying Kafka Producer class
 */
public class KafkaProducerWrapper implements Replayable {
    /** Global logging instance */
    private static final Logger logger = Logger.getLogger(KafkaProducerWrapper.class);

    /** The topic to publish to. */
    private final String topic;

    /** Producer implementation - this is basically < CampaignID, JSON >*/
    private final Producer<Long, String> producer;

    /** @param topic of the producer to write to */
    public KafkaProducerWrapper(final String topic, final Producer<Long, String> producer) {
        this.topic = topic;
        this.producer = producer;
    }

    /**
     * Callback implementation which will fail the application on failure
     */
    private volatile Callback callback = new Callback() {
        /**
         * A callback method the user can implement to provide asynchronous handling of request completion. This method
         * will be called when the record sent to the server has been acknowledged. Exactly one of the arguments will be
         * non-null.
         *
         * @param metadata The metadata for the record that was sent (i.e. the partition and offset). Null if an error
         *            occurred.
         * @param exception The exception thrown during processing of this record. Null if no error occurred.
         */
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (metadata == null) {
                MetricsClient.getInstance().meter("KafkaFailure");
                logger.error(exception);
            } else {
                logger.debug("Succeeded in sending kafka record=" + metadata);
            }
        }
    };

    /**
     * Flush and close the Kafka producer on shutdown.
     */
    private void flushAndClose() {
        producer.flush();
        producer.close();
    }

    /**
     * Flush and close the producer and reset instance
     */
    public void close() {
        this.flushAndClose();
    }

    @Override
    public String toString() {
        return "topic=" + topic + " producer_class=" + producer.getClass().getCanonicalName();
    }

    public void send(long partition, String message) {
        Validate.notNull(partition, message);
        producer.send(new ProducerRecord<>(topic, partition, message), callback);
    }

    /**
     * Send the record without partition
     */
    public void send(String message) {
        producer.send(new ProducerRecord<>(topic, message), callback);
    }

    @Override
    public void replay(long snapshotId, byte[] entry) {
        long partition = ByteBuffer.wrap(Arrays.copyOfRange(entry, 0, Long.BYTES)).getLong();
        String message = new String(Arrays.copyOfRange(entry, Long.BYTES, entry.length));
        send(partition, message);
    }
}
