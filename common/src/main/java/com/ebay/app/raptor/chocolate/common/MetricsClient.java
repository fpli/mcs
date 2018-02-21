package com.ebay.app.raptor.chocolate.common;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.ebayopensource.sherlock.client.*;
import org.ebayopensource.sherlock.client.MetricSet.Builder;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;

public class MetricsClient {
    /**
     * Logging instance
     */
    private static final Logger logger = Logger.getLogger(MetricsClient.class);

    /**
     * Timer instance
     */
    final Timer timer;

    /**
     * Session with Sherlock
     */
    Session session;

    /**
     * Meter metrics
     */
    final Map<String, Long> meterMetrics = new HashMap<>();

    /**
     * Mean metrics
     */
    final Map<String, Pair<Long, Long>> meanMetrics = new HashMap<>();

    /**
     * Send count for Frontier
     */
    volatile long sendCount = 0L;

    /**
     * Client for Frontier
     */
    static volatile MetricsClient INSTANCE = null;

    /* Frontier config.
    Harcoded here because changing it in a config file would require the same number of commits as changing it here */
    static String profileName = "Chocolate";
    static String appSvcName = null;
    static final int frontierResolution = 60;
    private static final int frontierDelay = 60000;
    private static final int frontierInterval = 60000;

    /**
     * Default value for Meter and Mean metrics
     */
    private static final long zeroCounter = 0L;
    private static final Pair<Long, Long> zeroPair = new ImmutablePair<>(0L,0L);

    /**
     * Singleton
     */
    MetricsClient() {
        session = null;
        timer = new Timer();
    }

    /**
     * Initialize a session with the given Frontier instance/service/default profile
     */
    public static synchronized void init(String frontierUrl, String inputAppSvcName) {
        init(frontierUrl, inputAppSvcName, profileName);
    }


    /**
     * Initialize a session with the given Frontier instance/service/profile
     */
    public static synchronized void init(String frontierUrl, String inputAppSvcName, String inputProfileName) {
        if (INSTANCE != null) return; // Don't re-init.
        Validate.notEmpty(inputAppSvcName, "AppSvcName cannot be empty");
        Validate.notEmpty(inputProfileName, "ProfileName cannot be empty");
        appSvcName = inputAppSvcName;
        profileName = inputProfileName;
        System.out.println("Profile: " + profileName);
        System.out.println(String.format(frontierUrl, appSvcName));
        init(SherlockClient.connect(String.format(frontierUrl, appSvcName)));
    }

    /**
     * @return the singleton instance.
     */
    public static MetricsClient getInstance() { return INSTANCE; }

    /**
     * Initialize the frontier client; sadly has to be public to mock for UTs
     *
     * @param session to initialize with
     */
    public static synchronized void init(Session session) {
        Validate.notNull(session, "Session cannot be null");
        Validate.isTrue(INSTANCE == null, "Cannot re-initialize instance");

        INSTANCE = new MetricsClient();
        INSTANCE.session = session;

        // Start the timer.
        INSTANCE.timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                INSTANCE.postMetrics();
            }
        }, frontierDelay, frontierInterval);
    }

    /**
     * Mark the occurrence of a given number of events. For throughput purpose
     *
     * @param name  : series name
     * @param value : number value like count
     */
    public synchronized void meter(String name, long value) {
        Validate.isTrue(StringUtils.isNotBlank(name), "Metrics name cannot be null");
        // Check if the metric exists, if not, add empty value
        if (!meterMetrics.containsKey(name)) {
            meterMetrics.put(name, 0L);
        }

        long meter = meterMetrics.get(name);
        meter += value;
        meterMetrics.put(name, meter);
    }

    /**
     * Mark the occurrence of a given number of events. For throughput purpose, set count as 1L
     *
     * @param name : series name
     */
    public void meter(String name) {
        meter(name, 1L);
    }

    /**
     * A metric which calculates the mean of a value
     *
     * @param name  : series name
     * @param value : number value like latency
     */
    public synchronized void mean(String name, long value) {
        Validate.isTrue(StringUtils.isNotBlank(name), "Metrics name cannot be null");

        // Check if the metric exists, if not, add empty value
        if (!meanMetrics.containsKey(name)) {
            meanMetrics.put(name, Pair.of(0L, 0L));
        }

        Pair<Long, Long> pair = meanMetrics.get(name);
        Validate.isTrue(pair != null && pair.getLeft() != null && pair.getRight() != null, "Pair cannot be null or have null elements");

        // Left is accumulator, right is count
        long accumulator = pair.getLeft() + value;
        long count = pair.getRight() + 1;

        meanMetrics.put(name, Pair.of(accumulator, count));
    }

    /**
     * Terminate the client
     */
    public void terminate() {
        if (timer != null) timer.cancel();
        if (session != null) session.close();
    }

    /**
     * @return a metric set based on current state, or null if there is none.
     */
    synchronized MetricSet buildMetrics() {
        if(meterMetrics.isEmpty() && meanMetrics.isEmpty()) {
            return null;
        }
        // Set common parameters: host, appSvcName
        final Builder metricsBuilder = MetricSet.builder().resolutionSeconds(frontierResolution).profile(profileName)
                .dimension("host", Hostname.HOSTNAME)
                .dimension("appSvcName", appSvcName);
        logger.debug(String.format("common parameters: %s = %d", "frontierResolution", frontierResolution));
        logger.debug(String.format("common parameters: %s = %s", "profileName", profileName));
        logger.debug(String.format("common parameters: %s = %s", "host", Hostname.HOSTNAME));
        // Handle count type metrics
        // Cleanup after process
        if (!meterMetrics.isEmpty()) {
            for (Entry<String, Long> item : meterMetrics.entrySet()) {
                Validate.isTrue(StringUtils.isNotBlank(item.getKey()), "Invalid meter key");
                Validate.notNull(item.getValue(), "Invalid meter value");
                logger.info(String.format("Send count type metrics: %s = %d", item.getKey(), item.getValue()));
                metricsBuilder.add(item.getKey(), Gauge.longType(), item.getValue());
            }
        }

        // Handle average type metrics
        // Cleanup after process
        if (!meanMetrics.isEmpty()) {
            for (Entry<String, Pair<Long, Long>> item : meanMetrics.entrySet()) {
                Validate.isTrue(StringUtils.isNotBlank(item.getKey()), "Invalid meter key");
                Validate.isTrue(item.getValue() != null && item.getValue().getLeft() != null && item.getValue().getRight() != null, "Invalid mean map state");
                long accumulator = item.getValue().getLeft();
                long count = item.getValue().getRight();
                double mean = 0d;
                if(count > 0) mean = Long.valueOf(accumulator).doubleValue() / Long.valueOf(count).doubleValue();

                logger.info(String.format("Send mean type metrics: %s = %f", item.getKey(), mean));
                metricsBuilder.add(item.getKey(), Gauge.doubleType(), mean);
            }
        }

        // Now empty out the metrics value, keep the metrics name
        clearCounter(meterMetrics, zeroCounter);
        clearCounter(meanMetrics, zeroPair);

        return metricsBuilder.build();
    }

    private void postMetrics() {
        send(buildMetrics());
    }

    /**
     * Cleanup counters in the metrics map
     * @param map the map to clear
     * @param zero default value to set
     */
    private <K, V> void clearCounter(Map<K, V> map, V zero){
        for (Map.Entry<K, V> entry: map.entrySet()) {
            entry.setValue(zero);
        }
    }

    void send(final MetricSet metricSet) {
        if (metricSet == null) return;

        Validate.notNull(session, "Session cannot be null at this point");
        // Create a post request for the provided metric set and execute it.
        // This is done asynchronously so a ResultFuture is returned.
        final ResultFuture<PostResult> f = session.post(metricSet).execute();
        ++sendCount;

        // Check the result of the post request.
        // How you choose to do this is dependent on your application's needs.
        //
        // You can of course just block the calling thread and wait for the
        // future
        // to complete which would provide natural back-pressure but limit
        // throughput.
        //
        // You can also attach a callback to the future to handle the result as
        // we
        // are doing below. This allows multiple requests to run in parallel
        // when
        // submitted by the same thread.
        //
        // Note that ResultFuture is actually a ListenableFuture from Guava.
        Futures.addCallback(f, new FutureCallback<PostResult>() {
            public void onSuccess(PostResult result) {
                logger.info("Send metrics to frontier successfully");
            }

            public void onFailure(Throwable t) {
                logger.warn("Fail to send metrics to frontier", t);
            }
        });

        // Block until it's all sent.
        try {
            f.get();
        } catch (InterruptedException e) {
            logger.error("Caught exception in Frontier interruption", e);
        } catch (ExecutionException e) {
            logger.error("Caught exception in Frontier execution", e);
        }
    }

    /**
     * @return the number of times the metrics have been sent.
     */
    public long getSendCount() {
        return sendCount;
    }
}
