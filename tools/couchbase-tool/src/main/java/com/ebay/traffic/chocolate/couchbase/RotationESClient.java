package com.ebay.traffic.chocolate.couchbase;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class RotationESClient {
    static Logger logger = LoggerFactory.getLogger(RotationESClient.class);
    private RestClientBuilder restClientBuilder;
    private Properties properties;

    public RotationESClient(Properties properties) {
        this.properties = properties;
        init();
    }

    //init es client builder
    private void init() {
        try {
            restClientBuilder = RestClient.builder(HttpHost.create(properties.getProperty("chocolate.elasticsearch.url")));
        } catch (Exception e) {
            logger.error("Failed to initialize ES client builder.");
            throw e;
        }
        logger.info("Initialize ES client builder successfully.");
    }

    //generate rest high level client for every search operation
    public RestHighLevelClient getESClient() {
        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
        return client;
    }

    //close restHighLevelClient after every search operation
    public void closeESClient(RestHighLevelClient restHighLevelClient) throws IOException {
        try {
            restHighLevelClient.close();
        } catch (IOException e) {
            logger.error("Failed to close ES client.");
            throw e;
        }
    }

}
