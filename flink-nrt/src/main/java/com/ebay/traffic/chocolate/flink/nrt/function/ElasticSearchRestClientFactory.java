package com.ebay.traffic.chocolate.flink.nrt.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.elasticsearch7.RestClientFactory;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClientBuilder;

public class ElasticSearchRestClientFactory implements RestClientFactory {
  private final String appKey;
  private final String appSecret;

  public ElasticSearchRestClientFactory(String appKey, String appSecret) {
    this.appKey = appKey;
    this.appSecret = appSecret;
  }

  @Override
  public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
    restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
      @Override
      public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
        if (StringUtils.isNotEmpty(appKey) && StringUtils.isNotEmpty(appSecret)) {
          CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
          credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(appKey, appSecret));
          return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }
        return httpClientBuilder;
      }
    });
  }
}