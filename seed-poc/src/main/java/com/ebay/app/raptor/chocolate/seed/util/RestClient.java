package com.ebay.app.raptor.chocolate.seed.util;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;

/**
 * @author yimeng on 2018/11/7
 */
public class RestClient {

  public static String get(String resource) {
    Client client = Client.create();

    WebResource webResource = client.resource(resource);

    ClientResponse response = webResource.accept(MediaType.APPLICATION_JSON_VALUE).get(ClientResponse.class);
    if (response.getStatus() != HttpStatus.OK.value()) {
      throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
    }
    return response.getEntity(String.class);
  }
}
