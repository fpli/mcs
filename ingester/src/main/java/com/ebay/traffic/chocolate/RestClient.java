package com.ebay.traffic.chocolate;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class RestClient {
  public static String get(String resource) {
    Client client = Client.create();

    WebResource webResource = client.resource(resource);

    ClientResponse response = webResource.accept(new String[]{"application/json"}).get(ClientResponse.class);
    if (response.getStatus() != 200) {
      throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
    }
    return response.getEntity(String.class);
  }
}
