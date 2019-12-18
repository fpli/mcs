package com.ebay.app.raptor.chocolate.adservice.util;

import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.util.Map;

public class ParametersParser {
  public static MultiValueMap<String, String> parse(Map<String, String[]> params) {
    MultiValueMap<String, String> result = new LinkedMultiValueMap<>();
    for (Map.Entry<String, String[]> param : params.entrySet()) {
      String[] values = param.getValue();
      for (String value : values) {
        result.add(param.getKey(), value);
      }
    }
    return result;
  }
}
