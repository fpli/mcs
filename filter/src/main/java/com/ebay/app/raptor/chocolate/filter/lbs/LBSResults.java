package com.ebay.app.raptor.chocolate.filter.lbs;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.ArrayList;
import java.util.List;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class LBSResults {
  /*
  {
      "allResults": [
          {
              "httpStatus": 200,
              "queryResult": [
                  {
                      "regionCode": "44",
                      "postalCode": "76137",
                      "metroCode": "623",
                      "isoCountryCode2": "US",
                      "stateCode": "TX",
                      "longitude": -97.2934,
                      "areaCodes": "817/682",
                      "latitude": 32.86,
                      "queryId": "chocolate_geotargeting_ip_1",
                      "city": "ft worth"
                  }
              ]
          }
      ]
  }
  */

  private List<LBSHttpResult> allResults = new ArrayList<LBSHttpResult>();

  public List<LBSHttpResult> getAllResults() {
    return allResults;
  }

  public void setAllResults(List<LBSHttpResult> allResults) {
    this.allResults = allResults;
  }
}