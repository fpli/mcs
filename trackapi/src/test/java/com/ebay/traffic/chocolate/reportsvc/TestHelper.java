package com.ebay.traffic.chocolate.reportsvc;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import rx.Observable;
import rx.functions.Func1;

import java.text.SimpleDateFormat;
import java.util.*;

public class TestHelper {

  public static void prepareTestData(Bucket bucket, String[] prefixes) {
    Calendar calendar = Calendar.getInstance();
    calendar.set(Calendar.YEAR, 2000);
    calendar.set(Calendar.MONTH, 0);
    calendar.set(Calendar.DAY_OF_MONTH, 2);
    calendar.set(Calendar.HOUR_OF_DAY, 0);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);

    // dt1 and dt represent the date range to generate test data.
    Date dt2 = calendar.getTime();
    calendar.set(Calendar.DAY_OF_MONTH, 1);
    Date dt1 = calendar.getTime();

    final String[] actions = {"CLICK", "IMPRESSION", "VIEWABLE"};

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    List<JsonArrayDocument> jsonArrayDocuments = new ArrayList<>();

    while (dt1.getTime() <= dt2.getTime()) {
      String currentDate = simpleDateFormat.format(dt1);
      for (String prefix : prefixes) {
        for (String action : actions) {
          String key1 = generateKey(prefix, currentDate, action, true, true);
          String key2 = generateKey(prefix, currentDate, action, true, false);
          String key3 = generateKey(prefix, currentDate, action, false, true);
          String key4 = generateKey(prefix, currentDate, action, false, false);

          Calendar delta = Calendar.getInstance();
          delta.setTime(dt1);
          Date deltaTime = delta.getTime();

          // Each day has similar data.
          JsonArray jsonArray = JsonArray.create();
          int numberOfRecordPerDay = 5;
          while (numberOfRecordPerDay > 0) {
            JsonObject jsonObject = JsonObject.create();
            jsonObject.put("count", 1);
            jsonObject.put("timestamp", deltaTime.getTime());
            jsonArray.add(jsonObject);
            numberOfRecordPerDay--;
            delta.add(Calendar.MINUTE, 15);
            deltaTime = delta.getTime();
          }

          jsonArrayDocuments.add(JsonArrayDocument.create(key1, jsonArray));
          jsonArrayDocuments.add(JsonArrayDocument.create(key2, jsonArray));
          jsonArrayDocuments.add(JsonArrayDocument.create(key3, jsonArray));
          jsonArrayDocuments.add(JsonArrayDocument.create(key4, jsonArray));
        }
      }

      calendar.add(Calendar.DAY_OF_MONTH, 1);
      dt1 = calendar.getTime();
    }

    Observable
            .from(jsonArrayDocuments)
            .flatMap((Func1<JsonArrayDocument, Observable<JsonArrayDocument>>) f -> bucket.async().upsert(f))
            .toList()
            .toBlocking()
            .single();
  }

  private static String generateKey(String prefix, String date, String action, boolean isMobile, boolean isFiltered) {
    String key = prefix + "_" + date + "_" + action;
    if (isMobile && isFiltered) {
      key += "_MOBILE_FILTERED";
    } else if (isMobile && !isFiltered) {
      key += "_MOBILE_RAW";
    } else if (!isMobile && isFiltered) {
      key += "_DESKTOP_FILTERED";
    } else {
      key += "_DESKTOP_RAW";
    }
    return key;
  }
}
