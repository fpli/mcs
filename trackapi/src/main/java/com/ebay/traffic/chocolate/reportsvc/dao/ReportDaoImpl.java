package com.ebay.traffic.chocolate.reportsvc.dao;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.ebay.traffic.chocolate.mkttracksvc.dao.CouchbaseClient;
import com.ebay.traffic.chocolate.reportsvc.constant.DateRange;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

@Component
public class ReportDaoImpl implements ReportDao {

  private Bucket bucket;

  @Autowired
  public ReportDaoImpl(CouchbaseClient client) {
    this.bucket = client.getReportBucket();
  }

  public List<ReportDo> getAllDataForMonth(String prefix, String month) throws ParseException {
    List<ReportDo> resultList = new ArrayList<>();
    for (DataType dataType : DataType.values()) {
      resultList.addAll(getAllDataForMonth(prefix, month, dataType));
    }
    return resultList;
  }

  public List<ReportDo> getAllDataForDate(String prefix, String date) throws ParseException {
    List<ReportDo> resultList = new ArrayList<>();
    for (DataType dataType : DataType.values()) {
      resultList.addAll(getAllDataForDate(prefix, date, dataType));
    }
    return resultList;
  }

  public List<ReportDo> getAllDataForDateRange(String prefix, String startDate, String endDate) throws ParseException {
    List<ReportDo> resultList = new ArrayList<>();
    for (DataType dataType : DataType.values()) {
      resultList.addAll(getAllDataForDateRange(prefix, startDate, endDate, dataType));
    }
    return resultList;
  }

  public List<ReportDo> getAllDataGreaterThanDate(String prefix, String date) throws ParseException {
    List<ReportDo> resultList = new ArrayList<>();
    for (DataType dataType : DataType.values()) {
      resultList.addAll(getAllDataGreaterThanDate(prefix, date, dataType));
    }
    return resultList;
  }

  public List<ReportDo> getAllDataLessThanDate(String prefix, String date) throws ParseException {
    List<ReportDo> resultList = new ArrayList<>();
    for (DataType dataType : DataType.values()) {
      resultList.addAll(getAllDataLessThanDate(prefix, date, dataType));
    }
    return resultList;
  }

  private List<ReportDo> getAllDataForMonth(String prefix, String month, DataType dataType) throws ParseException {
    List<ReportDo> resultList = new ArrayList<>();

    Date date = DateRange.MONTH_FORMAT.parse(month);
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);

    int maxDayOfMonth = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);

    for (int i = 1; i <= maxDayOfMonth; i++) {
      calendar.set(Calendar.DAY_OF_MONTH, i);
      String currentDate = DateRange.REQUEST_DATE_FORMAT.format(date);

      String key = getKey(prefix, currentDate, dataType);
      if (bucket.exists(key)) {
        List<Tuple<Long, Long>> document = new ArrayList<>();

        JsonArray jsonArray = bucket.get(key, JsonArrayDocument.class).content();
        for (Object obj : jsonArray) {
          JsonObject jsonObject = (JsonObject) obj;
          Long count = jsonObject.getLong("count");
          Long timestamp = jsonObject.getLong("timestamp");
          document.add(new Tuple<>(count, timestamp));
        }

        ReportDo reportDo = new ReportDo(key, document, dataType,
                Integer.valueOf(DateRange.formatRequestDate(currentDate)));
        resultList.add(reportDo);
      }
    }

    return resultList;
  }

  private List<ReportDo> getAllDataForDate(String prefix, String date, DataType dataType) throws ParseException {
    List<ReportDo> resultList = new ArrayList<>();

    String currentDate = DateRange.convertDateToRequestFormat(date);

    String key = getKey(prefix, currentDate, dataType);

    if (bucket.exists(key)) {
      List<Tuple<Long, Long>> document = new ArrayList<>();

      JsonArray jsonArray = bucket.get(key, JsonArrayDocument.class).content();
      for (Object obj : jsonArray) {
        JsonObject jsonObject = (JsonObject) obj;
        Long count = jsonObject.getLong("count");
        Long timestamp = jsonObject.getLong("timestamp");
        document.add(new Tuple<>(count, timestamp));
      }

      ReportDo reportDo = new ReportDo(key, document, dataType,
              Integer.valueOf(DateRange.formatRequestDate(currentDate)));
      resultList.add(reportDo);
    }

    return resultList;
  }

  private List<ReportDo> getAllDataForDateRange(String prefix, String startDate, String endDate, DataType dataType) throws ParseException {
    List<ReportDo> resultList = new ArrayList<>();

    Date dt1 = DateRange.DATE_FORMAT.parse(startDate);
    Date dt2 = DateRange.DATE_FORMAT.parse(endDate);

    Calendar calendar = Calendar.getInstance();
    calendar.setTime(dt1);

    while (dt1.getTime() <= dt2.getTime()) {
      String currentDate = DateRange.REQUEST_DATE_FORMAT.format(dt1);
      String key = getKey(prefix, currentDate, dataType);
      if (bucket.exists(key)) {
        List<Tuple<Long, Long>> document = new ArrayList<>();

        JsonArray jsonArray = bucket.get(key, JsonArrayDocument.class).content();
        for (Object obj : jsonArray) {
          JsonObject jsonObject = (JsonObject) obj;
          Long count = jsonObject.getLong("count");
          Long timestamp = jsonObject.getLong("timestamp");
          document.add(new Tuple<>(count, timestamp));
        }

        ReportDo reportDo = new ReportDo(key, document, dataType,
                Integer.valueOf(DateRange.formatRequestDate(currentDate)));
        resultList.add(reportDo);
      }

      calendar.add(Calendar.DATE, 1);
      dt1 = calendar.getTime();
    }

    return resultList;
  }

  private List<ReportDo> getAllDataGreaterThanDate(String prefix, String date, DataType dataType) throws ParseException {
    List<ReportDo> resultList = new ArrayList<>();

    Date dt1 = DateRange.DATE_FORMAT.parse(date);
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(dt1);

    calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
    Date dt2 = calendar.getTime();

    calendar.setTime(dt1); // set back
    calendar.add(Calendar.DAY_OF_MONTH, 1);
    dt1 = calendar.getTime();

    while (dt1.getTime() <= dt2.getTime()) {
      String currentDate = DateRange.REQUEST_DATE_FORMAT.format(dt1);
      String key = getKey(prefix, currentDate, dataType);
      if (bucket.exists(key)) {
        List<Tuple<Long, Long>> document = new ArrayList<>();

        JsonArray jsonArray = bucket.get(key, JsonArrayDocument.class).content();
        for (Object obj : jsonArray) {
          JsonObject jsonObject = (JsonObject) obj;
          Long count = jsonObject.getLong("count");
          Long timestamp = jsonObject.getLong("timestamp");
          document.add(new Tuple<>(count, timestamp));
        }

        ReportDo reportDo = new ReportDo(key, document, dataType,
                Integer.valueOf(DateRange.formatRequestDate(currentDate)));
        resultList.add(reportDo);
      }

      calendar.add(Calendar.DAY_OF_MONTH, 1);
      dt1 = calendar.getTime();
    }

    return resultList;
  }

  private List<ReportDo> getAllDataLessThanDate(String prefix, String date, DataType dataType) throws ParseException {
    List<ReportDo> resultList = new ArrayList<>();

    Date dt2 = DateRange.DATE_FORMAT.parse(date);
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(dt2);

    calendar.set(Calendar.DAY_OF_MONTH, 1);
    Date dt1 = calendar.getTime();

    while (dt1.getTime() < dt2.getTime()) {
      String currentDate = DateRange.REQUEST_DATE_FORMAT.format(dt1);
      String key = getKey(prefix, currentDate, dataType);
      if (bucket.exists(key)) {
        List<Tuple<Long, Long>> document = new ArrayList<>();

        JsonArray jsonArray = bucket.get(key, JsonArrayDocument.class).content();
        for (Object obj : jsonArray) {
          JsonObject jsonObject = (JsonObject) obj;
          Long count = jsonObject.getLong("count");
          Long timestamp = jsonObject.getLong("timestamp");
          document.add(new Tuple<>(count, timestamp));
        }

        ReportDo reportDo = new ReportDo(key, document, dataType,
                Integer.valueOf(DateRange.formatRequestDate(currentDate)));
        resultList.add(reportDo);
      }

      calendar.add(Calendar.DAY_OF_MONTH, 1);
      dt1 = calendar.getTime();
    }

    return resultList;
  }

  private static String getKey(String prefix, String date, DataType dataType) {
    switch (dataType) {
      case CLICK:
        return getKeyForClick(prefix, date);
      case IMPRESSION:
        return getKeyForImpression(prefix, date);
      case GROSS_IMPRESSION:
        return getKeyForGrossImpression(prefix, date);
      case VIEWABLE:
        return getKeyForViewable(prefix, date);
      case MOBILE_CLICK:
        return getKeyForMobileClick(prefix, date);
      case MOBILE_IMPRESSION:
      default:
        return getKeyForMobileImpression(prefix, date);
    }
  }

  private static String getKeyForClick(String prefix, String date) {
    return generateKey(prefix, date, "CLICK", false, true);
  }

  private static String getKeyForImpression(String prefix, String date) {
    return generateKey(prefix, date, "IMPRESSION", false, true);
  }

  private static String getKeyForGrossImpression(String prefix, String date) {
    return generateKey(prefix, date, "IMPRESSION", false, false);
  }

  private static String getKeyForViewable(String prefix, String date) {
    return generateKey(prefix, date, "VIEWABLE", false, true);
  }

  private static String getKeyForMobileClick(String prefix, String date) {
    return generateKey(prefix, date, "CLICK", true, true);
  }

  private static String getKeyForMobileImpression(String prefix, String date) {
    return generateKey(prefix, date, "IMPRESSION", true, true);
  }

  private static String generateKey(String prefix, String date, String action, boolean isMobile, boolean isFiltered) {
    String key = prefix + "_" + date + "_" + action;
    if (isMobile && isFiltered) {
      key += "_MOBILE_FILTERED";
    } else if (isMobile && !isFiltered) {
      key += "_MOBILE_RAW";
    } else if (!isMobile && !isFiltered) {
      key += "_DESKTOP_FILTERED";
    } else {
      key += "_DESKTOP_RAW";
    }
    return key;
  }
}
