package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.ebay.dukes.CacheClient;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;


public class DumpRotationToHadoop {
  private static CorpRotationCouchbaseClient client;
  private static Bucket bucket;
  private static Properties couchbasePros;
  private static Logger logger = LoggerFactory.getLogger(DumpLegacyRotationFiles.class);

  public static void main(String args[]) throws IOException {
    boolean hasParams = true;
    String configFilePath = (args != null && args.length > 0) ? args[0] : null;
    if(StringUtils.isEmpty(configFilePath)){
      logger.error("No configFilePath was defined. please set configFilePath for rotation jobs");
      hasParams = false;
    }

    String updateTimeStartKey = (args != null && args.length > 1) ? args[1] : null;
    if(StringUtils.isEmpty(updateTimeStartKey)){
      logger.error("No updateTimeStartKey was defined. please set updateTimeStartKey for rotation jobs");
      hasParams = false;
    }

    String updateTimeEndKey = (args != null && args.length > 2) ? args[2] : null;
    if(StringUtils.isEmpty(updateTimeEndKey)){
      logger.error("No updateTimeEndKey was defined. please set updateTimeEndKey for rotation jobs");
      hasParams = false;
    }
    if(!hasParams) return;

    String outputFilePath = (args != null && args.length > 3) ? args[3] : null;

    initCB(configFilePath);
    initLog4j(configFilePath);

    try {
      client = new CorpRotationCouchbaseClient(couchbasePros);
      CacheClient cacheClient = client.getCacheClient();
      bucket = client.getBuctet(cacheClient);
      dumpFileFromCouchbase(updateTimeStartKey, updateTimeEndKey, outputFilePath);
      client.returnClient(cacheClient);
    } finally {
      close();
    }
  }


  public static void initCB(String configFilePath) throws IOException {
    couchbasePros = new Properties();
    InputStream in = new FileInputStream(configFilePath + "couchbase.properties");
    couchbasePros.load(in);
  }

  private static void initLog4j(String configFilePath) throws IOException {
    Properties log4jProps = new Properties();
    try {
      log4jProps.load(new FileInputStream(configFilePath + "log4j.properties"));
      PropertyConfigurator.configure(log4jProps);
    } catch (IOException e) {
      logger.error("Can't load seed properties");
      throw e;
    }
  }


  public static void dumpFileFromCouchbase(String startKey, String endKey, String outputFilePath) throws IOException {
    ESMetrics.init("batch-metrics-", couchbasePros.getProperty("chocolate.elasticsearch.url"));
    ESMetrics esMetrics = ESMetrics.getInstance();

    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    if(outputFilePath == null){
      outputFilePath = couchbasePros.getProperty("job.dumpRotationFiles.outputFilePath");
    }
    Calendar c = Calendar.getInstance();
    c.setTimeInMillis(Long.valueOf(startKey));
    outputFilePath = outputFilePath + "rotation-" + sdf.format(c.getTime()) + ".txt";
    Boolean compress = (couchbasePros.getProperty("job.dumpRotationFiles.compressed") == null) ?  Boolean.valueOf(couchbasePros.getProperty("job.dumpRotationFiles.compressed")) : Boolean.FALSE;

    OutputStream out = null;
    Integer count = 0 ;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(outputFilePath +  ".gz"), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(outputFilePath));
      }
      ViewQuery query = ViewQuery.from(couchbasePros.getProperty("couchbase.corp.rotation.designName"),
          couchbasePros.getProperty("couchbase.corp.rotation.viewName"));
      query.startKey(Long.valueOf(startKey));
      query.endKey(Long.valueOf(endKey));

      List<ViewRow> viewResult = bucket.query(query).allRows();

      List<String> keys = new ArrayList<>();

      if(viewResult != null && viewResult.size() > 0) {
        for (ViewRow row : viewResult) {
          keys.add(row.id());
        }
        List<JsonDocument> result = Observable
            .from(keys)
            .flatMap((Func1<String, Observable<JsonDocument>>) k -> bucket.async().get(k, JsonDocument.class))
            .toList()
            .toBlocking()
            .single();
        for (JsonDocument row : result) {
          String rotationInfoStr = row.content().toString();
          if(rotationInfoStr == null) continue;
          out.write(rotationInfoStr.getBytes());
          out.write(RotationConstant.RECORD_SEPARATOR);
          out.flush();
          count++;
        }
      }

    } catch (IOException e) {
      esMetrics.meter("rotation.dump.FromCBToHIVE.error");
      System.out.println("Error happened when write couchbase data to chocolate file");
      throw e;
    } finally {
      if(out != null){
        out.close();
      }
    }
    esMetrics.meter("rotation.dump.FromCBToHIVE.success", count);
    esMetrics.flushMetrics();
    System.out.println("Successfully dump " + count + " records into chocolate file: " + outputFilePath);
  }

  private static void close() {
    if(client != null){
      client.shutdown();
    }
    System.exit(0);
  }

  public static void setBucket(Bucket bucket) {
    DumpRotationToHadoop.bucket = bucket;
  }

  public static void setCouchbasePros(Properties couchbasePros) {
    DumpRotationToHadoop.couchbasePros = couchbasePros;
  }
}