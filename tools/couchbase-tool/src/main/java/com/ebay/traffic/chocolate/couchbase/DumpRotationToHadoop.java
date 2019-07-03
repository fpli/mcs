package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewRow;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.ebay.dukes.CacheClient;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.log4j.PropertyConfigurator;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPOutputStream;

/**
 * Dump Rotation Info From Couchbase
 */
public class DumpRotationToHadoop {
  private static CorpRotationCouchbaseClient client;
  private static Bucket bucket;
  private static Properties couchbasePros;
  private static Logger logger = LoggerFactory.getLogger(DumpRotationToHadoop.class);

  private static final String FILENAME_SURFIX = ".txt";
  private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
  private static final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);

  //rotation es client
  private static RotationESClient rotationESClient;
  private static RestHighLevelClient esRestHighLevelClient;

  /**
   * Main Executor
   * @param args
   * @throws IOException
   */
  public static void main(String args[]) throws IOException {
    String configFilePath = (args != null && args.length > 0) ? args[0] : null;
    if (StringUtils.isEmpty(configFilePath)) {
      logger.error("No configFilePath was defined. please set configFilePath for rotation jobs");
      return;
    }
    String outputFilePath = (args != null && args.length > 1) ? args[1] : null;
    String queryStartTimestamp = (args != null && args.length > 2) ? args[2] : null;
    String queryEndTimestamp = (args != null && args.length > 3) ? args[3] : null;

    initCB(configFilePath);
    initLog4j(configFilePath);

    try {
      //couchbase client
      client = new CorpRotationCouchbaseClient(couchbasePros);
      CacheClient cacheClient = client.getCacheClient();
      bucket = client.getBuctet(cacheClient);

      //es client
      rotationESClient = new RotationESClient(couchbasePros);
      esRestHighLevelClient = rotationESClient.getESClient();

      dumpFileFromCouchbase(queryStartTimestamp, queryEndTimestamp, outputFilePath);
      client.returnClient(cacheClient);
    } finally {
      close();
    }
  }

  /**
   * Initial Couchbase configurations
   * @param configFilePath config folder
   * @throws IOException
   */
  public static void initCB(String configFilePath) throws IOException {
    couchbasePros = new Properties();
    InputStream in = new FileInputStream(configFilePath + "couchbase.properties");
    couchbasePros.load(in);
  }

  /**
   * Initial Log4j configurations
   * @param configFilePath config folder
   * @throws IOException
   */
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

  /**
   *
   * @param outputFileFolder
   * @param startKey
   * @return
   */
  private static String getOutputFilePath(String outputFileFolder, String startKey){
    String outputFilePath = outputFileFolder;

    if (outputFilePath == null) {
      outputFilePath = couchbasePros.getProperty("job.dumpRotationFiles.outputFilePath");
    }

    if(StringUtils.isEmpty(startKey)){
      SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
      outputFilePath = outputFilePath + "rotation-snapshot-" + sdf.format(new Date()) + FILENAME_SURFIX;
    }else{
      SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
      Calendar c = Calendar.getInstance();
      c.setTimeInMillis(Long.valueOf(startKey));
      outputFilePath = outputFilePath + "rotation-" + sdf.format(c.getTime()) + FILENAME_SURFIX;
    }
    return outputFilePath;
  }


  public static void dumpFileFromCouchbase(String startKey, String endKey, String outputFilePath) throws IOException {
    ESMetrics.init("batch-metrics-", couchbasePros.getProperty("chocolate.elasticsearch.url"));
    Metrics metrics = ESMetrics.getInstance();

    Boolean compress = (couchbasePros.getProperty("job.dumpRotationFiles.compressed") == null) ? Boolean.valueOf(couchbasePros.getProperty("job.dumpRotationFiles.compressed")) : Boolean.FALSE;

    OutputStream out = null;
    Integer count = 0;
    try {
      outputFilePath = outputFilePath.contains(FILENAME_SURFIX)? outputFilePath : getOutputFilePath(outputFilePath, startKey);
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(outputFilePath + ".gz"), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(outputFilePath));
      }
      ViewQuery query = ViewQuery.from(couchbasePros.getProperty("couchbase.corp.rotation.designName"),
          couchbasePros.getProperty("couchbase.corp.rotation.viewName"));

      if (StringUtils.isNotEmpty(startKey) && Long.valueOf(startKey) > -1 ) {
        query.startKey(Long.valueOf(startKey));
      }
      if (StringUtils.isNotEmpty(endKey) && Long.valueOf(endKey) > -1) {
        query.endKey(Long.valueOf(endKey));
      }

      List<ViewRow> viewResult = bucket.query(query).allRows();

      int size = 0;
      if(viewResult != null) size = viewResult.size();

      //get new-create rotation quantity and update rotation quantity from es per hour
      String esSearchStartTime = sdf.format(new Date(Long.parseLong(startKey)));
      String esSearchEndTime = sdf.format(new Date(Long.parseLong(endKey)));
      Integer newCreateRotationQuantity = getChangeRotationQuantity(esSearchStartTime, esSearchEndTime, RotationConstant.ES_CREATE_ROTATION_KEY);
      Integer updateRotationQuantity = getChangeRotationQuantity(esSearchStartTime, esSearchEndTime, RotationConstant.ES_UPDATE_ROTATION_KEY);
      Integer changeRotationQuantity = newCreateRotationQuantity + updateRotationQuantity;

      //compare rotation change quantity from es and rotation change quantity dump from couchbase
      //if rotation change quantity from es >0 but rotation dump from couchbase =0, throw couchbase dump exception
      if (changeRotationQuantity > 0 && size == 0) {
        logger.error("couchbase dump rotation data count = 0, rotation change quantity from es = " + changeRotationQuantity + ", throw exception!");
        throw new IOException("couchbase dump rotation data count = 0");
      }

      List<String> keys = new ArrayList<>();

      if (viewResult != null && viewResult.size() > 0) {
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
          if (rotationInfoStr == null) continue;
          out.write(rotationInfoStr.getBytes());
          out.write(RotationConstant.RECORD_SEPARATOR);
          out.flush();
          count++;
        }
      }

    } catch (IOException e) {
      metrics.meter("rotation.dump.FromCBToHIVE.error");
      System.out.println("Error happened when write couchbase data to chocolate file");
      throw e;
    } finally {
      if (out != null) {
        out.close();
      }
    }
    metrics.meter("rotation.dump.FromCBToHIVE.success", count);
    metrics.flush();
    System.out.println("Successfully dump " + count + " records into chocolate file: " + outputFilePath);
  }

  //get new-create rotation quantity and update rotation quantity per hour, depends on es search key
  public static Integer getChangeRotationQuantity(String esSearchStartTime, String esSearchEndTime, String esRotationKey) throws IOException {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
    boolQueryBuilder.must(QueryBuilders.matchQuery(RotationConstant.ES_SEARCH_KEY, esRotationKey));
    boolQueryBuilder.filter(QueryBuilders.rangeQuery(RotationConstant.ES_SEARCH_DATE).gte(esSearchStartTime).lte(esSearchEndTime).format(DATE_FORMAT));
    searchSourceBuilder.query(boolQueryBuilder);

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(searchSourceBuilder);
    SearchResponse searchResponse = esRestHighLevelClient.search(searchRequest, new Header[0]);
    SearchHits hits = searchResponse.getHits();
    SearchHit[] searchHits = hits.getHits();

    Integer totalChangeRotationCount = 0;
    for (SearchHit hit : searchHits) {
      Map<String, Object> sourceAsMap = hit.getSourceAsMap();
      Integer count = (Integer) sourceAsMap.get("value");
      totalChangeRotationCount = totalChangeRotationCount + count;
    }
    return totalChangeRotationCount;
  }

  private static void close() {
    if (client != null) {
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

  /**
   * Set Mocked ES RestHighLevelClient for Unit Testing
   * @param restHighLevelClient ES RestHighLevelClient
   */
  public static void setEsRestHighLevelClient(RestHighLevelClient restHighLevelClient) {
    DumpRotationToHadoop.esRestHighLevelClient = restHighLevelClient;
  }
}