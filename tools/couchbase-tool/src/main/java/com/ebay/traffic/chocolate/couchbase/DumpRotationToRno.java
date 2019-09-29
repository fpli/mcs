package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.deps.io.netty.util.internal.StringUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewRow;
import com.ebay.app.raptor.chocolate.constant.MPLXClientEnum;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.ebay.dukes.CacheClient;
import com.ebay.traffic.monitoring.ESMetrics;
import com.ebay.traffic.monitoring.Metrics;
import com.google.gson.Gson;
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
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPOutputStream;


/**
 * Dump Rotation Info From Couchbase
 */
public class DumpRotationToRno {
    private static CorpRotationCouchbaseClient client;
    private static Bucket bucket;
    private static Properties couchbasePros;
    private static Logger logger = LoggerFactory.getLogger(DumpRotationToRno.class);

    private static final String DATE_FORMAT_DETAIL = "yyyy-MM-dd HH:mm:ss";
    private static final SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_DETAIL);
    private static final DateFormat ORIGIN_DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    //rotation es client
    private static RotationESClient rotationESClient;
    private static RestHighLevelClient esRestHighLevelClient;



    public static void main(String args[]) throws Exception {
        boolean hasParams = true;
        String configFilePath = (args != null && args.length > 0) ? args[0] : null;
        if (StringUtils.isEmpty(configFilePath)){
            logger.error("No configFilePath was defined. please set configFilePath for rotation jobs");
            hasParams = false;
        }

        String updateTimeStartKey = (args != null && args.length > 1) ? args[1] : null;
        if (StringUtils.isEmpty(updateTimeStartKey)){
            logger.error("No updateTimeStartKey was defined. please set updateTimeStartKey for rotation jobs");
            hasParams = false;
        }

        String updateTimeEndKey = (args != null && args.length > 2) ? args[2] : null;
        if (StringUtils.isEmpty(updateTimeEndKey)) {
            logger.error("No updateTimeEndKey was defined. please set updateTimeEndKey for rotation jobs");
            hasParams = false;
        }

        if(!hasParams) return;

        String outputFilePath = (args != null && args.length > 3) ? args[3] : null;

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

            dumpFileFromCouchbase(updateTimeStartKey, updateTimeEndKey, outputFilePath);
            client.returnClient(cacheClient);
            rotationESClient.closeESClient(esRestHighLevelClient);
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


    public static void dumpFileFromCouchbase(String startKey, String endKey, String outputFilePath) throws Exception {
        ESMetrics.init("batch-metrics-", couchbasePros.getProperty("chocolate.elasticsearch.url"));
        Metrics metrics = ESMetrics.getInstance();

        // File Path
        if (outputFilePath == null) {
            outputFilePath = couchbasePros.getProperty("job.dumpRotationFiles.outputFilePath");
        }

        // If the file need to be compressed, set "true".  default is "false"
        Boolean compress = (couchbasePros.getProperty("job.dumpRotationFiles.compressed") == null) ? Boolean.valueOf(couchbasePros.getProperty("job.dumpRotationFiles.compressed")) : Boolean.FALSE;

        List<ViewRow> viewResult = null;

        if (StringUtils.isNotEmpty(startKey) && Long.valueOf(startKey) > -1
                && StringUtils.isNotEmpty(endKey) && Long.valueOf(endKey) > -1) {

            ViewQuery query = ViewQuery.from(couchbasePros.getProperty("couchbase.corp.rotation.designName"),
                    couchbasePros.getProperty("couchbase.corp.rotation.viewName"));
            query.startKey(Long.valueOf(startKey));
            query.endKey(Long.valueOf(endKey));
            viewResult = bucket.query(query).allRows();
        }
        int size = 0;
        if(viewResult != null) size = viewResult.size();

        List<String> keys = new ArrayList<>();

        List<JsonDocument> result = null;
        if(viewResult != null && viewResult.size() > 0) {
            for (ViewRow row : viewResult) {
                keys.add(row.id());
            }
            result = Observable
                    .from(keys)
                    .flatMap((Func1<String, Observable<JsonDocument>>) k -> bucket.async().get(k, JsonDocument.class))
                    .toList()
                    .toBlocking()
                    .single();
        }

        //get new-create rotation quantity and update rotation quantity from es per hour
        String esSearchStartTime = sdf.format(new Date(Long.parseLong(startKey)));
        String esSearchEndTime = sdf.format(new Date(Long.parseLong(endKey)));
        logger.info("es search start time: " + esSearchStartTime);
        logger.info("es search end time: " + esSearchEndTime);

        Integer newCreateRotationQuantity = getChangeRotationQuantity(esSearchStartTime, esSearchEndTime, RotationConstant.ES_CREATE_ROTATION_KEY);
        Integer updateRotationQuantity = getChangeRotationQuantity(esSearchStartTime, esSearchEndTime, RotationConstant.ES_UPDATE_ROTATION_KEY);
        Integer changeRotationQuantity = newCreateRotationQuantity + updateRotationQuantity;

        //compare rotation change quantity from es and rotation change quantity dump from couchbase
        //if rotation change quantity from es >0 but rotation dump from couchbase =0, throw couchbase dump exception
        logger.info("es change rotation quantity = " + changeRotationQuantity);
        logger.info("couchbase dump rotation data count = " + size);
        if (changeRotationQuantity > 0 && size == 0) {
            logger.error("couchbase dump rotation data count = 0, throw exception!");
            throw new IOException("couchbase dump rotation data count = 0");
        }

        metrics.meter("rotation.dump.FromCBToRno.total", size);
        metrics.meter("rotation.changeRotationQuantity", changeRotationQuantity);
        // sample: 2018-02-22_01_rotations.txt
        genFileForRotation(outputFilePath, compress, result, metrics);
        // sample: 2018-02-22_01_campaigns.txt
        genFileForCampaign(outputFilePath, compress, result, metrics);
        metrics.flush();
    }

    private static void genFileForRotation(String output, boolean compress, List<JsonDocument> result, Metrics metrics) throws Exception {
        OutputStream out = null;
        String filePath = output + RotationConstant.FILE_NAME_ROTATIONS + RotationConstant.FILE_NAME_SUFFIX_TXT;
        Integer count = 0;
        try {
            if (compress) {
                out = new GZIPOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_ZIP), 8192);
            } else {
                out = new BufferedOutputStream(new FileOutputStream(filePath));
            }

            if (result == null) {
                out.flush();
                out.close();
                logger.info("Successfully generate empty file " + filePath);
                return;
            }

            RotationInfo rotationInfo = null;
            Gson gson = new Gson();
            Map rotationTag = null;
            for (JsonDocument row : result) {
                rotationInfo = gson.fromJson(row.content().toString(), RotationInfo.class);
                rotationTag = rotationInfo.getRotation_tag();
                // Rotation ID|Rotation String
                writeString(out, rotationInfo.getRotation_id());
                out.write(RotationConstant.FIELD_SEPARATOR);
                String rotationStr = rotationInfo.getRotation_string();
                writeString(out, rotationStr);

                Integer clientId = Integer.valueOf(rotationStr.split("-")[0]);
                MPLXClientEnum clientEnum = MPLXClientEnum.getByClientId(clientId);
                // |Rotation Name
                out.write(RotationConstant.FIELD_SEPARATOR);
                writeString(out, rotationInfo.getRotation_name());
                // |Size
                out.write(RotationConstant.FIELD_SEPARATOR);
                if (rotationTag != null && "I".equalsIgnoreCase(String.valueOf(rotationTag.get(RotationConstant.FIELD_ROTATION_COUNT_TYPE)))) {
                    out.write("1x1".getBytes());
                }
                // |Channel ID
                out.write(RotationConstant.FIELD_SEPARATOR);
                out.write(String.valueOf(rotationInfo.getChannel_id()).getBytes());
                // |Rotation Click Thru URL
                out.write(RotationConstant.FIELD_SEPARATOR);
                if (rotationTag != null) {
                    writeString(out, rotationTag.get(RotationConstant.FIELD_ROTATION_CLICK_THRU_URL));
                }
                // |Rotation Status
                out.write(RotationConstant.FIELD_SEPARATOR);
                writeString(out, rotationInfo.getStatus());
                // |Rotation Cost (Rate)|Rotation Count|
                out.write(RotationConstant.FIELD_SEPARATOR);
                out.write(String.valueOf(0).getBytes());
                out.write(RotationConstant.FIELD_SEPARATOR);
                out.write(String.valueOf(0).getBytes());
                out.write(RotationConstant.FIELD_SEPARATOR);
                // Rotation Count Type
                if (rotationTag != null) {
                    writeString(out, rotationTag.get(RotationConstant.FIELD_ROTATION_COUNT_TYPE));
                }
                // |Rotation Date Start
                out.write(RotationConstant.FIELD_SEPARATOR);
                if (rotationTag != null && rotationTag.get(RotationConstant.FIELD_ROTATION_START_DATE) != null) {
                    String start = String.valueOf(rotationTag.get(RotationConstant.FIELD_ROTATION_START_DATE));
                    start = StringUtil.isNullOrEmpty(start) ? start : DATE_FORMAT.format(ORIGIN_DATE_FORMAT.parse(start));
                    out.write(start.getBytes());
                }
                // |Rotation Date End
                out.write(RotationConstant.FIELD_SEPARATOR);
                if (rotationTag != null && rotationTag.get(RotationConstant.FIELD_ROTATION_END_DATE) != null) {
                    String end = String.valueOf(rotationTag.get(RotationConstant.FIELD_ROTATION_END_DATE));
                    end = StringUtil.isNullOrEmpty(end) ? end : DATE_FORMAT.format(ORIGIN_DATE_FORMAT.parse(end));
                    out.write(end.getBytes());
                }
                // |Rotation Description
                out.write(RotationConstant.FIELD_SEPARATOR);
                writeString(out, rotationInfo.getRotation_description());
                // |Org Code|TO-Std|TO-JS|TO-text|TO-text-tracer
                out.write("|||||".getBytes());
                // |Vendor ID
                out.write(RotationConstant.FIELD_SEPARATOR);
                writeString(out, rotationInfo.getVendor_id());
                // |Vendor Name
                out.write(RotationConstant.FIELD_SEPARATOR);
                writeString(out, rotationInfo.getVendor_name());
                // |Vendor URL
                out.write(RotationConstant.FIELD_SEPARATOR);
                if (rotationTag != null) {
                    writeString(out, rotationTag.get(RotationConstant.FIELD_VENDOR_URL));
                }
                // |Vendor Type
                out.write(RotationConstant.FIELD_SEPARATOR);
                out.write(RotationConstant.FIELD_VENDOR_TYPE.getBytes());
                // | Mplx ClientId
                out.write(RotationConstant.FIELD_SEPARATOR);
                if (clientEnum != null) {
                    out.write(String.valueOf(clientEnum.getMplxClientId()).getBytes());
                }
                // |Campaign ID
                out.write(RotationConstant.FIELD_SEPARATOR);
                writeString(out, rotationInfo.getCampaign_id());
                // |Client Name
                out.write(RotationConstant.FIELD_SEPARATOR);
                if (clientEnum != null) {
                    out.write(clientEnum.getMplxClientName().getBytes());
                }
                // |Campaign Name
                out.write(RotationConstant.FIELD_SEPARATOR);
                writeString(out, rotationInfo.getCampaign_name());
                // |Placement ID
                out.write(RotationConstant.FIELD_SEPARATOR);
                if (rotationTag != null) {
                    writeString(out, rotationTag.get(RotationConstant.FIELD_PLACEMENT_ID));
                }
                // |Perf track 1|Perf track 2|Perf track 3|Perf track 4|Perf track 5|Perf track 6|Perf track 7|Perf track 8|Perf track 9|Perf track 10
                out.write("|Undefined|Undefined|Undefined|Undefined|Undefined|Undefined|Undefined|Undefined|Undefined|Undefined".getBytes());
                // |Create Date
                out.write(RotationConstant.FIELD_SEPARATOR);
                if (rotationInfo.getCreate_date() != null) {
                    String create_date = rotationInfo.getCreate_date().split(" ")[0];
                    writeString(out, create_date);
                }
                // |Create User
                out.write(RotationConstant.FIELD_SEPARATOR);
                writeString(out, rotationInfo.getCreate_user());
                // |Update User
                out.write(RotationConstant.FIELD_SEPARATOR);
                writeString(out, rotationInfo.getUpdate_user());

                out.write(RotationConstant.RECORD_SEPARATOR);
                out.flush();
                count++;
            }
        } catch (IOException e) {
            metrics.meter("rotation.dump.FromCBToHIVE.rotation.error");
            logger.error("Error happened when write couchbase data to rotation file");
            throw e;
        } finally {
            if (out != null) out.close();
        }
        metrics.meter("rotation.dump.FromCBToHIVE.rotation.success", count);
        logger.info("Successfully dump " + count + " records into " + filePath);
    }

    private static void genFileForCampaign(String output, boolean compress, List<JsonDocument> result, Metrics metrics) throws Exception {
        OutputStream out = null;
        String filePath = output + RotationConstant.FILE_NAME_CAMPAIGN + RotationConstant.FILE_NAME_SUFFIX_TXT;
        Integer count = 0;
        try {
            if (compress) {
                out = new GZIPOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_ZIP), 8192);
            } else {
                out = new BufferedOutputStream(new FileOutputStream(filePath));
            }

            if (result == null) {
                out.flush();
                out.close();
                logger.info("Successfully generate empty file " + filePath);
                return;
            }

            RotationInfo rotationInfo = null;
            Gson gson = new Gson();
            for (JsonDocument row : result) {
                rotationInfo = gson.fromJson(row.content().toString(), RotationInfo.class);
                if (rotationInfo.getCampaign_id() == null) {
                    continue;
                }
                String rotationStr = rotationInfo.getRotation_string();
                // CAMPAIGN ID
                writeString(out, rotationInfo.getCampaign_id());
                out.write(RotationConstant.FIELD_SEPARATOR);
                // CAMPAIGN NAME
                writeString(out, rotationInfo.getCampaign_name());
                out.write(RotationConstant.FIELD_SEPARATOR);
                // CLIENT ID
                Integer clientId = Integer.valueOf(rotationStr.split("-")[0]);
                MPLXClientEnum clientEnum = MPLXClientEnum.getByClientId(clientId);
                if (clientEnum != null) {
                    out.write(String.valueOf(clientEnum.getMplxClientId()).getBytes());
                }
                out.write(RotationConstant.FIELD_SEPARATOR);
                // CREATE DATE
                if (rotationInfo.getCreate_date() != null) {
                    String create_date = rotationInfo.getCreate_date().split(" ")[0];
                    writeString(out, create_date);
                }
                out.write(RotationConstant.FIELD_SEPARATOR);
                // CREATE USER
                writeString(out, rotationInfo.getCreate_user());
                out.write(RotationConstant.FIELD_SEPARATOR);
                // UPDATE USER
                writeString(out, rotationInfo.getUpdate_user());
                out.write(RotationConstant.RECORD_SEPARATOR);
                out.flush();
                count++;
            }
        }
        catch (Exception e) {
            metrics.meter("rotation.dump.FromCBToHIVE.campaign.error");
            logger.error("Error happened when write couchbase data to rotation file");
            throw e;
        } finally {
            if (out != null) out.close();
        }
        metrics.meter("rotation.dump.FromCBToHIVE.campaign.success", count);
        logger.info("Successfully dump " + count + " records into " + filePath);
    }


    //get new-create rotation quantity and update rotation quantity per hour, depends on es search key
    public static Integer getChangeRotationQuantity(String esSearchStartTime, String esSearchEndTime, String esRotationKey) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.matchQuery(RotationConstant.ES_SEARCH_KEY, esRotationKey));
        boolQueryBuilder.filter(QueryBuilders.rangeQuery(RotationConstant.ES_SEARCH_DATE).gte(esSearchStartTime).lte(esSearchEndTime).format(DATE_FORMAT_DETAIL));
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

    private static void writeString(OutputStream out, Object content) throws IOException {
        String s = content == null? null : String.valueOf(content);
        if(StringUtils.isNotEmpty(s)) out.write(s.getBytes());
    }

    public static void setBucket(Bucket bucket) {
        DumpRotationToRno.bucket = bucket;
    }

    public static void setCouchbasePros(Properties couchbasePros) {
        DumpRotationToRno.couchbasePros = couchbasePros;
    }

    /**
     * Set Mocked ES RestHighLevelClient for Unit Testing
     * @param restHighLevelClient ES RestHighLevelClient
     */
    public static void setEsRestHighLevelClient(RestHighLevelClient restHighLevelClient) {
        DumpRotationToRno.esRestHighLevelClient = restHighLevelClient;
    }

}
