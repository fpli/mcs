package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.deps.io.netty.util.internal.StringUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewRow;
import com.ebay.app.raptor.chocolate.constant.MPLXClientEnum;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import com.ebay.dukes.CacheClient;
import com.ebay.traffic.chocolate.monitoring.ESMetrics;
import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

public class DumpRotationToTD {
  static Logger logger = LoggerFactory.getLogger(DumpRotationToTD.class);
  private static CorpRotationCouchbaseClient client;

  private static Bucket bucket;

  private static Properties couchbasePros;

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

    init(configFilePath);

    try {
      client = new CorpRotationCouchbaseClient(couchbasePros);
      CacheClient cacheClient = client.getCacheClient();
      bucket = client.getBuctet(cacheClient);
      dumpFileFromCouchbase(updateTimeStartKey, updateTimeEndKey, outputFilePath);
      client.returnClient(cacheClient);
    } catch (Exception e) {
      logger.error(e.getMessage());
      throw e;
    } finally {
      close();
    }
  }

   public static void init(String configFilePath) throws IOException {
    couchbasePros = new Properties();
    InputStream in = new FileInputStream(configFilePath);
    couchbasePros.load(in);
  }


  public static void dumpFileFromCouchbase(String startKey, String endKey, String outputFilePath) throws IOException {
    ESMetrics.init("batch-metrics-", couchbasePros.getProperty("chocolate.elasticsearch.url"));
    ESMetrics esMetrics = ESMetrics.getInstance();

    // File Path
    if (outputFilePath == null) {
      outputFilePath = couchbasePros.getProperty("job.dumpLegacyRotationFiles.outputFilePath");
    }
    // If the file need to be compressed, set "true".  default is "false"
    Boolean compress = (couchbasePros.getProperty("job.dumpLegacyRotationFiles.compressed") == null) ? Boolean.valueOf(couchbasePros.getProperty("job.dumpLegacyRotationFiles.compressed")) : Boolean.FALSE;

    List<ViewRow> result = null;

    if (StringUtils.isNotEmpty(startKey) && Long.valueOf(startKey) > -1
        && StringUtils.isNotEmpty(endKey) && Long.valueOf(endKey) > -1) {

      ViewQuery query = ViewQuery.from(couchbasePros.getProperty("couchbase.corp.rotation.designName"),
          couchbasePros.getProperty("couchbase.corp.rotation.viewName"));
      query.startKey(Long.valueOf(startKey));
      query.endKey(Long.valueOf(endKey));
      result = bucket.query(query).allRows();
    }
    int size = 0;
    if(result != null) size = result.size();

    esMetrics.meter("rotation.dump.FromCBToTD.total", size);
    // sample: 2018-02-22_01_rotations.txt
    genFileForRotation(outputFilePath, compress, result, esMetrics);
    // sample: 2018-02-22_01_campaigns.txt
    genFileForCampaign(outputFilePath, compress, result, esMetrics);
    // sample: 2018-02-22_01_creatives.txt
    genEmptyFile(outputFilePath + RotationConstant.FILE_NAME_CREATIVES, compress, RotationConstant.FILE_HEADER_CREATIVES);
    // sample: 2018-02-22_01_rotation-creative.txt
    genEmptyFile(outputFilePath + RotationConstant.FILE_NAME_ROTATION_CREATIVE, compress, RotationConstant.FILE_HEADER_ROTATION_CREATIVE);
    // sample: 2018-02-22_01_df.status
    genEmptyFile(outputFilePath + RotationConstant.FILE_NAME_DF, compress, null);
    // sample: 2018-02-22_01_position_rotations.txt
    genEmptyFile(outputFilePath + RotationConstant.FILE_NAME_POSITION_ROTATION, compress, RotationConstant.FILE_HEADER_POSITION_ROTATION);
    // sample: 2018-02-22_01_position_rules.txt
    genEmptyFile(outputFilePath + RotationConstant.FILE_NAME_POSITION_RULES, compress, RotationConstant.FILE_HEADER_POSITION_RULES);
    // sample: 2018-02-22_01_positions.txt
    genEmptyFile(outputFilePath + RotationConstant.FILE_NAME_POSITION, compress, RotationConstant.FILE_HEADER_POSITION);
    // sample: 2018-02-22_03_rules.txt
    genEmptyFile(outputFilePath + RotationConstant.FILE_NAME_RULES, compress, RotationConstant.FILE_HEADER_RULES);
    // sample: 2018-02-22_01_lt_roi.txt
    genEmptyFile(outputFilePath + RotationConstant.FILE_NAME_LT, compress, RotationConstant.FILE_HEADER_LT);
    // sample: 2018-02-22_01_roi_credit_v2.txt
    genEmptyFile(outputFilePath + RotationConstant.FILE_NAME_ROI_CREDIT, compress, RotationConstant.FILE_HEADER_ROI_CREDIT);
    // sample: 2018-02-22_01_roi_v2.txt
    genEmptyFile(outputFilePath + RotationConstant.FILE_NAME_ROI, compress, RotationConstant.FILE_HEADER_ROI);

    esMetrics.flushMetrics();
  }

  private static void close() {
    if (client != null) {
      client.shutdown();
    }
    System.exit(0);
  }

  private static void genFileForRotation(String output, boolean compress, List<ViewRow> result, ESMetrics esMetrics) throws IOException {
    OutputStream out = null;
    String filePath = output + RotationConstant.FILE_NAME_ROTATIONS + RotationConstant.FILE_NAME_SUFFIX_TXT;
    Integer count = 0;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_ZIP), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(filePath));
      }
      out.write(RotationConstant.FILE_HEADER_ROTATIONS.getBytes());
      out.write(RotationConstant.RECORD_SEPARATOR);

      if (result == null) {
        out.flush();
        out.close();
        logger.info("Successfully generate empty file " + filePath);
        return;
      }

      RotationInfo rotationInfo = null;
      Gson gson = new Gson();
      Map rotationTag = null;
      for (ViewRow row : result) {
        rotationInfo = gson.fromJson(row.value().toString(), RotationInfo.class);
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
        //|Size
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
          start = StringUtil.isNullOrEmpty(start) ? start : start.replace("-", "");
          out.write(start.getBytes());
        }
        // |Rotation Date End
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (rotationTag != null && rotationTag.get(RotationConstant.FIELD_ROTATION_END_DATE) != null) {
          String end = String.valueOf(rotationTag.get(RotationConstant.FIELD_ROTATION_END_DATE));
          end = StringUtil.isNullOrEmpty(end) ? end : end.replace("-", "");
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
        // |Client ID
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
        out.write("||||||||||".getBytes());
        out.write(RotationConstant.RECORD_SEPARATOR);
        out.flush();
        count++;
      }
    } catch (IOException e) {
      esMetrics.meter("rotation.dump.FromCBToTD.rotation.error");
      logger.error("Error happened when write couchbase data to legacy rotation file");
      throw e;
    } finally {
      if (out != null) out.close();
    }
    esMetrics.meter("rotation.dump.FromCBToTD.rotation.success", count);
    logger.info("Successfully dump " + count + " records into " + filePath);
  }

  private static void genFileForCampaign(String output, boolean compress, List<ViewRow> result, ESMetrics esMetrics) throws IOException {
    OutputStream out = null;
    String filePath = output + RotationConstant.FILE_NAME_CAMPAIGN;
    Integer count = 0;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_ZIP), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_TXT));
      }
      out.write(RotationConstant.FILE_HEADER_CAMPAIGN.getBytes());
      out.write(RotationConstant.RECORD_SEPARATOR);

      if (result == null) {
        out.flush();
        out.close();
        logger.info("Successfully generate empty file " + filePath);
        return;
      }

      RotationInfo rotationInfo = null;
      Gson gson = new Gson();
      for (ViewRow row : result) {
        rotationInfo = gson.fromJson(row.value().toString(), RotationInfo.class);
        if (rotationInfo.getCampaign_id() == null) {
          continue;
        }
        // CLIENT ID|CAMPAIGN ID|
        String rotationStr = rotationInfo.getRotation_string();
        Integer clientId = Integer.valueOf(rotationStr.split("-")[0]);
        MPLXClientEnum clientEnum = MPLXClientEnum.getByClientId(clientId);
        if (clientEnum != null) {
          out.write(String.valueOf(clientEnum.getMplxClientId()).getBytes());
        }
        out.write(RotationConstant.FIELD_SEPARATOR);
        writeString(out, rotationInfo.getCampaign_id());
        out.write(RotationConstant.FIELD_SEPARATOR);
        // CLIENT NAME|CAMPAIGN NAME
        if (clientEnum != null) {
          out.write(clientEnum.getMplxClientName().getBytes());
        }
        out.write(RotationConstant.FIELD_SEPARATOR);
        writeString(out, rotationInfo.getCampaign_name());
        out.write(RotationConstant.RECORD_SEPARATOR);
        out.flush();
        count++;
      }
    } catch (IOException e) {
      esMetrics.meter("rotation.dump.FromCBToTD.campaign.error");
      logger.error("Error happened when write couchbase data to legacy rotation file");
      throw e;
    } finally {
      if (out != null) out.close();
    }
    esMetrics.meter("rotation.dump.FromCBToTD.campaign.success", count);
    logger.info("Successfully dump " + count + " records into " + filePath);
  }

  private static void genEmptyFile(String outputFilePath, boolean compress, String fileHeaders) throws IOException {
    OutputStream out = null;
    String filePath = outputFilePath;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_ZIP), 8192);
      } else {
        if(StringUtils.isNotEmpty(fileHeaders)) filePath = filePath + RotationConstant.FILE_NAME_SUFFIX_TXT;
        out = new BufferedOutputStream(new FileOutputStream(filePath));
      }
      if (StringUtils.isNotEmpty(fileHeaders)) {
        out.write(fileHeaders.getBytes());
        out.write(RotationConstant.RECORD_SEPARATOR);
      }
      out.flush();
      out.close();
      logger.info("Successfully generate empty file " + filePath);
    } catch (IOException e) {
      logger.error("Error happened when write couchbase data to legacy rotation file");
      throw e;
    } finally {
      if (out != null) out.close();
    }
  }

  /**
   * Set Mocked Couchbase Bucket for Unit Testing
   * @param bucket mocked couchbase bucket
   */
  public static void setBucket(Bucket bucket) {
    DumpRotationToTD.bucket = bucket;
  }

  /**
   * Set Mocked Couchbase properties for Unit Testing
   * @param couchbasePros couchbase properties
   */
  public static void setCouchbasePros(Properties couchbasePros) {
    DumpRotationToTD.couchbasePros = couchbasePros;
  }

  private static void writeString(OutputStream out, Object content) throws IOException {
    String s = content == null? null : String.valueOf(content);
    if(StringUtils.isNotEmpty(s)) out.write(s.getBytes());
  }
}
