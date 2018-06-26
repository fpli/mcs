package com.ebay.traffic.chocolate.couchbase;

import com.couchbase.client.deps.io.netty.util.internal.StringUtil;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.ebay.app.raptor.chocolate.constant.MPLXChannelEnum;
import com.ebay.app.raptor.chocolate.constant.MPLXClientEnum;
import com.ebay.app.raptor.chocolate.constant.RotationConstant;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;


public class DumpLegacyRotationFiles {
  static Logger logger = LoggerFactory.getLogger(DumpLegacyRotationFiles.class);
  private static Cluster cluster;
  private static Bucket bucket;
  private static Properties couchbasePros;

  public static void main(String args[]) throws IOException {
    String configFilePath = (args != null && args.length > 0) ? args[0] : null;
    if (StringUtils.isEmpty(configFilePath))
      logger.error("No configFilePath was defined. please set configFilePath for rotation jobs");

    String lastUpdateTime = (args != null && args.length > 1) ? args[1] : null;
    if (StringUtils.isEmpty(lastUpdateTime))
      logger.error("No lastUpdateTime was defined. please set lastUpdateTime for rotation jobs");

    String outputFilePath = (args != null && args.length > 2) ? args[2] : null;

    init(configFilePath);

    try {
      connect();
      dumpFileFromCouchbase(lastUpdateTime, outputFilePath);
    } finally {
      close();
    }
  }

  private static void init(String configFilePath) throws IOException {
    couchbasePros = new Properties();
    InputStream in = new FileInputStream(configFilePath);
    ;
    couchbasePros.load(in);
  }

  private static void connect() {
    CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().connectTimeout(100000).queryTimeout(500000).build();
    cluster = CouchbaseCluster.create(env, couchbasePros.getProperty("couchbase.cluster.rotation"));
    cluster.authenticate(couchbasePros.getProperty("couchbase.user.rotation"), couchbasePros.getProperty("couchbase.password.rotation"));
    bucket = cluster.openBucket(couchbasePros.getProperty("couchbase.bucket.rotation"), 300000, TimeUnit.SECONDS);
  }

  private static void dumpFileFromCouchbase(String lastUpdateTime, String outputFilePath) throws IOException {
    // File Path
    if (outputFilePath == null) {
      outputFilePath = couchbasePros.getProperty("job.dumpLegacyRotationFiles.outputFilePath");
    }
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH_");
    outputFilePath = outputFilePath + sdf.format(System.currentTimeMillis());
    // If the file need to be compressed, set "true".  default is "false"
    Boolean compress = (couchbasePros.getProperty("job.dumpLegacyRotationFiles.compressed") == null) ? Boolean.valueOf(couchbasePros.getProperty("job.dumpLegacyRotationFiles.compressed")) : Boolean.FALSE;
    // n1qlQueryString
    String n1qlQueryString = couchbasePros.getProperty("job.dumpRotationFiles.n1ql");
    n1qlQueryString = String.format(n1qlQueryString, lastUpdateTime);


    N1qlQueryResult result = bucket.query(N1qlQuery.simple(n1qlQueryString));
    // sample: 2018-02-22_01_rotations.txt
    genFileForRotation(outputFilePath, compress, result);
    // sample: 2018-02-22_01_campaigns.txt
    genFileForCampaign(outputFilePath, compress, result);
    // sample: 2018-02-22_01_creatives.txt
    genFileForCreatives(outputFilePath, compress, result);
    // sample: 2018-02-22_01_rotation-creative.txt
    genFileForRotationCreatives(outputFilePath, compress, result);
    // sample: 2018-02-22_01_df.status
    genFileForDFStatus(outputFilePath, compress, result);
    // sample: 2018-02-22_01_lt_roi.txt
    genFileForLtRoi(outputFilePath, compress, result);
    // sample: 2018-02-22_01_position_rotations.txt
    genFileForPositionRotation(outputFilePath, compress, result);
    // sample: 2018-02-22_01_position_rules.txt
    genFileForPositionRule(outputFilePath, compress, result);
    // sample: 2018-02-22_01_positions.txt
    genFileForPosition(outputFilePath, compress, result);
    // sample: 2018-02-22_01_roi_credit_v2.txt
    genFileForRoiCredit(outputFilePath, compress, result);
    // sample: 2018-02-22_01_roi_v2.txt
    genFileForRoiV2(outputFilePath, compress, result);
    // sample: 2018-02-22_03_rules.txt
    genFileForRules(outputFilePath, compress, result);
  }

  private static void close() {
    bucket.close();
    cluster.disconnect();
  }

  private static void genFileForRotation(String output, boolean compress, N1qlQueryResult result) throws IOException {
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


      JsonObject rotationInfo = null;
      JsonObject rotationTag = null;
      for (N1qlQueryRow row : result) {
        rotationInfo = row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO);
        if (rotationInfo.containsKey(RotationConstant.CHOCO_ROTATION_TAG)) {
          rotationTag = rotationInfo.getObject(RotationConstant.CHOCO_ROTATION_TAG);
        } else {
          rotationTag = null;
        }
        // Rotation ID|Rotation String
        out.write(String.valueOf(rotationInfo.getLong(RotationConstant.FIELD_ROTATION_ID)).getBytes());
        out.write(RotationConstant.FIELD_SEPARATOR);
        out.write(rotationInfo.getString(RotationConstant.FIELD_ROTATION_STRING).getBytes());

        MPLXClientEnum clientEnum = null;
        if (rotationInfo.containsKey(RotationConstant.CHOCO_SITE_ID)) {
          clientEnum = MPLXClientEnum.getBySiteId(rotationInfo.getInt(RotationConstant.CHOCO_SITE_ID));
        }
        // |Rotation Name
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (rotationInfo.containsKey(RotationConstant.FIELD_ROTATION_NAME)) {
          out.write(rotationInfo.getString(RotationConstant.FIELD_ROTATION_NAME).getBytes());
        }
        //|Size
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (rotationTag != null && "I".equalsIgnoreCase(rotationTag.getString(RotationConstant.FIELD_ROTATION_COUNT_TYPE))) {
          out.write(rotationTag.getString("1x1").getBytes());
        }
        // |Channel ID
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (rotationInfo.containsKey(RotationConstant.FIELD_CHANNEL_ID)) {
          MPLXChannelEnum mplxChannelEnum = MPLXChannelEnum.getByRoverChannelId(rotationInfo.getInt(RotationConstant.FIELD_CHANNEL_ID));
          Integer channelId = mplxChannelEnum == null ? rotationInfo.getInt(RotationConstant.FIELD_CHANNEL_ID) : mplxChannelEnum.getMplxChannelId();
          out.write(String.valueOf(channelId).getBytes());
        }
        // |Rotation Click Thru URL
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (rotationTag != null && rotationTag.containsKey(RotationConstant.FIELD_ROTATION_CLICK_THRU_URL)) {
          out.write(rotationTag.getString(RotationConstant.FIELD_ROTATION_CLICK_THRU_URL).getBytes());
        }
        // |Rotation Status
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (rotationInfo.containsKey(RotationConstant.FIELD_ROTATION_STATUS)) {
          out.write(rotationInfo.getString(RotationConstant.FIELD_ROTATION_STATUS).getBytes());
        }
        // |Rotation Cost (Rate)|Rotation Count|
        out.write(RotationConstant.FIELD_SEPARATOR);
        out.write(String.valueOf(0).getBytes());
        out.write(RotationConstant.FIELD_SEPARATOR);
        out.write(String.valueOf(0).getBytes());
        out.write(RotationConstant.FIELD_SEPARATOR);

        // Rotation Count Type
        if (rotationTag != null && rotationTag.containsKey(RotationConstant.FIELD_ROTATION_COUNT_TYPE)) {
          out.write(String.valueOf(rotationTag.get(RotationConstant.FIELD_ROTATION_COUNT_TYPE)).getBytes());
        }
        // |Rotation Date Start
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (rotationTag != null && rotationTag.containsKey(RotationConstant.FIELD_ROTATION_START_DATE)) {
          String start = rotationTag.getString(RotationConstant.FIELD_ROTATION_START_DATE);
          start = StringUtil.isNullOrEmpty(start) ? start : start.replace("-", "");
          out.write(start.getBytes());
        }
        // |Rotation Date End
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (rotationTag != null && rotationTag.containsKey(RotationConstant.FIELD_ROTATION_END_DATE)) {
          String end = rotationTag.getString(RotationConstant.FIELD_ROTATION_END_DATE);
          end = StringUtil.isNullOrEmpty(end) ? end : end.replace("-", "");
          out.write(end.getBytes());
        }
        // |Rotation Description
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (rotationInfo.containsKey(RotationConstant.FIELD_ROTATION_DESCRIPTION)) {
          out.write(rotationInfo.getString(RotationConstant.FIELD_ROTATION_DESCRIPTION).getBytes());
        }
        // |Org Code|TO-Std|TO-JS|TO-text|TO-text-tracer
        out.write("|||||".getBytes());
        // |Vendor ID
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (rotationInfo.containsKey(RotationConstant.FIELD_VENDOR_ID)) {
          out.write(String.valueOf(rotationInfo.getInt(RotationConstant.FIELD_VENDOR_ID)).getBytes());
        }
        // |Vendor Name
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (rotationInfo.containsKey(RotationConstant.FIELD_VENDOR_NAME)) {
          out.write(rotationInfo.getString(RotationConstant.FIELD_VENDOR_NAME).getBytes());
        }
        // |Vendor URL
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (rotationTag != null && rotationTag.containsKey(RotationConstant.FIELD_VENDOR_URL)) {
          out.write(rotationTag.getString(RotationConstant.FIELD_VENDOR_URL).getBytes());
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
        if (rotationInfo.containsKey(RotationConstant.FIELD_CAMPAIGN_ID)) {
          out.write(String.valueOf(rotationInfo.getLong(RotationConstant.FIELD_CAMPAIGN_ID)).getBytes());
        }
        // |Client Name
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (clientEnum != null) {
          out.write(clientEnum.getMplxClientName().getBytes());
        }
        // |Campaign Name
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (rotationInfo.containsKey(RotationConstant.FIELD_CAMPAIGN_NAME)) {
          out.write(rotationInfo.getString(RotationConstant.FIELD_CAMPAIGN_NAME).getBytes());
        }
        // |Placement ID
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (rotationTag.containsKey(RotationConstant.FIELD_PLACEMENT_ID)) {
          out.write(String.valueOf(rotationTag.getLong(RotationConstant.FIELD_PLACEMENT_ID)).getBytes());
        }

        // |Perf track 1|Perf track 2|Perf track 3|Perf track 4|Perf track 5|Perf track 6|Perf track 7|Perf track 8|Perf track 9|Perf track 10
        out.write("||||||||||".getBytes());
        out.write(RotationConstant.RECORD_SEPARATOR);
        out.flush();
        count++;
      }
    } catch (IOException e) {
      System.out.println("Error happened when write couchbase data to legacy rotation file");
      throw e;
    } finally {
      out.close();
    }
    System.out.println("Successfully dump " + count + " records into " + filePath);
  }

  private static void genFileForCampaign(String output, boolean compress, N1qlQueryResult result) throws IOException {
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

      JsonObject rotationInfo = null;
      for (N1qlQueryRow row : result) {
        rotationInfo = row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO);
        if (rotationInfo == null) continue;

        if (rotationInfo.getLong(RotationConstant.FIELD_CAMPAIGN_ID) == null) {
          continue;
        }
        // CLIENT ID|CAMPAIGN ID|
        MPLXClientEnum clientEnum = null;
        if (rotationInfo.containsKey(RotationConstant.CHOCO_SITE_ID)) {
          clientEnum = MPLXClientEnum.getBySiteId(rotationInfo.getInt(RotationConstant.CHOCO_SITE_ID));
          out.write(String.valueOf(clientEnum.getMplxClientId()).getBytes());
        }
        out.write(RotationConstant.FIELD_SEPARATOR);
        if (rotationInfo.containsKey(RotationConstant.FIELD_CAMPAIGN_ID)) {
          out.write(String.valueOf(rotationInfo.getLong(RotationConstant.FIELD_CAMPAIGN_ID)).getBytes());
        }
        out.write(RotationConstant.FIELD_SEPARATOR);
        // CLIENT NAME|CAMPAIGN NAME
        if (clientEnum != null) {
          out.write(clientEnum.getMplxClientName().getBytes());
        }
        out.write(RotationConstant.FIELD_SEPARATOR);
        // CAMPAIGN NAME
        if (rotationInfo.containsKey(RotationConstant.FIELD_CAMPAIGN_NAME)) {
          out.write(rotationInfo.getString(RotationConstant.FIELD_CAMPAIGN_NAME).getBytes());
        }

        out.write(RotationConstant.RECORD_SEPARATOR);
        out.flush();
        count++;
      }
    } catch (IOException e) {
      System.out.println("Error happened when write couchbase data to legacy rotation file");
      throw e;
    } finally {
      out.close();
    }
    System.out.println("Successfully dump " + count + " records into " + filePath);
  }

  private static void genFileForRotationCreatives(String output, boolean compress, N1qlQueryResult result) throws IOException {
    OutputStream out = null;
    String filePath = output + RotationConstant.FILE_NAME_ROTATION_CREATIVE;
    Integer count = 0;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_ZIP), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_TXT));
      }
      out.write(RotationConstant.FILE_HEADER_ROTATION_CREATIVE.getBytes());
      out.write(RotationConstant.RECORD_SEPARATOR);

      JsonObject rotationInfo = null;
      JsonObject rotationTag = null;
      for (N1qlQueryRow row : result) {
        rotationInfo = row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO);
        String rotationId = String.valueOf(rotationInfo.getLong(RotationConstant.FIELD_ROTATION_ID));

        if (rotationInfo.containsKey(RotationConstant.CHOCO_ROTATION_TAG)) {
          rotationTag = rotationInfo.getObject(RotationConstant.CHOCO_ROTATION_TAG);
        } else {
          rotationTag = null;
        }
        if (rotationTag == null) continue;

        if (rotationTag.containsKey(RotationConstant.FIELD_CREATIVE_SETS)) {
          JsonArray creativeSets = rotationTag.getArray(RotationConstant.FIELD_CREATIVE_SETS);
          if (creativeSets != null && creativeSets.size() > 0) {
            for (int i = 0; i < creativeSets.size(); i++) {
              setPositionCreatives(out, creativeSets.getObject(i), rotationId);
              count++;
            }
          }
        } else {
          setPositionCreatives(out, rotationTag, rotationId);
          count++;
        }
      }
    } catch (IOException e) {
      System.out.println("Error happened when write couchbase data to legacy rotation file");
      throw e;
    } finally {
      out.close();
    }
    System.out.println("Successfully dump " + count + " records into " + filePath);
  }

  private static void setPositionCreatives(OutputStream out, JsonObject creativeSets, String rotationId18) throws IOException {
    // Rot-Cr ID|Rotation ID|Creative ID|Creative Set Name|Weight|Creative Click Thru URL|Creative Date Start|Creative Date End|Rot-Cr Status|Org Code
    if (creativeSets.containsKey(RotationConstant.FIELD_ROT_CR_ID)) {
      out.write(String.valueOf(creativeSets.get(RotationConstant.FIELD_ROT_CR_ID)).getBytes());
    } else {
      return;
    }

    out.write(RotationConstant.FIELD_SEPARATOR);
    out.write(rotationId18.getBytes());

    out.write(RotationConstant.FIELD_SEPARATOR);
    if (creativeSets.containsKey(RotationConstant.FIELD_CREATIVE_ID)) {
      out.write(creativeSets.getString(RotationConstant.FIELD_CREATIVE_ID).getBytes());
    }
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (creativeSets.containsKey(RotationConstant.FIELD_CREATIVE_SET_NAME)) {
      out.write(creativeSets.getString(RotationConstant.FIELD_CREATIVE_SET_NAME).getBytes());
    }
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (creativeSets.containsKey(RotationConstant.FIELD_CREATIVE_WEIGHT)) {
      out.write(creativeSets.getString(RotationConstant.FIELD_CREATIVE_WEIGHT).getBytes());
    }
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (creativeSets.containsKey(RotationConstant.FIELD_CREATIVE_CLICK_THRU_URL)) {
      out.write(creativeSets.getString(RotationConstant.FIELD_CREATIVE_CLICK_THRU_URL).getBytes());
    }
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (creativeSets.containsKey(RotationConstant.FIELD_CREATIVE_DATE_START)) {
      out.write(creativeSets.getString(RotationConstant.FIELD_CREATIVE_DATE_START).getBytes());
    }
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (creativeSets.containsKey(RotationConstant.FIELD_CREATIVE_DATE_END)) {
      out.write(creativeSets.getString(RotationConstant.FIELD_CREATIVE_DATE_END).getBytes());
    }
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (creativeSets.containsKey(RotationConstant.FIELD_ROT_CR_STATUS)) {
      out.write(creativeSets.getString(RotationConstant.FIELD_ROT_CR_STATUS).getBytes());
    }
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (creativeSets.containsKey(RotationConstant.FIELD_CREATIVE_ORG_CODE)) {
      out.write(creativeSets.getString(RotationConstant.FIELD_CREATIVE_ORG_CODE).getBytes());
    }
    out.write(RotationConstant.RECORD_SEPARATOR);
    out.flush();
  }

  private static void genFileForCreatives(String output, boolean compress, N1qlQueryResult result) throws IOException {
    OutputStream out = null;
    String filePath = output + RotationConstant.FILE_NAME_CREATIVES;
    Integer count = 0;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_ZIP), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_TXT));
      }
      out.write(RotationConstant.FILE_HEADER_CREATIVES.getBytes());
      out.write(RotationConstant.RECORD_SEPARATOR);

      JsonObject rotationInfo = null;
      JsonObject rotationTag = null;
      for (N1qlQueryRow row : result) {
        rotationInfo = row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO);
        if (rotationInfo.containsKey(RotationConstant.CHOCO_ROTATION_TAG)) {
          rotationTag = rotationInfo.getObject(RotationConstant.CHOCO_ROTATION_TAG);
        } else {
          rotationTag = null;
        }
        if (rotationTag == null) continue;

        if (rotationTag.containsKey(RotationConstant.FIELD_CREATIVE_SETS)) {
          JsonArray creativeSets = rotationTag.getArray(RotationConstant.FIELD_CREATIVE_SETS);
          if (creativeSets != null && creativeSets.size() > 0) {
            for (int i = 0; i < creativeSets.size(); i++) {
              setCreativeSets(out, creativeSets.getObject(i), rotationInfo);
              count++;
            }
          }
        } else {
          setCreativeSets(out, rotationTag, rotationInfo);
          count++;
        }
      }
    } catch (IOException e) {
      System.out.println("Error happened when write couchbase data to legacy rotation file");
      throw e;
    } finally {
      out.close();
    }
    System.out.println("Successfully dump " + count + " records into " + filePath);
  }

  private static void setCreativeSets(OutputStream out, JsonObject creativeSets, JsonObject rotationInfo) throws IOException {
    if (!creativeSets.containsKey(RotationConstant.FIELD_CREATIVE_ID)) {
      return;
    }
    //Creative ID
    if (creativeSets.containsKey(RotationConstant.FIELD_CREATIVE_ID)) {
      out.write(creativeSets.getString(RotationConstant.FIELD_CREATIVE_ID).getBytes());
    }
    //|Creative File Name
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (creativeSets.containsKey(RotationConstant.FIELD_CREATIVE_FILE_NAME)) {
      out.write(creativeSets.getString(RotationConstant.FIELD_CREATIVE_FILE_NAME).getBytes());
    }
    //|Creative Location
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (creativeSets.containsKey(RotationConstant.FIELD_CREATIVE_LOCATION)) {
      out.write(creativeSets.getString(RotationConstant.FIELD_CREATIVE_LOCATION).getBytes());
    }
    //|Size
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (creativeSets.containsKey(RotationConstant.FIELD_CREATIVE_SIZE)) {
      out.write(creativeSets.getString(RotationConstant.FIELD_CREATIVE_SIZE).getBytes());
    }
    //|Org Code
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (creativeSets.containsKey(RotationConstant.FIELD_CREATIVE_ORG_CODE)) {
      out.write(creativeSets.getString(RotationConstant.FIELD_CREATIVE_ORG_CODE).getBytes());
    }
    //|Creative Type
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (creativeSets.containsKey(RotationConstant.FIELD_CREATIVE_TYPE)) {
      out.write(creativeSets.getString(RotationConstant.FIELD_CREATIVE_TYPE).getBytes());
    }
    //|Campaign ID
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (rotationInfo.containsKey(RotationConstant.FIELD_CAMPAIGN_ID)) {
      out.write(String.valueOf(rotationInfo.getLong(RotationConstant.FIELD_CAMPAIGN_ID)).getBytes());
    }
    //|Campaign Name
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (rotationInfo.containsKey(RotationConstant.FIELD_CAMPAIGN_NAME)) {
      out.write(rotationInfo.getString(RotationConstant.FIELD_CAMPAIGN_NAME).getBytes());
    }
    //|Client ID|Client Name
    MPLXClientEnum clientEnum = null;
    if (rotationInfo.containsKey(RotationConstant.CHOCO_SITE_ID)) {
      clientEnum = MPLXClientEnum.getBySiteId(rotationInfo.getInt(RotationConstant.CHOCO_SITE_ID));
    }
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (clientEnum != null) {
      out.write(String.valueOf(clientEnum.getMplxClientId()).getBytes());
    }
    out.write(RotationConstant.FIELD_SEPARATOR);
    if (clientEnum != null) {
      out.write(String.valueOf(clientEnum.getMplxClientName()).getBytes());
    }
    out.write(RotationConstant.RECORD_SEPARATOR);
    out.flush();
  }

  private static void genFileForPosition(String output, boolean compress, N1qlQueryResult result) throws IOException {
    OutputStream out = null;
    String filePath = output + RotationConstant.FILE_NAME_POSITION;
    Integer count = 0;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_ZIP), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_TXT));
      }
      out.write(RotationConstant.FILE_HEADER_POSITION.getBytes());
      out.write(RotationConstant.RECORD_SEPARATOR);

      JsonObject rotationInfo = null;
      JsonObject rotationTag = null;
      for (N1qlQueryRow row : result) {
        rotationInfo = row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO);
        if (rotationInfo.containsKey(RotationConstant.CHOCO_ROTATION_TAG)) {
          rotationTag = rotationInfo.getObject(RotationConstant.CHOCO_ROTATION_TAG);
        } else {
          rotationTag = null;
        }
        if (rotationTag == null) continue;

        if (rotationTag.containsKey(RotationConstant.FIELD_POSITIONS)) {
          JsonArray positions = rotationTag.getArray(RotationConstant.FIELD_POSITIONS);
          if (positions != null && positions.size() > 0) {
            for (int i = 0; i < positions.size(); i++) {
              // Position Id|Position Name|Size|Org Code|Active
              JsonObject position = positions.getObject(i);
              if (position.containsKey(RotationConstant.FIELD_POSITION_ID)) {
                out.write(String.valueOf(position.getInt(RotationConstant.FIELD_POSITION_ID)).getBytes());
              } else {
                continue;
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (position.containsKey(RotationConstant.FIELD_POSITION_NAME)) {
                out.write(position.getString(RotationConstant.FIELD_POSITION_NAME).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (position.containsKey(RotationConstant.FIELD_POSITION_SIZE)) {
                out.write(position.getString(RotationConstant.FIELD_POSITION_SIZE).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (position.containsKey(RotationConstant.FIELD_CREATIVE_ORG_CODE)) {
                out.write(position.getString(RotationConstant.FIELD_RULE_SET_NAME).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (position.containsKey(RotationConstant.FIELD_POSITION_ACTIVE)) {
                out.write(position.getString(RotationConstant.FIELD_POSITION_ACTIVE).getBytes());
              }
              out.write(RotationConstant.RECORD_SEPARATOR);
              out.flush();
              count++;
            }
          }
        }
      }
    } catch (IOException e) {
      System.out.println("Error happened when write couchbase data to legacy rotation file");
      throw e;
    } finally {
      out.close();
    }
    System.out.println("Successfully dump " + count + " records into " + filePath);
  }

  private static void genFileForPositionRotation(String output, boolean compress, N1qlQueryResult result) throws IOException {
    OutputStream out = null;
    String filePath = output + RotationConstant.FILE_NAME_POSITION_ROTATION;
    Integer count = 0;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_ZIP), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_TXT));
      }
      out.write(RotationConstant.FILE_HEADER_POSITION_ROTATION.getBytes());
      out.write(RotationConstant.RECORD_SEPARATOR);

      JsonObject rotationInfo = null;
      JsonObject rotationTag = null;
      for (N1qlQueryRow row : result) {
        rotationInfo = row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO);
        if (rotationInfo.containsKey(RotationConstant.CHOCO_ROTATION_TAG)) {
          rotationTag = rotationInfo.getObject(RotationConstant.CHOCO_ROTATION_TAG);
        } else {
          rotationTag = null;
        }
        if (rotationTag == null) continue;

        if (rotationTag.containsKey(RotationConstant.FIELD_POSITIONS)) {
          JsonArray positionArr = rotationTag.getArray(RotationConstant.FIELD_POSITIONS);
          if (positionArr != null && positionArr.size() > 0) {
            for (int i = 0; i < positionArr.size(); i++) {
              //Position Id
              JsonObject position = positionArr.getObject(i);
              if (position.containsKey(RotationConstant.FIELD_POSITION_ID)) {
                out.write(String.valueOf(position.getInt(RotationConstant.FIELD_POSITION_ID)).getBytes());
              } else {
                continue;
              }
              // |Set Name
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (position.containsKey(RotationConstant.FIELD_CREATIVE_SET_NAME)) {
                out.write(String.valueOf(position.get(RotationConstant.FIELD_CREATIVE_SET_NAME)).getBytes());
              }
              // |Rotation ID
              out.write(RotationConstant.FIELD_SEPARATOR);
              rotationInfo = row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO);
              out.write(String.valueOf(rotationInfo.getLong(RotationConstant.FIELD_ROTATION_ID)).getBytes());
              // |Active
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (position.containsKey(RotationConstant.FIELD_POSITION_ACTIVE)) {
                out.write(position.getString(RotationConstant.FIELD_POSITION_ACTIVE).getBytes());
              }
              // |Start Date|End Date
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (position.containsKey(RotationConstant.FIELD_POSITION_START_DATE)) {
                out.write(position.getString(RotationConstant.FIELD_POSITION_START_DATE).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (position.containsKey(RotationConstant.FIELD_POSITION_END_DATE)) {
                out.write(position.getString(RotationConstant.FIELD_POSITION_END_DATE).getBytes());
              }
              out.write(RotationConstant.RECORD_SEPARATOR);
              out.flush();
              count++;
            }
          }
        }
      }
    } catch (IOException e) {
      System.out.println("Error happened when write couchbase data to legacy rotation file");
      throw e;
    } finally {
      out.close();
    }
    System.out.println("Successfully dump " + count + " records into " + filePath);
  }

  private static void genFileForPositionRule(String output, boolean compress, N1qlQueryResult result) throws IOException {
    OutputStream out = null;
    String filePath = output + RotationConstant.FILE_NAME_POSITION_RULES;
    Integer count = 0;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_ZIP), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_TXT));
      }
      out.write(RotationConstant.FILE_HEADER_POSITION_RULES.getBytes());
      out.write(RotationConstant.RECORD_SEPARATOR);

      JsonObject rotationInfo = null;
      JsonObject rotationTag = null;
      for (N1qlQueryRow row : result) {
        rotationInfo = row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO);
        if (rotationInfo.containsKey(RotationConstant.CHOCO_ROTATION_TAG)) {
          rotationTag = rotationInfo.getObject(RotationConstant.CHOCO_ROTATION_TAG);
        } else {
          rotationTag = null;
        }
        if (rotationTag == null) continue;

        if (rotationTag.containsKey(RotationConstant.FIELD_POSITION_RULES)) {
          JsonArray positionRules = rotationTag.getArray(RotationConstant.FIELD_POSITION_RULES);
          if (positionRules != null && positionRules.size() > 0) {
            for (int i = 0; i < positionRules.size(); i++) {
              JsonObject pRule = positionRules.getObject(i);

              // Position Id|Rule ID|Rule Name|Set Name|Active|Start Date|End Date
              if (pRule.containsKey(RotationConstant.FIELD_POSITION_ID)) {
                out.write(String.valueOf(pRule.getInt(RotationConstant.FIELD_POSITION_ID)).getBytes());
              } else {
                continue;
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (pRule.containsKey(RotationConstant.FIELD_RULE_ID)) {
                out.write(String.valueOf(pRule.getLong(RotationConstant.FIELD_RULE_ID)).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (pRule.containsKey(RotationConstant.FIELD_RULE_NAME)) {
                out.write(pRule.getString(RotationConstant.FIELD_RULE_NAME).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (pRule.containsKey(RotationConstant.FIELD_RULE_SET_NAME)) {
                out.write(pRule.getString(RotationConstant.FIELD_RULE_SET_NAME).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (pRule.containsKey(RotationConstant.FIELD_RULE_ACTIVE)) {
                out.write(pRule.getString(RotationConstant.FIELD_RULE_ACTIVE).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (pRule.containsKey(RotationConstant.FIELD_RULE_START_DATE)) {
                out.write(pRule.getString(RotationConstant.FIELD_RULE_START_DATE).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (pRule.containsKey(RotationConstant.FIELD_RULE_END_DATE)) {
                out.write(pRule.getString(RotationConstant.FIELD_RULE_END_DATE).getBytes());
              }
              out.write(RotationConstant.RECORD_SEPARATOR);
              out.flush();
              count++;
            }
          }
        }
      }
    } catch (IOException e) {
      System.out.println("Error happened when write couchbase data to legacy rotation file");
      throw e;
    } finally {
      out.close();
    }
    System.out.println("Successfully dump " + count + " records into " + filePath);
  }

  private static void genFileForDFStatus(String output, boolean compress, N1qlQueryResult result) throws IOException {
    OutputStream out = null;
    String filePath = output + RotationConstant.FILE_NAME_DF;
    Integer count = 0;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_ZIP), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(filePath));
      }
    } catch (IOException e) {
      System.out.println("Error happened when write couchbase data to legacy rotation file");
      throw e;
    } finally {
      out.close();
    }
    System.out.println("Successfully dump " + count + " records into " + filePath);
  }

  private static void genFileForLtRoi(String output, boolean compress, N1qlQueryResult result) throws IOException {
    OutputStream out = null;
    String filePath = output + RotationConstant.FILE_NAME_LT;
    Integer count = 0;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_ZIP), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_TXT));
      }
      out.write(RotationConstant.FILE_HEADER_LT.getBytes());
      out.write(RotationConstant.RECORD_SEPARATOR);

      JsonObject rotationTag = null;
      for (N1qlQueryRow row : result) {
        if (row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO).containsKey(RotationConstant.CHOCO_ROTATION_TAG)) {
          rotationTag = row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO).getObject(RotationConstant.CHOCO_ROTATION_TAG);
        }
        if (rotationTag == null) continue;

        // LtRoi
        if (rotationTag.containsKey(RotationConstant.FIELD_LT_ROIS)) {
          JsonArray ltRoiArr = rotationTag.getArray(RotationConstant.FIELD_LT_ROIS);
          if (ltRoiArr != null && ltRoiArr.size() > 0) {
            for (int i = 0; i < ltRoiArr.size(); i++) {
              JsonObject ltRoi = ltRoiArr.getObject(i);
              if (!ltRoi.containsKey(RotationConstant.FIELD_UNIQUE_ID)) {
                continue;
              }
              if (ltRoi.containsKey(RotationConstant.FIELD_PACIFIC_TIMESTAMP)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_PACIFIC_TIMESTAMP).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_LT_PLACEMENT_ID)) {
                out.write(String.valueOf(ltRoi.getLong(RotationConstant.FIELD_LT_PLACEMENT_ID)).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_IM_PLACEMENT_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_IM_PLACEMENT_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_CREATIVE_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_CREATIVE_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_RULE_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_RULE_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_LT_LATENCY_TIME)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_LT_LATENCY_TIME).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_ROI_PLACEMENT_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_ROI_PLACEMENT_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_ROI_EVENT_NAME)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_ROI_EVENT_NAME).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_ROI_CATEGORY_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_ROI_CATEGORY_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_ROI_EVENT_COUNT)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_ROI_EVENT_COUNT).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_COOKIE_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_COOKIE_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_UNIQUE_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_UNIQUE_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_USER_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_USER_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_ITEM_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_ITEM_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_TRANSACTION_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_TRANSACTION_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_CART_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_CART_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_VIEWTHRU_CLICKTHRU)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_VIEWTHRU_CLICKTHRU).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_DUPLICATE)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_DUPLICATE).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_LT_KEYWORD)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_LT_KEYWORD).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_OQ_KEYWORD)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_OQ_KEYWORD).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_REFERRING_DOMAIN)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_REFERRING_DOMAIN).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_DESTINATION_URL)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_DESTINATION_URL).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_CLICK_UNIQUE_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_CLICK_UNIQUE_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_DEVICE)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_DEVICE).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_OS)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_OS).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_DEVICE_TYPE)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_DEVICE_TYPE).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_SID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_SID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_PID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_PID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_AID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_AID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_UID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_UID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_IMP_RVR_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_IMP_RVR_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_CLK_RVR_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_CLK_RVR_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_EXT_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_EXT_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_NS_RVR_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_NS_RVR_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_PARM2_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_PARM2_ID).getBytes());
              }
              out.write(RotationConstant.FIELD_SEPARATOR);
              if (ltRoi.containsKey(RotationConstant.FIELD_PARM3_ID)) {
                out.write(ltRoi.getString(RotationConstant.FIELD_PARM3_ID).getBytes());
              }
              out.write(RotationConstant.RECORD_SEPARATOR);
              out.flush();
              count++;
            }
          }
        }
      }
    } catch (IOException e) {
      System.out.println("Error happened when write couchbase data to legacy rotation file");
      throw e;
    } finally {
      out.close();
    }
    System.out.println("Successfully dump " + count + " records into " + filePath);
  }

  private static void genFileForRoiCredit(String output, boolean compress, N1qlQueryResult result) throws IOException {
    OutputStream out = null;
    String filePath = output + RotationConstant.FILE_NAME_ROI_CREDIT;
    Integer count = 0;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_ZIP), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_TXT));
      }
      out.write(RotationConstant.FILE_HEADER_ROI_CREDIT.getBytes());
      out.write(RotationConstant.RECORD_SEPARATOR);

      JsonObject rotationTag = null;
      for (N1qlQueryRow row : result) {
        if (row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO).containsKey(RotationConstant.CHOCO_ROTATION_TAG)) {
          rotationTag = row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO).getObject(RotationConstant.CHOCO_ROTATION_TAG);
        }
        if (rotationTag == null) continue;
        if (!rotationTag.containsKey(RotationConstant.FIELD_ROI_CREDITS_V2)) continue;

        JsonArray roiCreditArr = rotationTag.getArray(RotationConstant.FIELD_ROI_CREDITS_V2);
        if (roiCreditArr != null && roiCreditArr.size() > 0) {
          for (int i = 0; i < roiCreditArr.size(); i++) {
            JsonObject roiCredit = roiCreditArr.getObject(i);
            if (roiCredit.containsKey(RotationConstant.FIELD_CLICK_UNIQUE_ID)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_CLICK_UNIQUE_ID).getBytes());
            } else {
              continue;
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_IM_PLACEMENT_ID)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_IM_PLACEMENT_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_CREATIVE_ID)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_CREATIVE_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_RULE_ID)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_RULE_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_CLICK_TIMPSTAMP)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_CLICK_TIMPSTAMP).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_IM_PLACEMENT_IP)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_IM_PLACEMENT_IP).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_IM_PLACEMENT_IP_COUNTRY)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_IM_PLACEMENT_IP_COUNTRY).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PERF_TRACK_1)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PERF_TRACK_1).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PERF_TRACK_2)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PERF_TRACK_2).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PERF_TRACK_3)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PERF_TRACK_3).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PERF_TRACK_2)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PERF_TRACK_1).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PERF_TRACK_4)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PERF_TRACK_4).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PERF_TRACK_5)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PERF_TRACK_5).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PERF_TRACK_6)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PERF_TRACK_6).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PERF_TRACK_7)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PERF_TRACK_7).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PERF_TRACK_8)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PERF_TRACK_8).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PERF_TRACK_9)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PERF_TRACK_9).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PERF_TRACK_10)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PERF_TRACK_10).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PERF_TRACK_11)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PERF_TRACK_11).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PERF_TRACK_1)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PERF_TRACK_1).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_DEVICE)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_DEVICE).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_OS)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_OS).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_DEVICE_TYPE)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_DEVICE_TYPE).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_SID)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_SID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PID)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_AID)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_AID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_UID)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_UID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_IMP_RVR_ID)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_IMP_RVR_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_CLK_RVR_ID)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_CLK_RVR_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_EXT_ID)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_EXT_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_NS_RVR_ID)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_NS_RVR_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PARM2_ID)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PARM2_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roiCredit.containsKey(RotationConstant.FIELD_PARM3_ID)) {
              out.write(roiCredit.getString(RotationConstant.FIELD_PARM3_ID).getBytes());
            }
            out.write(RotationConstant.RECORD_SEPARATOR);
            out.flush();
            count++;
          }
        }
      }
    } catch (IOException e) {
      System.out.println("Error happened when write couchbase data to legacy rotation file");
      throw e;
    } finally {
      out.close();
    }
    System.out.println("Successfully dump " + count + " records into " + filePath);
  }

  private static void genFileForRoiV2(String output, boolean compress, N1qlQueryResult result) throws IOException {
    OutputStream out = null;
    String filePath = output + RotationConstant.FILE_NAME_ROI;
    Integer count = 0;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_ZIP), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_TXT));
      }
      out.write(RotationConstant.FILE_HEADER_ROI.getBytes());
      out.write(RotationConstant.RECORD_SEPARATOR);

      JsonObject rotationTag = null;
      for (N1qlQueryRow row : result) {
        if (row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO).containsKey(RotationConstant.CHOCO_ROTATION_TAG)) {
          rotationTag = row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO).getObject(RotationConstant.CHOCO_ROTATION_TAG);
        }
        if (rotationTag == null) continue;
        if (!rotationTag.containsKey(RotationConstant.FIELD_ROI_V2)) continue;

        JsonArray roiV2Array = rotationTag.getArray(RotationConstant.FIELD_ROI_V2);
        if (roiV2Array != null && roiV2Array.size() > 0) {
          for (int i = 0; i < roiV2Array.size(); i++) {
            JsonObject roi = roiV2Array.getObject(i);
            if (rotationTag.containsKey(RotationConstant.FIELD_PDT_TIMESTAMP)) {
              out.write(rotationTag.getString(RotationConstant.FIELD_PDT_TIMESTAMP).getBytes());
            } else {
              continue;
            }

            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_ROI_PLACEMENT_ID)) {
              out.write(roi.getString(RotationConstant.FIELD_ROI_PLACEMENT_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_ROI_EVENT_NAME)) {
              out.write(roi.getString(RotationConstant.FIELD_ROI_EVENT_NAME).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_ROI_EVENT_COUNT)) {
              out.write(roi.getString(RotationConstant.FIELD_ROI_EVENT_COUNT).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_COOKIE_ID)) {
              out.write(roi.getString(RotationConstant.FIELD_COOKIE_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_FREQUENCY)) {
              out.write(roi.getString(RotationConstant.FIELD_FREQUENCY).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_UNIQUE_ID)) {
              out.write(roi.getString(RotationConstant.FIELD_UNIQUE_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_USER_ID)) {
              out.write(roi.getString(RotationConstant.FIELD_USER_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_ITEM_ID)) {
              out.write(roi.getString(RotationConstant.FIELD_ITEM_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_TRANSACTION_ID)) {
              out.write(roi.getString(RotationConstant.FIELD_TRANSACTION_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_CART_ID)) {
              out.write(roi.getString(RotationConstant.FIELD_CART_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_CJ_ACTION_ID)) {
              out.write(roi.getString(RotationConstant.FIELD_CJ_ACTION_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_CONVERSION_TYPE_IND)) {
              out.write(roi.getString(RotationConstant.FIELD_CONVERSION_TYPE_IND).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_DUPLICATE)) {
              out.write(roi.getString(RotationConstant.FIELD_DUPLICATE).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_REV_SHARE_IND)) {
              out.write(roi.getString(RotationConstant.FIELD_REV_SHARE_IND).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_LATENCY_TIME)) {
              out.write(roi.getString(RotationConstant.FIELD_LATENCY_TIME).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_CLICK1_UNIQUE_ID)) {
              out.write(roi.getString(RotationConstant.FIELD_CLICK1_UNIQUE_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_CLICK2_UNIQUE_ID)) {
              out.write(roi.getString(RotationConstant.FIELD_CLICK2_UNIQUE_ID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_CJ_ONLY_SID)) {
              out.write(roi.getString(RotationConstant.FIELD_CJ_ONLY_SID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_CJ_ONLY_PID)) {
              out.write(roi.getString(RotationConstant.FIELD_CJ_ONLY_PID).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (roi.containsKey(RotationConstant.FIELD_CJ_ONLY_AID)) {
              out.write(roi.getString(RotationConstant.FIELD_CJ_ONLY_AID).getBytes());
            }
            out.write(RotationConstant.RECORD_SEPARATOR);
            out.flush();
            count++;
          }
        }
      }
    } catch (IOException e) {
      System.out.println("Error happened when write couchbase data to legacy rotation file");
      throw e;
    } finally {
      out.close();
    }
    System.out.println("Successfully dump " + count + " records into " + filePath);
  }

  private static void genFileForRules(String output, boolean compress, N1qlQueryResult result) throws IOException {
    OutputStream out = null;
    String filePath = output + RotationConstant.FILE_NAME_RULES;
    Integer count = 0;
    try {
      if (compress) {
        out = new GZIPOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_ZIP), 8192);
      } else {
        out = new BufferedOutputStream(new FileOutputStream(filePath + RotationConstant.FILE_NAME_SUFFIX_TXT));
      }
      out.write(RotationConstant.FILE_HEADER_RULES.getBytes());
      out.write(RotationConstant.RECORD_SEPARATOR);

      JsonObject rotationInfo = null;
      JsonObject rotationTag = null;
      for (N1qlQueryRow row : result) {
        rotationInfo = row.value().getObject(RotationConstant.CHOCO_ROTATION_INFO);
        if (rotationInfo.containsKey(RotationConstant.CHOCO_ROTATION_TAG)) {
          rotationTag = rotationInfo.getObject(RotationConstant.CHOCO_ROTATION_TAG);
        } else {
          rotationTag = null;
        }
        String rotationId18 = String.valueOf(rotationInfo.getLong(RotationConstant.FIELD_ROTATION_ID));

        if (rotationTag == null) continue;
        if (!rotationTag.containsKey(RotationConstant.FIELD_RULES)) continue;

        MPLXClientEnum clientEnum = null;
        if (rotationInfo.containsKey(RotationConstant.CHOCO_SITE_ID)) {
          clientEnum = MPLXClientEnum.getBySiteId(rotationInfo.getInt(RotationConstant.CHOCO_SITE_ID));
        }

        JsonArray ruleArray = rotationTag.getArray(RotationConstant.FIELD_RULES);
        if (ruleArray != null && ruleArray.size() > 0) {
          for (int i = 0; i < ruleArray.size(); i++) {
            JsonObject rule = ruleArray.getObject(i);
            // RULE ID
            if (rule.containsKey(RotationConstant.FIELD_RULE_ID)) {
              out.write(rule.getString(RotationConstant.FIELD_RULE_ID).getBytes());
            } else {
              continue;
            }
            // |RULE NAME
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (rule.containsKey(RotationConstant.FIELD_RULE_NAME)) {
              out.write(rule.getString(RotationConstant.FIELD_RULE_NAME).getBytes());
            }
            // |CREATIVE SET
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (rule.containsKey(RotationConstant.FIELD_CREATIVE_SET_NAME)) {
              out.write(rule.getString(RotationConstant.FIELD_CREATIVE_SET_NAME).getBytes());
            }
            // |CREATIVE ID
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (rule.containsKey(RotationConstant.FIELD_CREATIVE_ID)) {
              out.write(rule.getString(RotationConstant.FIELD_CREATIVE_ID).getBytes());
            }
            // |RULE CLICK THRU URL
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (rule.containsKey(RotationConstant.FIELD_CREATIVE_ID)) {
              out.write(rule.getString(RotationConstant.FIELD_CREATIVE_ID).getBytes());
            }
            // |ROTATION ID
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (rotationInfo.containsKey(RotationConstant.FIELD_ROTATION_ID)) {
              out.write(rotationId18.getBytes());
            }
            // |ROTATION NAME
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (rotationInfo.containsKey(RotationConstant.FIELD_ROTATION_NAME)) {
              out.write(rotationInfo.getString(RotationConstant.FIELD_ROTATION_NAME).getBytes());
            }
            // |CAMPAIGN ID
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (rotationInfo.containsKey(RotationConstant.FIELD_CAMPAIGN_ID)) {
              out.write(String.valueOf(rotationInfo.getLong(RotationConstant.FIELD_CAMPAIGN_ID)).getBytes());
            }
            // |CAMPAIGN NAME
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (rotationInfo.containsKey(RotationConstant.FIELD_CAMPAIGN_NAME)) {
              out.write(rotationInfo.getString(RotationConstant.FIELD_CAMPAIGN_NAME).getBytes());
            }
            // |CLIENT ID|CLIENT NAME
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (clientEnum != null) {
              out.write(String.valueOf(clientEnum.getMplxClientId()).getBytes());
            }
            out.write(RotationConstant.FIELD_SEPARATOR);
            if (clientEnum != null) {
              out.write(clientEnum.getMplxClientName().getBytes());
            }
            out.write(RotationConstant.RECORD_SEPARATOR);
            out.flush();
            count++;
          }

        }
      }
    } catch (IOException e) {
      System.out.println("Error happened when write couchbase data to legacy rotation file");
      throw e;
    } finally {
      out.close();
    }
    System.out.println("Successfully dump " + count + " records into " + filePath);
  }
}
