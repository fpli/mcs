package com.ebay.traffic.chocolate.sparknrt.epnnrt_v2

import java.text.SimpleDateFormat

import com.ebay.app.raptor.chocolate.avro.{ChannelAction, ChannelType, FilterMessage}
import com.ebay.traffic.chocolate.common.TestHelper
import com.ebay.traffic.chocolate.spark.BaseFunSuite
import com.ebay.traffic.chocolate.sparknrt.meta.{DateFiles, MetaFiles, Metadata, MetadataEnum}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.spark.sql.functions.col

class TestEpnNrtClickJob_v2 extends BaseFunSuite{
  private val tmpPath = createTempPath()
  private val inputDir = tmpPath + "/inputDir/"
  private val inputWorkDir = tmpPath + "/inputWorkDir/"
  private val outputWorkDir = tmpPath + "/outputWorkDir/"
  private val outputDir = tmpPath + "/outputDir/"
  private val resourceDir = tmpPath

  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  @transient private lazy val hadoopConf = {
    new Configuration()
  }

  private lazy val fs = {
    val fs = FileSystem.get(hadoopConf)
    sys.addShutdownHook(fs.close())
    fs
  }

  override def beforeAll(): Unit = {
    createTestDataForEPN()
  }

  test("Test EPN Nrt Click job") {

    val args = Array(
      "--mode", "local[8]",
      "--inputWorkDir", inputWorkDir,
      "--outputWorkDir", outputWorkDir,
      "--resourceDir", resourceDir,
      "--filterTime", "0",
      "--outputDir", outputDir
    )
    val params = Parameter_v2(args)
    val job = new EpnNrtClickJob_v2(params)

    val inputMetadata = Metadata(inputWorkDir, "EPN", MetadataEnum.capping)
    val outputMetadata = Metadata(outputWorkDir, "EPN", MetadataEnum.capping)

    val dedupeMeta = inputMetadata.readDedupeOutputMeta(".epnnrt_v2")
    val dedupeMetaPath = new Path(dedupeMeta(0)._1)

    assert (fs.exists(dedupeMetaPath))
    //job.properties = properties
    job.run()
    val clickDf = job.readFilesAsDF(outputDir)
    assert(clickDf.count() == 11)

    clickDf.printSchema()

    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("CRLTN_GUID_TXT").first().getString(0) == "12cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^")
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("GUID_TXT").first().getString(0) == "12cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^")
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("USER_ID").first().getDecimal(0).longValue()== 1L)
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("USER_ID").first().getDecimal(0)== BigDecimal(1).bigDecimal)
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("CLNT_RMT_IP").first().getString(0) == "127.0.0.1")
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("RFR_URL_NAME").first().getString(0) == "http://rover.ebay.com/rover/1/1185-53479-19255-0/1?ff3=4&pub=5575118796&toolid=10001&campid=5337725402&customid=&mpre=http://www.ebay.es/itm/Etude-House-Drawing-Eye-Brow-Pencil-/191616582622%3Fpt%3DLH_DefaultDomain_0%26var%3D%26hash%3Ditem2c9d3d03de")
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("PLCMNT_DATA_TXT").first().getString(0) == "711-53200-19255-0")
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("PBLSHR_ID").first().getDecimal(0).longValue() == 7000001262L)
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("AMS_PBLSHR_CMPGN_ID").first().getDecimal(0).longValue() == 435453655L)
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("AMS_TOOL_ID").first().getDecimal(0).longValue() == 10044L)
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("CSTM_ID").first().getString(0) == "1")
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("USER_QUERY_TXT").first().getString(0) == "292832042631")
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("SRC_PLCMNT_DATA_TXT").first().getString(0) == "711-53200-19255-0")
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("ITEM_ID").first().getDecimal(0).longValue() == 292832042631L)
    //    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("CLICK_TS").first().getString(0) == "2017-03-10 17:13:40.000")
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429249L).select("ROVER_URL_TXT").first().getString(0) == "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5336203178&customid=1&lgeo=1&vectorid=229466&item=292832042631&raptor=1")

    // validate special case
    // filter click from special publisher
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429241L).count() == 0)
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429242L).count() == 0)
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429243L).count() == 0)

    // filter click whose uri && referrer are ebay sites
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429246L).count() == 0)
    assert(clickDf.filter(col("CLICK_ID") === 6453984045429247L).count() == 0)

    // rover guid fixed case
    //    assert(clickDf.filter(col("CLICK_ID") === 2909817128329241L).select("GUID_TXT").first().getString(0) == "abcdefg3412445")
    //    assert(clickDf.filter(col("CLICK_ID") === 2909817128329242L).select("GUID_TXT").first().getString(0) == "abcdefg3412446")
    assert(clickDf.filter(col("CLICK_ID") === 2909817128329243L).select("GUID_TXT").first().getString(0) == "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^")
    assert(clickDf.filter(col("CLICK_ID") === 2909817128329244L).select("GUID_TXT").first().getString(0) == "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^")
    assert(clickDf.filter(col("CLICK_ID") === 2909817128329245L).select("GUID_TXT").first().getString(0) == "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^")

//    // bullseye token validation
//    assert(clickDf.filter(col("CLICK_ID") === 6453984045429251L).select("LAST_VWD_ITEM_ID").first().getString(0) == "250012780462")
//    assert(clickDf.filter(col("CLICK_ID") === 6453984045429251L).select("LAST_VWD_ITEM_TS").first().getString(0) == "2020-06-03 15:29:37.92")
  }

  def createTestDataForEPN(): Unit = {
    val metadata = Metadata(inputWorkDir, "EPN", MetadataEnum.capping)
    val dateFiles1 = DateFiles("date=2018-05-01", Array("file://" + inputDir + "/date=2018-05-01/part-00000.snappy.parquet",
      "file://" + inputDir + "/date=2018-05-01/part-00001.snappy.parquet",
      "file://" + inputDir + "/date=2018-05-01/part-00002.snappy.parquet"))
    val dateFiles2 = DateFiles("date=2018-05-02", Array("file://" + inputDir + "/date=2018-05-02/part-00000.snappy.parquet"))

    val meta: MetaFiles = MetaFiles(Array(dateFiles1,dateFiles2))
    metadata.writeDedupeOutputMeta(meta, Array(".epnnrt_v2"))

    // prepare data file
    val writer1 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-05-01/part-00000.snappy.parquet"))
      .withSchema(FilterMessage.getClassSchema)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val writer2 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-05-01/part-00001.snappy.parquet"))
      .withSchema(FilterMessage.getClassSchema)
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val writer3 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-05-02/part-00000.snappy.parquet"))
      .withSchema(FilterMessage.getClassSchema)
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val writer4 = AvroParquetWriter.
      builder[GenericRecord](new Path(inputDir + "/date=2018-05-01/part-00002.snappy.parquet"))
      .withSchema(FilterMessage.getClassSchema)
      .withConf(hadoopConf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .build()

    val timestamp = getTimestamp("2018-05-01")

    // Desktop
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 6453984045429241L, 5574651234L, 435453655L, "76cbd9ea15b0a93d12831833fff1c1065ad49dd7^", 1489151020000L, writer1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 6453984045429242L, 5574651234L, 435453655L, "12cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489166020000L, writer1)

    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 7453984045429241L, 7000001564L, -1L, "34cbd9iqoiwjddws09ydwa33fff1c1065ad49dd7^", 1489189020000L, writer1)
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 7453984045429242L, 7000000007L, -1L, "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489098020000L,  writer1)
    writer1.close()

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 6453984045429243L, 5574651234L, -1L, "76cbd9ea15b0a93d12831833fff1c1065ad49dd7^", 1489998020000L, writer2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.SERVE, 1109090984045429247L, 7000001262L, -1L, "12cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489156070000L, writer2)

    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 7453984045429243L, 7000001564L, -1L, "34cbd9iqoiwjddws09ydwa33fff1c1065ad49dd7^", 1489998020000L, writer2)
    writeFilterMessage(ChannelType.EPN, ChannelAction.VIEWABLE, 2909817128329247L, 7000001538L, -1L, "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489159020000L,  writer2)
    writer2.close()

    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 6453984045429244L, 7000001262L, -1L, "76cbd9ea15b0a93d12831833fff1c1065ad49dd7^", 1489100020000L, writer3)
    writeFilterMessage(ChannelType.EPN, ChannelAction.CLICK, 6453984045429245L, 7000001531L, -1L, "12cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489999020000L, writer3)

    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 7453984045429244L, 7000001285L, -1L, "34cbd9iqoiwjddws09ydwa33fff1c1065ad49dd7^", timestamp - 8, writer3)
    writeFilterMessage(ChannelType.EPN, ChannelAction.IMPRESSION, 7453984045429245L, 7000001727L, 9000052575L, "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", timestamp - 7,  writer3)
    writer3.close()

    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.CLICK, 6453984045429246L, 7000001262L, 435453655L, "76cbd9ea15b0a93d12831833fff1c1065ad49dd7^", 1489151020000L, "https://www.ebay.com/p/216444975?iid=392337788578&rt=nc&mkevt=1&mkcid=1&mkrid=4080-157294-765411-6&mksid=1234556&item=292832042631&toolid=10044&customid=1&ff3=2&campid=5336203178&lgeo=1&vectorid=229466", "http://www.ebay.de/?mkevt=1&mkcid=1&mkrid=707-53477-19255-0&campid=5338586075&customid=dede-edge-ntp-topsites-affiliates", writer4)
    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.CLICK, 6453984045429247L, 7000001262L, 435453655L, "76cbd9ea15b0a93d12831833fff1c1065ad49dd7^", 1489151020000L, "https://www.ebay.com/p/216444975?iid=392337788578&rt=nc&mkevt=1&mkcid=1&mkrid=4080-157294-765411-6&mksid=1234556&item=292832042631&toolid=10044&customid=1&ff3=2&campid=5336203178&lgeo=1&vectorid=229466", "http://rover.ebay.com/rover/1/1185-53479-19255-0/1?ff3=4&pub=5575118796&toolid=10001&campid=5337725402&customid=&mpre=http://www.ebay.es/itm/Etude-House-Drawing-Eye-Brow-Pencil-/191616582622%3Fpt%3DLH_DefaultDomain_0%26var%3D%26hash%3Ditem2c9d3d03de", writer4)
    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.CLICK, 6453984045429250L, 7000001262L, 435453655L, "76cbd9ea15b0a93d12831833fff1c1065ad49dd7^", 1489151020000L, "https://www.ebay.com/p/216444975?iid=392337788578&rt=nc&mkevt=1&mkcid=1&mkrid=4080-157294-765411-6&mksid=1234556&item=292832042631&toolid=10044&customid=1&ff3=2&campid=5336203178&lgeo=1&vectorid=229466", "http://www.google.com", writer4)
    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.CLICK, 6453984045429248L, 7000001262L, 435453655L, "12cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489166020000L, "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5336203178&customid=1&lgeo=1&vectorid=229466&item=292832042631&raptor=1", "http://www.ebay.de/?mkevt=1&mkcid=1&mkrid=707-53477-19255-0&campid=5338586075&customid=dede-edge-ntp-topsites-affiliates", writer4)
    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.CLICK, 6453984045429249L, 7000001262L, 435453655L, "12cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489166020000L, "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5336203178&customid=1&lgeo=1&vectorid=229466&item=292832042631&raptor=1", "http://rover.ebay.com/rover/1/1185-53479-19255-0/1?ff3=4&pub=5575118796&toolid=10001&campid=5337725402&customid=&mpre=http://www.ebay.es/itm/Etude-House-Drawing-Eye-Brow-Pencil-/191616582622%3Fpt%3DLH_DefaultDomain_0%26var%3D%26hash%3Ditem2c9d3d03de", writer4)
    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.CLICK, 6453984045429251L, 7000001262L, 435453655L, "6018ed8b1720a9cc5e628468f7d256a5", 1591169377921L, "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5336203178&customid=1&lgeo=1&vectorid=229466&item=292832042631&raptor=1", "http://rover.ebay.com/rover/1/1185-53479-19255-0/1?ff3=4&pub=5575118796&toolid=10001&campid=5337725402&customid=&mpre=http://www.ebay.es/itm/Etude-House-Drawing-Eye-Brow-Pencil-/191616582622%3Fpt%3DLH_DefaultDomain_0%26var%3D%26hash%3Ditem2c9d3d03de", writer4)


    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.IMPRESSION, 7453984045429246L, 7000001564L, -1L, "34cbd9iqoiwjddws09ydwa33fff1c1065ad49dd7^", 1489189020000L, "https://www.ebayadservices.com/p/216444975?iid=392337788578&rt=nc&mkevt=1&mkcid=1&mkrid=4080-157294-765411-6&mksid=1234556&item=292832042631&toolid=10044&customid=1", "http://www.ebay.de/?mkevt=1&mkcid=1&mkrid=707-53477-19255-0&campid=5338586075&customid=dede-edge-ntp-topsites-affiliates", writer4)
    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.IMPRESSION, 7453984045429247L, 7000001564L, -1L, "34cbd9iqoiwjddws09ydwa33fff1c1065ad49dd7^", 1489189020000L, "https://www.ebayadservices.com/p/216444975?iid=392337788578&rt=nc&mkevt=1&mkcid=1&mkrid=4080-157294-765411-6&mksid=1234556&item=292832042631&toolid=10044&customid=1", "http://rover.ebay.com/rover/1/1185-53479-19255-0/1?ff3=4&pub=5575118796&toolid=10001&campid=5337725402&customid=&mpre=http://www.ebay.es/itm/Etude-House-Drawing-Eye-Brow-Pencil-/191616582622%3Fpt%3DLH_DefaultDomain_0%26var%3D%26hash%3Ditem2c9d3d03de", writer4)
    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.IMPRESSION, 7453984045429248L, 7000000007L, -1L, "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489098020000L,  "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5336203178&customid=1&lgeo=1&vectorid=229466&item=292832042631&raptor=1", "http://www.ebay.de/?mkevt=1&mkcid=1&mkrid=707-53477-19255-0&campid=5338586075&customid=dede-edge-ntp-topsites-affiliates", writer4)
    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.IMPRESSION, 7453984045429249L, 7000000007L, -1L, "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489098020000L,  "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5336203178&customid=1&lgeo=1&vectorid=229466&item=292832042631&raptor=1", "http://rover.ebay.com/rover/1/1185-53479-19255-0/1?ff3=4&pub=5575118796&toolid=10001&campid=5337725402&customid=&mpre=http://www.ebay.es/itm/Etude-House-Drawing-Eye-Brow-Pencil-/191616582622%3Fpt%3DLH_DefaultDomain_0%26var%3D%26hash%3Ditem2c9d3d03de", writer4)

    // replace guid for click
    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.CLICK, 2909817128329241L, 7000000007L, -1L, "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489098020000L,  "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5336203178&customid=1&lgeo=1&vectorid=229466&item=292832042631&raptor=1&dashenId=3412445", "http://rover.ebay.com/rover/1/1185-53479-19255-0/1?ff3=4&pub=5575118796&toolid=10001&campid=5337725402&customid=&mpre=http://www.ebay.es/itm/Etude-House-Drawing-Eye-Brow-Pencil-/191616582622%3Fpt%3DLH_DefaultDomain_0%26var%3D%26hash%3Ditem2c9d3d03de", writer4)
    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.CLICK, 2909817128329242L, 7000000007L, -1L, "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489098020000L,  "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5336203178&customid=1&lgeo=1&vectorid=229466&item=292832042631&raptor=1&dashenId=3412446", "http://rover.ebay.com/rover/1/1185-53479-19255-0/1?ff3=4&pub=5575118796&toolid=10001&campid=5337725402&customid=&mpre=http://www.ebay.es/itm/Etude-House-Drawing-Eye-Brow-Pencil-/191616582622%3Fpt%3DLH_DefaultDomain_0%26var%3D%26hash%3Ditem2c9d3d03de", writer4)

    // no chocotag guid mapping in cb for dashenId 3412441, keep original guid
    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.CLICK, 2909817128329243L, 7000000007L, -1L, "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489098020000L,  "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5336203178&customid=1&lgeo=1&vectorid=229466&item=292832042631&raptor=1&dashenId=3412441", "http://rover.ebay.com/rover/1/1185-53479-19255-0/1?ff3=4&pub=5575118796&toolid=10001&campid=5337725402&customid=&mpre=http://www.ebay.es/itm/Etude-House-Drawing-Eye-Brow-Pencil-/191616582622%3Fpt%3DLH_DefaultDomain_0%26var%3D%26hash%3Ditem2c9d3d03de", writer4)

    // no dashenId, keep original guid
    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.CLICK, 2909817128329244L, 7000000007L, -1L, "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489098020000L,  "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5336203178&customid=1&lgeo=1&vectorid=229466&item=292832042631&raptor=1", "http://rover.ebay.com/rover/1/1185-53479-19255-0/1?ff3=4&pub=5575118796&toolid=10001&campid=5337725402&customid=&mpre=http://www.ebay.es/itm/Etude-House-Drawing-Eye-Brow-Pencil-/191616582622%3Fpt%3DLH_DefaultDomain_0%26var%3D%26hash%3Ditem2c9d3d03de", writer4)
    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.CLICK, 2909817128329245L, 7000000007L, -1L, "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489098020000L,  "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5336203178&customid=1&lgeo=1&vectorid=229466&item=292832042631&raptor=1", "http://rover.ebay.com/rover/1/1185-53479-19255-0/1?ff3=4&pub=5575118796&toolid=10001&campid=5337725402&customid=&mpre=http://www.ebay.es/itm/Etude-House-Drawing-Eye-Brow-Pencil-/191616582622%3Fpt%3DLH_DefaultDomain_0%26var%3D%26hash%3Ditem2c9d3d03de", writer4)

    // keep original guid for impression
    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.IMPRESSION, 7453984045429250L, 7000000007L, -1L, "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489098020000L,  "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5336203178&customid=1&lgeo=1&vectorid=229466&item=292832042631&raptor=1&dashenId=3412448", "http://rover.ebay.com/rover/1/1185-53479-19255-0/1?ff3=4&pub=5575118796&toolid=10001&campid=5337725402&customid=&mpre=http://www.ebay.es/itm/Etude-House-Drawing-Eye-Brow-Pencil-/191616582622%3Fpt%3DLH_DefaultDomain_0%26var%3D%26hash%3Ditem2c9d3d03de", writer4)
    writeFilterMessageWithSpecificUri(ChannelType.EPN, ChannelAction.IMPRESSION, 7453984045429251L, 7000000007L, -1L, "56cbd9iqoiwjddwswdwdwa33fff1c1065ad49dd7^", 1489098020000L,  "http://rover.ebay.com/rover/1/711-53200-19255-0/1?ff3=2&toolid=10044&campid=5336203178&customid=1&lgeo=1&vectorid=229466&item=292832042631&raptor=1&dashenId=3412449", "http://rover.ebay.com/rover/1/1185-53479-19255-0/1?ff3=4&pub=5575118796&toolid=10001&campid=5337725402&customid=&mpre=http://www.ebay.es/itm/Etude-House-Drawing-Eye-Brow-Pencil-/191616582622%3Fpt%3DLH_DefaultDomain_0%26var%3D%26hash%3Ditem2c9d3d03de", writer4)
    writer4.close()




    //create ams_landing_page_type_lookup.csv file
    import java.io.PrintWriter
    var printWriter = new PrintWriter(resourceDir + "/" + "ams_rfrng_dmn_pblshr_map.csv")
    printWriter.println("471\ttranslate.google.com.mx\t5574665384\t1\t2/18/13\tYAJI_DBA")
    printWriter.println("248\tmaps.google.de\t5574737746\t1\t2/18/13\tYAJI_DBA ")
    printWriter.println("6038\twww.google.si\t5574643520\t1\t3/26/14\tSHIDLEKAR_DBA ")
    printWriter.println("3691\twww.google.com.ng\t5574630834\t1\t3/26/14\tSHIDLEKAR_DBA")
    printWriter.println("2957\twww.google.kz\t5574674899\t1\t2/18/13\tYAJI_DBA")
    printWriter.println("5243\twww.google.jo\t5575042380\t1\t3/26/14\tSHIDLEKAR_DBA")
    printWriter.println("1594\twww.google.com.bh\t5574636783\t1\t2/18/13\tYAJI_DBA")
    printWriter.close()

    printWriter = new PrintWriter(resourceDir + "/" + "ams_landing_page_type_lookup.csv")
    printWriter.println("Search Results\t539000\t2\t2\thttp://local-services.shop.ebay.ie/items/\t3099180\t1\t2/25/10\tSKHADER")
    printWriter.println("Search Results\t658500\t2\t15\thttp://motors.shop.ebay.co.uk/\t3003200\t1\t2/25/10\tSKHADER")
    printWriter.println("Item Page\t23500\t4\t4\thttp://cgi.ebay.com.au/\t4015015\t1\t2/25/10\tSKHADER")
    printWriter.println("Seller/Store Results\t794500\t3\t12\thttp://cgi6.ebay.it/ws/eBayISAPI.dll?ViewStoreV4&name=\t2101020\t1\t2/25/10\tSKHADER")
    printWriter.close()
  }


  def getTimestamp(date: String): Long = {
    sdf.parse(date).getTime
  }

  def writeFilterMessage(channelType: ChannelType,
                         channelAction: ChannelAction,
                         snapshotId: Long,
                         publisherId: Long,
                         campaignId: Long,
                         cguid: String,
                         timestamp: Long,
                         writer: ParquetWriter[GenericRecord]): FilterMessage = {
    val message = TestHelper.newFilterMessage(channelType,
      channelAction,
      snapshotId,
      publisherId,
      campaignId,
      cguid,
      timestamp)
    writer.write(message)
    message
  }

  def writeFilterMessageWithSpecificUri(channelType: ChannelType,
                                        channelAction: ChannelAction,
                                        snapshotId: Long,
                                        publisherId: Long,
                                        campaignId: Long,
                                        cguid: String,
                                        timestamp: Long,
                                        uri: String,
                                        referer: String,
                                        writer: ParquetWriter[GenericRecord]): FilterMessage = {
    val message = TestHelper.newFilterMessage(channelType,
      channelAction,
      snapshotId,
      publisherId,
      campaignId,
      cguid,
      timestamp)
    message.setUri(uri)
    message.setReferer(referer)
    message.setGuid(cguid)
    writer.write(message)
    message
  }
}

