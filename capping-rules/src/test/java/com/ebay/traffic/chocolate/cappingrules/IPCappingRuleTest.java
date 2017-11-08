package com.ebay.traffic.chocolate.cappingrules;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

public class IPCappingRuleTest {

    String TRANSACTION_TABLE_NAME = "prod_transactional";
    String TRANSACTION_CF_DEFAULT = "x";

    private SparkContext sparkContext;
    private JavaSparkContext javaSparkContext;
    private SparkSession sparkSession;
    private SQLContext sqlContext;
    private HBaseTestingUtility hbaseUtility;
    private Configuration configuration;
    private Connection connection;
    private HTable transactionalTable;
    private HBaseAdmin hbaseAdmin;
    private ZonedDateTime now = ZonedDateTime.now();

    @Before
    public void setUp() throws Exception {

        hbaseUtility = new HBaseTestingUtility();
        hbaseUtility.startMiniCluster();

        configuration = hbaseUtility.getConfiguration();
        connection = hbaseUtility.getConnection();
        hbaseAdmin = hbaseUtility.getHBaseAdmin();
        transactionalTable = new HTable(TableName.valueOf(TRANSACTION_TABLE_NAME), connection);
        sparkSession = SparkSession.builder().master("local[4]").appName("test")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir"))
                .getOrCreate();
        sparkContext = sparkSession.sparkContext();
        sqlContext = sparkSession.sqlContext();
        javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
        initHBaseTransactionTable();
        IPCappingRule.init(javaSparkContext, configuration, sqlContext);
    }

    @After
    public void tearDown() throws Exception {
        sparkSession.stop();
        hbaseUtility.shutdownMiniCluster();
    }

    @Test
    public void testReadEvents() throws Exception {
        long threashHold = 3l;
        Dataset<Row> schemaRDD = IPCappingRule.readEvents(now.toInstant().toEpochMilli(),now.plusDays(1).toInstant().toEpochMilli());
        assertEquals(schemaRDD.count(), 6);
    }

    @Test
    public void testFilterEvents() throws Exception {
        long threashHold = 3l;
        Dataset<Row> invalidsRDD = IPCappingRule.filterEvents(now.toInstant().toEpochMilli(),now.plusDays(1).toInstant().toEpochMilli(), threashHold);
        assertEquals(invalidsRDD.count(), 4);
        IPCappingRule.writeInvalidEvents(invalidsRDD);
        Dataset<Row> resultRDD = IPCappingRule.readEvents(now.toInstant().toEpochMilli(),now.plusDays(1).toInstant().toEpochMilli());
        Dataset<Row> invalidsRDDAfterWritten = resultRDD.filter(resultRDD.col("valid").equalTo(false));
        assertEquals(invalidsRDDAfterWritten.count(), 4);
    }

    private void initHBaseTransactionTable() throws IOException {
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(TRANSACTION_TABLE_NAME));
        tableDesc.addFamily(new HColumnDescriptor(TRANSACTION_CF_DEFAULT)
                .setCompressionType(Compression.Algorithm.NONE));
        hbaseAdmin.createTable(tableDesc);

        String requestHeaderValid = "Cookie: aaa ;|X-eBay-Client-IP: 50.206.232.22|Connection: keep-alive|User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36";
        String requestHeaderInvalid = "Cookie: aaa ;|X-eBay-Client-IP: 11.11.11.11|Connection: keep-alive|User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36";

        addEvent(transactionalTable, new Event((now.toInstant().toEpochMilli() & ~IPCappingRule.TIME_MASK) <<24l, now.toInstant().toEpochMilli(), 0, 1, 100, requestHeaderValid,true, true));
        addEvent(transactionalTable, new Event((now.plusMinutes(1).toInstant().toEpochMilli() & ~IPCappingRule.TIME_MASK) <<24l, now.toInstant().toEpochMilli(), 0, 2, 101, requestHeaderValid,true, true));
        addEvent(transactionalTable, new Event((now.plusMinutes(2).toInstant().toEpochMilli() & ~IPCappingRule.TIME_MASK) <<24l, now.toInstant().toEpochMilli(), 0, 3, 101, requestHeaderValid,true, true));
        addEvent(transactionalTable, new Event((now.plusHours(1).toInstant().toEpochMilli() & ~IPCappingRule.TIME_MASK) <<24l, now.toInstant().toEpochMilli(), 0, 4, 101, requestHeaderInvalid,true, true));
        addEvent(transactionalTable, new Event((now.plusHours(2).toInstant().toEpochMilli() & ~IPCappingRule.TIME_MASK) <<24l, now.toInstant().toEpochMilli(), 0, 5, 101, requestHeaderInvalid,true, true));
        addEvent(transactionalTable, new Event((now.plusHours(3).toInstant().toEpochMilli() & ~IPCappingRule.TIME_MASK) <<24l, now.toInstant().toEpochMilli(), 0, 6, 102, requestHeaderInvalid,true, true));
        addEvent(transactionalTable, new Event((now.plusHours(23).toInstant().toEpochMilli() & ~IPCappingRule.TIME_MASK) <<24l, now.toInstant().toEpochMilli(), 0, 7, 103, requestHeaderInvalid,true, true));
        addEvent(transactionalTable, new Event(((now.plusHours(25).toInstant().toEpochMilli()) & ~IPCappingRule.TIME_MASK) <<24l, now.toInstant().toEpochMilli(), 0, 8, 104, requestHeaderInvalid,true, true));
    }

    private <T> void putCell(Put put, String family, String qualifier, T value)  {

        byte[] bytes;
        if(value instanceof Long) {
            bytes = Bytes.toBytes(((Long) value).longValue());
        }
        else if(value instanceof  String) {
            bytes = Bytes.toBytes((String)value);
        }
        else if(value instanceof Boolean) {
            bytes = Bytes.toBytes(((Boolean) value).booleanValue());
        }
        else if(value instanceof Timestamp) {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            String str = ((Timestamp) value).toString();
            int bufferSize = str.getBytes().length;
            System.arraycopy(str.getBytes(), 0, buffer.array(), 0, bufferSize);
            bytes = buffer.array();
        }
        else{
            bytes = Bytes.toBytes(value.toString());
        }

        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), bytes);
    }

    private void addEvent(HTable table, Event event) throws IOException {
        Put put = new Put(Bytes.toBytes(event.getSnapshotId()));
        putCell(put, TRANSACTION_CF_DEFAULT, "request_timestamp", event.getTimestamp());
        putCell(put, TRANSACTION_CF_DEFAULT, "publisher_id", event.getPublisherId());
        putCell(put, TRANSACTION_CF_DEFAULT, "campaign_id", event.getCampaignId());
        putCell(put, TRANSACTION_CF_DEFAULT, "snid", event.getSnid());
        putCell(put, TRANSACTION_CF_DEFAULT, "request_headers", event.getRequestHeaders());
        putCell(put, TRANSACTION_CF_DEFAULT, "is_tracked", event.getTracked());
        putCell(put, TRANSACTION_CF_DEFAULT, "is_valid", event.getValid());
        table.put(put);
    }

}