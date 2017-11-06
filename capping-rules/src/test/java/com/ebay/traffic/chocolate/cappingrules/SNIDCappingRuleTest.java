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

public class SNIDCappingRuleTest {

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
    private long current = System.currentTimeMillis();

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
        SNIDCappingRule.init(javaSparkContext, configuration, sqlContext);

    }

    @After
    public void tearDown() throws Exception {
        sparkSession.stop();
        hbaseUtility.shutdownMiniCluster();
    }



    @Test
    public void readEvents() throws Exception {
        Dataset<Row> invalidsRDD = SNIDCappingRule.filterEvents(current,current+10);
        SNIDCappingRule.writeInvalidEvents(invalidsRDD);
        SNIDCappingRule.readEvents(current,current+10);

    }

    private void initHBaseTransactionTable() throws IOException {
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(TRANSACTION_TABLE_NAME));
        tableDesc.addFamily(new HColumnDescriptor(TRANSACTION_CF_DEFAULT)
                .setCompressionType(Compression.Algorithm.NONE));
        hbaseAdmin.createTable(tableDesc);


        addEvent(transactionalTable, new Event(1001, current, 0, 2, 100, true, true));
        addEvent(transactionalTable, new Event(1002, current + 10, 0, 3, 101, true, true));
        addEvent(transactionalTable, new Event(1003, current + 10, 0, 3, 101, true, true));
        addEvent(transactionalTable, new Event(1004, current + 10, 0, 3, 101, true, true));
        addEvent(transactionalTable, new Event(1005, current + 10, 0, 3, 101, true, true));
        addEvent(transactionalTable, new Event(1006, current + 10, 0, 4, 102, true, true));
        addEvent(transactionalTable, new Event(1007, current + 10, 0, 5, 103, true, true));
        addEvent(transactionalTable, new Event(1008, current + 100, 0, 6, 104, true, true));
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
        putCell(put, TRANSACTION_CF_DEFAULT, "is_tracked", event.getTracked());
        putCell(put, TRANSACTION_CF_DEFAULT, "is_valid", event.getValid());
        table.put(put);
    }

}