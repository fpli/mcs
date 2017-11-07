package com.ebay.traffic.chocolate.cappingrules;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import scala.Tuple2;

import java.io.IOException;
import java.nio.ByteBuffer;

public class IPCappingRule {
    private static IPCappingRule IPCappingRule = new IPCappingRule();
    protected JavaSparkContext javaSparkContext;
    protected Configuration config;
    protected SQLContext sqlContext;
    public static final long TIME_MASK = 0xFFFFFFl << 40l;

    private IPCappingRule() {
        javaSparkContext = null;
        config = null;
    }

    /**
     *
     * @param jsc
     * @param config
     * @param sqlContext
     */
    public static void init( JavaSparkContext jsc, Configuration config, SQLContext sqlContext) {
        IPCappingRule.javaSparkContext = jsc;
        IPCappingRule.config = config;
        IPCappingRule.sqlContext = sqlContext;
    }

    public static Dataset<Row> readEvents(long timestampStart, long timestampStop) {
        try {
            HBaseAdmin.checkHBaseAvailable(IPCappingRule.config);
            System.out.println("HBase is running!");
        } catch (MasterNotRunningException e) {
            System.out.println("HBase is not running!");
            System.exit(1);
        } catch (Exception ce) {
            ce.printStackTrace();
        }

        IPCappingRule.config.set(TableInputFormat.INPUT_TABLE, "prod_transactional");

        ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE);
        byte[] timestampStartBytes;
        buffer.putLong((timestampStart & ~TIME_MASK) << 24l);
        timestampStartBytes = buffer.array();
        byte[] timestampStopBytes;
        buffer = ByteBuffer.allocate(Long.SIZE);
        buffer.putLong((timestampStop & ~TIME_MASK) << 24l);
        timestampStopBytes = buffer.array();

        Scan scan = new Scan();
        scan.setStartRow(timestampStartBytes);
        scan.setStopRow(timestampStopBytes);

        try {
            IPCappingRule.config.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                IPCappingRule.javaSparkContext.newAPIHadoopRDD(IPCappingRule.config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<Long, Event> rowPairRDD = hBaseRDD.mapToPair(
                new PairFunction<Tuple2<ImmutableBytesWritable, Result>, Long, Event>() {
                    @Override
                    public Tuple2<Long, Event> call(
                            Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {

                        Result r = entry._2;
                        long keyRow = Bytes.toLong(r.getRow());
                        System.out.println("keyRow: " + keyRow);

                        Event event = new Event();
                        event.setSnapshotId(keyRow);
                        event.setTimestamp((Long) Bytes.toLong(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("request_timestamp"))));
                        event.setCampaignId((Long) Bytes.toLong(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("campaign_id"))));
                        event.setRequestHeaders((String) Bytes.toString(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("request_headers"))));
                        event.setValid((Boolean) Bytes.toBoolean(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("is_valid"))));
                        event.setSnid((Long)Bytes.toLong(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("snid"))));
                        return new Tuple2<Long, Event>(keyRow, event);
                    }
                });
        Dataset<Row> schemaRDD = IPCappingRule.sqlContext.createDataFrame(rowPairRDD.values(), Event.class);
        System.out.println("Original events:");
        schemaRDD.show();
        return schemaRDD;
    }


    public static Dataset<Row> filterEvents(long timestampStart, long timestampStop, long threshhold) {

        Dataset<Row> schemaRDD = readEvents(timestampStart, timestampStop);
        Dataset<Row> schemaRDDWithIP = schemaRDD.withColumn("headerWithIP", functions.split(schemaRDD.col("requestHeaders"), "X-eBay-Client-IP:").getItem(1));
        Dataset<Row> schemaRDDWithIPOnly = schemaRDDWithIP.withColumn("headerWithIPOnly", functions.split(schemaRDDWithIP.col("headerWithIP"), "\\|").getItem(0));
        Dataset<Row> schemaRDDWithIPTrim =  schemaRDDWithIPOnly.withColumn("ip", functions.trim(schemaRDDWithIPOnly.col("headerWithIPOnly")))
                .drop("headerWithIP").drop("headerWithIPOnly");

        WindowSpec window = Window.partitionBy(schemaRDDWithIPTrim.col("ip")).orderBy(schemaRDDWithIPTrim.col("ip"));
        Dataset<Row> df = schemaRDDWithIPTrim.withColumn("count", functions.count("*").over(window));
        Dataset<Row> res = df.withColumn("isValid", functions.when(df.col("count").$greater(threshhold), false).otherwise(true));
        Dataset<Row> invalids = res.filter(res.col("isValid").equalTo(false));

        System.out.println("Filtered result:");
        invalids.show();
        return invalids;
    }

    public static void writeInvalidEvents(Dataset<Row> invalids) {

        Job newAPIJobConfiguration = null;
        try {
            newAPIJobConfiguration = Job.getInstance(IPCappingRule.config);
        } catch (IOException e) {
            e.printStackTrace();
        }
        newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, "prod_transactional");
        newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

        JavaRDD<Event> records = invalids.toJavaRDD().map(new Function<Row, Event>() {
            @Override
            public Event call(Row row) throws Exception {
                Event event = new Event(row.getLong(row.fieldIndex("snapshotId")), row.getBoolean(row.fieldIndex("isValid")));
                return event;
            }
        });

        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = records.mapToPair(new PairFunction<Event, ImmutableBytesWritable, Put>() {
            @Override
            public Tuple2<ImmutableBytesWritable, Put> call(Event event)
                    throws Exception {
                Put put = new Put(Bytes.toBytes(event.getSnapshotId()));
                put.add(Bytes.toBytes("x"),
                        Bytes.toBytes("is_valid"),
                        Bytes.toBytes(event.getValid()));

                return new Tuple2<ImmutableBytesWritable, Put>(
                        new ImmutableBytesWritable(), put);
            }
        });
        hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());
    }


    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "lvschocolatemaster-1448894.stratus.lvs.ebay.com,lvschocolatemaster-1448895.stratus.lvs.ebay.com,lvschocolatemaster-1448897.stratus.lvs.ebay.com,lvschocolatemaster-1582061.stratus.lvs.ebay.com,lvschocolatemaster-1582062.stratus.lvs.ebay.com");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", "lvschocolatemaster-1448895.stratus.lvs.ebay.com:60000");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");

        SparkSession sparkSession = SparkSession.builder().master("yarn-cluster").appName("cappingrule")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.sql.warehouse.dir", System.getProperty("java.io.tmpdir"))
                .getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        SQLContext sqlContext = sparkSession.sqlContext();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkContext);
        IPCappingRule.init(javaSparkContext, config, sqlContext);
        IPCappingRule.filterEvents(Long.valueOf(args[0]), Long.valueOf(args[1]), Long.valueOf(args[2]));
    }
}
