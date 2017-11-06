package com.ebay.traffic.chocolate.cappingrules;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
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
import java.sql.Timestamp;

//import org.apache.hadoop.hbase.client.Row;
public class SNIDCappingRule {
    private static SNIDCappingRule snidCappingRule = new SNIDCappingRule();
    protected JavaSparkContext javaSparkContext;
    protected Configuration config;
    protected SQLContext sqlContext;

    private SNIDCappingRule() {
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
        snidCappingRule.javaSparkContext = jsc;
        snidCappingRule.config = config;
        snidCappingRule.sqlContext = sqlContext;
    }

    public static byte[] timestampToBytes(Timestamp timestamp) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        String str = ((Timestamp) timestamp).toString();
        int bufferSize = str.getBytes().length;
        System.arraycopy(str.getBytes(), 0, buffer.array(), 0, bufferSize);
        return buffer.array();
    }

    public static Dataset<Row> readEvents(Long timestampStart, Long timestampStop) {
        try {
            HBaseAdmin.checkHBaseAvailable(snidCappingRule.config);
            System.out.println("HBase is running!");
        } catch (MasterNotRunningException e) {
            System.out.println("HBase is not running!");
            System.exit(1);
        } catch (Exception ce) {
            ce.printStackTrace();
        }

        snidCappingRule.config.set(TableInputFormat.INPUT_TABLE, "prod_transactional");
//        snidCappingRule.config.set(TableInputFormat.SCAN_COLUMN_FAMILY, "x");
//        snidCappingRule.config.set(TableInputFormat.SCAN_COLUMNS, "x:campaign_id x:channel_type x:request_timestamp");

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        byte[] timestampStartBytes;
        buffer.putLong(timestampStart);
        timestampStartBytes = buffer.array();
        byte[] timestampStopBytes;
        buffer.clear();
        buffer.putLong(timestampStop);
        timestampStopBytes = buffer.array();


        SingleColumnValueFilter requestTimestampGreaterFilter=new SingleColumnValueFilter("x".getBytes(), "request_timestamp".getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(timestampStartBytes));
        SingleColumnValueFilter requestTimestampLesserFilter=new SingleColumnValueFilter("x".getBytes(), "request_timestamp".getBytes(), CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(timestampStopBytes));
        FilterList requestTimestampFilterList= new FilterList(FilterList.Operator.MUST_PASS_ALL);
        requestTimestampFilterList.addFilter(requestTimestampGreaterFilter);
        requestTimestampFilterList.addFilter(requestTimestampLesserFilter);

        Scan scan = new Scan();
        scan.setFilter(requestTimestampFilterList);

        try {
            snidCappingRule.config.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                snidCappingRule.javaSparkContext.newAPIHadoopRDD(snidCappingRule.config, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

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
                        event.setValid((Boolean) Bytes.toBoolean(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("is_valid"))));
                        event.setSnid((Long)Bytes.toLong(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("snid"))));
                        return new Tuple2<Long, Event>(keyRow, event);
                    }
                });
        Dataset<Row> schemaRDD = snidCappingRule.sqlContext.createDataFrame(rowPairRDD.values(), Event.class);
        System.out.println("Original events:");
        schemaRDD.show();
        return schemaRDD;

    }


    public static Dataset<Row> filterEvents(Long timestampStart, Long timestampStop) {

        Dataset<Row> schemaRDD = readEvents(timestampStart, timestampStop);
        WindowSpec window = Window.partitionBy(schemaRDD.col("snid")).orderBy(schemaRDD.col("snid"));

        Dataset<Row> df = schemaRDD.withColumn("count", functions.count("snid").over(window));
        Dataset<Row> res = df.withColumn("isValid", functions.when(df.col("count").$greater(3), false).otherwise(true));
        Dataset<Row> invalids = res.filter(res.col("isValid").equalTo(false));

        System.out.println("Filtered result:");
        invalids.show();
        //invalids.printSchema();
        return invalids;
    }

    public static void writeInvalidEvents(Dataset<Row> invalids) {

        Job newAPIJobConfiguration = null;
        try {
            newAPIJobConfiguration = Job.getInstance(snidCappingRule.config);
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
        //FIXME: mirar como quitar la carga de un texto arbitrario para crear un JavaRDD
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

        SparkConf sparkConf = new SparkConf().setAppName("SparkHBaseTest").setMaster("yarn-cluster");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);

        // create connection with HBase
        Configuration config = null;
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "lvschocolatemaster-1448894.stratus.lvs.ebay.com,lvschocolatemaster-1448895.stratus.lvs.ebay.com,lvschocolatemaster-1448897.stratus.lvs.ebay.com,lvschocolatemaster-1582061.stratus.lvs.ebay.com,lvschocolatemaster-1582062.stratus.lvs.ebay.com");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.master", "lvschocolatemaster-1448895.stratus.lvs.ebay.com:60000");
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
    }
}
