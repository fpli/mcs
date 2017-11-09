package com.ebay.traffic.chocolate.cappingrules.ip;

import com.ebay.traffic.chocolate.BaseSparkJob;
import com.ebay.traffic.chocolate.cappingrules.Event;
import org.apache.commons.cli.*;
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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.codehaus.janino.Java;
import org.slf4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The IPCappingRule is to check whether there are too many clicks
 * from specific IP address during a certain period.
 */
public class IPCappingRuleJob extends BaseSparkJob {
    private final Logger logger;
    private final String table;
    private final long time;
    private final long timeRange;
    private final long threshold;
    private Configuration hbaseConf;
    private JavaSparkContext javaSparkContext;
    private SQLContext sqlContext;
    private static final long TIME_MASK = 0xFFFFFFl << 40l;

    // Only for test
    private long numberOfRow;

    /**
     * Conctructor of the rule
     * @param jobName spark job name
     * @param mode spark sumbit mode
     * @param table hbase table we query from
     * @param time scan stop time
     * @param timeRange time range from start time to end time
     * @param threshold ip occurance threshold
     */
    public IPCappingRuleJob(String jobName, String mode, String table, long time, long timeRange, long threshold) {
        super(jobName, mode, false);
        this.table = table;
        this.time = time;
        this.timeRange = timeRange;
        this.threshold = threshold;
        this.logger = logger();
    }

    /**
     * PairFunction used to map hbase to event poj
     */
    static PairFunction<Tuple2<ImmutableBytesWritable, Result>, Long, Event> readHBaseMapFunc = new PairFunction<Tuple2<ImmutableBytesWritable, Result>, Long, Event>() {
        @Override
        public Tuple2<Long, Event> call(
                Tuple2<ImmutableBytesWritable, Result> entry) throws Exception {

            Result r = entry._2;
            long keyRow = Bytes.toLong(r.getRow());

            Event event = new Event();
            event.setSnapshotId(keyRow);
            event.setRequestHeaders((String) Bytes.toString(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("request_headers"))));
            event.setFilterFailedRule((String) Bytes.toString(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("filter_failed_rule"))));
            event.setFilterPassed((Boolean) Bytes.toBoolean(r.getValue(Bytes.toBytes("x"), Bytes.toBytes("filter_passed"))));
            return new Tuple2<Long, Event>(keyRow, event);
        }
    };

    /**
     * map function used to read from dataframe to javardd
     */
    static Function<Row, Event> mapFunc = new Function<Row, Event>() {
        @Override
        public Event call(Row row) throws Exception {
            Event event = new Event(row.getLong(row.fieldIndex("snapshotId")), "IPCappingRule", row.getBoolean(row.fieldIndex("isValid")));
            return event;
        }
    };

    /**
     * pairFunction used to map poj to hbase for writing
     */
    static PairFunction<Event, ImmutableBytesWritable, Put> writeHBaseMapFunc = new PairFunction<Event, ImmutableBytesWritable, Put>() {
        @Override
        public Tuple2<ImmutableBytesWritable, Put> call(Event event)
                throws Exception {
            Put put = new Put(Bytes.toBytes(event.getSnapshotId()));
            put.add(Bytes.toBytes("x"),
                    Bytes.toBytes("filter_passed"),
                    Bytes.toBytes(event.isFilterPassed()));
            put.add(Bytes.toBytes("x"),
                    Bytes.toBytes("filter_failed_rule"),
                    Bytes.toBytes(event.getFilterFailedRule()));

            return new Tuple2<ImmutableBytesWritable, Put>(
                    new ImmutableBytesWritable(), put);
        }
    };

    /**
     * Read events into dataframe
     * @param table hbase table
     * @return dataframe containing the data in the scan range
     */
    public Dataset<Row> readEvents(String table) {
        try {
            HBaseAdmin.checkHBaseAvailable(hbaseConf);
            logger.info("HBase is running!");
        } catch (MasterNotRunningException e) {
            logger.error("HBase is not running!");
            logger.error(e.getMessage());
        } catch (Exception ce) {
            logger.error("Unexpected exception when check HBase!");
            logger.error(ce.getMessage());
        }

        hbaseConf.set(TableInputFormat.INPUT_TABLE, table);

        long timestampStart = time - timeRange;
        long timestampStop = time;

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
            hbaseConf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                javaSparkContext.newAPIHadoopRDD(hbaseConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<Long, Event> rowPairRDD = hBaseRDD.mapToPair(readHBaseMapFunc);
        Dataset<Row> schemaRDD = sqlContext.createDataFrame(rowPairRDD.values(), Event.class);
        return schemaRDD;
    }

    /**
     * Filter ipcapping events
     * @param table hbase table
     * @return invalids events which don't pass the capping rule
     */
    public Dataset<Row> filterEvents(String table) {

        Dataset<Row> schemaRDD = readEvents(table);
        Dataset<Row> schemaRDDWithIP = schemaRDD.withColumn("headerWithIP", functions.split(schemaRDD.col("requestHeaders"), "X-eBay-Client-IP:").getItem(1));
        Dataset<Row> schemaRDDWithIPOnly = schemaRDDWithIP.withColumn("headerWithIPOnly", functions.split(schemaRDDWithIP.col("headerWithIP"), "\\|").getItem(0));
        Dataset<Row> schemaRDDWithIPTrim = schemaRDDWithIPOnly.withColumn("ip", functions.trim(schemaRDDWithIPOnly.col("headerWithIPOnly")))
                .drop("headerWithIP").drop("headerWithIPOnly");

        WindowSpec window = Window.partitionBy(schemaRDDWithIPTrim.col("ip")).orderBy(schemaRDDWithIPTrim.col("ip"));
        Dataset<Row> df = schemaRDDWithIPTrim.withColumn("count", functions.count("*").over(window));
        Dataset<Row> res = df.withColumn("isValid", functions.when(df.col("count").$greater(threshold), false).otherwise(true));
        Dataset<Row> invalids = res.filter(res.col("isValid").equalTo(false));
        return invalids;
    }

    /**
     * write invalids events back to hbase
     * @param invalids invalids events
     * @param table hbase table writing to
     */
    public void writeInvalidEvents(Dataset<Row> invalids, String table) {

        Job newAPIJobConfiguration = null;
        try {
            newAPIJobConfiguration = Job.getInstance(hbaseConf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        newAPIJobConfiguration.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
        newAPIJobConfiguration.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);

        JavaRDD<Event> records = invalids.toJavaRDD().map(mapFunc);

        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = records.mapToPair(writeHBaseMapFunc);
        hbasePuts.saveAsNewAPIHadoopDataset(newAPIJobConfiguration.getConfiguration());
    }

    /**
     * Filter rule entrance
     */
    @Override
    public void run() {
        initContexts();
        Dataset<Row> failedRecords = filterEvents(this.table);
        //TODO: change it back to prod_transacational table
        writeInvalidEvents(failedRecords, "capping_result");
        // For test
        numberOfRow = failedRecords.count();
    }

    /**
     * Initialize hbase config
     * @param zookeeperQuorum zookeeper
     * @param clientPort zookeeper port
     * @param znode znode
     * @param hbaseMaster hbase master
     */
    private void initHBaseConf(String zookeeperQuorum, String clientPort, String znode, String hbaseMaster) {
        if (hbaseConf == null) {
            hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("hbase.zookeeper.quorum", zookeeperQuorum);
            hbaseConf.set("hbase.zookeeper.property.clientPort", clientPort);
            hbaseConf.set("zookeeper.znode.parent", znode);
            hbaseConf.set("hbase.master", hbaseMaster);
        }
    }

    /**
     * Init JavaSparkContext and SQLContext from spark session
     */
    private void initContexts() {
        if (javaSparkContext == null) {
            javaSparkContext = JavaSparkContext.fromSparkContext(spark().sparkContext());
        }
        if (sqlContext == null) {
            sqlContext = spark().sqlContext();
        }
    }

    /**
     * Only for test
     */
    public void setHBaseConf(Configuration hbaseConf) {
        this.hbaseConf = hbaseConf;
    }

    /**
     * Only for test
     */
    public long getNumberOfRow() {
        return numberOfRow;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        Option jobName = new Option((String) null, "jobName", true, "The job name");
        options.addOption(jobName);

        Option mode = new Option((String) null, "mode", true, "spark on yarn or local");
        mode.setRequired(true);
        options.addOption(mode);

        Option table = new Option((String) null, "table", true, "HBase table");
        table.setRequired(true);
        options.addOption(table);

        Option time = new Option((String) null, "time", true, "The time point for IP capping rule");
        time.setRequired(true);
        options.addOption(time);

        Option timeRange = new Option((String) null, "timeRange", true, "time range for IP capping rule");
        timeRange.setRequired(true);
        options.addOption(timeRange);

        Option threshold = new Option((String) null, "threshold", true, "threshold for IP capping rule");
        threshold.setRequired(true);
        options.addOption(threshold);

        Option zookeeperQuorum = new Option((String) null, "hbase.zookeeper.quorum", true, "zookeeper quorum");
        zookeeperQuorum.setRequired(true);
        options.addOption(zookeeperQuorum);

        Option zookeeperClientPort = new Option((String) null, "hbase.zookeeper.property.clientPort", true, "zookeeper client port ");
        zookeeperClientPort.setRequired(true);
        options.addOption(zookeeperClientPort);

        Option znode = new Option((String) null, "zookeeper.znode.parent", true, "znode");
        znode.setRequired(true);
        options.addOption(znode);

        Option hbaseMaster = new Option((String) null, "hbase.master", true, "hbase master");
        hbaseMaster.setRequired(true);
        options.addOption(hbaseMaster);

        CommandLineParser parser = new BasicParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("IPCappingRuleJob", options);

            System.exit(1);
            return;
        }

        IPCappingRuleJob job = new IPCappingRuleJob(cmd.getOptionValue("jobName"),
                cmd.getOptionValue("mode"), cmd.getOptionValue("table"), Long.parseLong(cmd.getOptionValue("time")),
                Long.parseLong(cmd.getOptionValue("timeRange")), Long.parseLong(cmd.getOptionValue("threshold")));
        //TODO: find a better way to get the configuration
        job.initHBaseConf(cmd.getOptionValue("hbase.zookeeper.quorum"), cmd.getOptionValue("hbase.zookeeper.property.clientPort"), cmd.getOptionValue("zookeeper.znode.parent"), cmd.getOptionValue("hbase.master"));
        try {
            job.run();
        } finally {
            job.stop();
        }
    }
}
