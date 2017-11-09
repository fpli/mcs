export HADOOP_CONF_DIR=/usr/hdp/current/hadoop-yarn-client/etc/hadoop/
./spark/bin/spark-submit --class com.ebay.traffic.chocolate.cappingrules.ip.IPCappingRuleJob \
    --master yarn \
    --deploy-mode cluster \
    chocolate-capping-rules-1.0-SNAPSHOT-fat.jar \
      --jobName ipcappingjob \
      --mode yarn \
      --table prod_transactional \
      --time 1510177765 \
      --timeRange 86400000 \
      --threshold 1000 \
      --hbase.zookeeper.quorum lvschocolatemaster-1448894.stratus.lvs.ebay.com,lvschocolatemaster-1448895.stratus.lvs.ebay.com,lvschocolatemaster-1448897.stratus.lvs.ebay.com,lvschocolatemaster-1582061.stratus.lvs.ebay.com,lvschocolatemaster-1582062.stratus.lvs.ebay.com \
      --hbase.zookeeper.property.clientPort 2181 \
      --zookeeper.znode.parent /hbase-unsecure \
      --hbase.master lvschocolatemaster-1448895.stratus.lvs.ebay.com:60000