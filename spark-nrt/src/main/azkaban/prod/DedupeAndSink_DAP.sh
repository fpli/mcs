#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark yimeng@lvschocolatepits-1585074.stratus.lvs.ebay.com <<EOSSH
hostname
cd /home/hbase/sparkjobs/chocolate-3.0-RELEASE-bin/chocolate-sparknrt/bin/prod
pwd
export HADOOP_USER_NAME=hdfs
echo $HADOOP_USER_NAME
./dedupeAndSink.sh DISPLAY marketingtech.ap.tracking-events.filtered-display /tmp/jialili1/workdir /tmp/jialili1/outputdir