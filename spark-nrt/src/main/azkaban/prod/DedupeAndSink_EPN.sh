#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark yimeng@lvschocolatepits-1585074.stratus.lvs.ebay.com <<EOSSH
hostname
cd /home/spark/chocolate-sparknrt/bin/prod
pwd
export HADOOP_USER_NAME=hdfs
echo $HADOOP_USER_NAME
./dedupeAndSink.sh EPN marketingtech.ap.tracking-events.filtered-epn /apps/tracking-events-workdir /apps/tracking-events/EPN0