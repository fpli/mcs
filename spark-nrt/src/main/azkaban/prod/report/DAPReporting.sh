#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark stack@slcchocolatepits-1154246.stratus.slc.ebay.com <<EOSSH
hostname
cd /home/chocolate/chocolate-sparknrt-crab/bin/prod
pwd
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME
./nonEpnReporting.sh hdfs://elvisha/apps/tracking-events-workdir hdfs://elvisha/apps/tracking-events-archiveDir DISPLAY
