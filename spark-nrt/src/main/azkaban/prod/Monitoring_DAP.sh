#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark stack@slcchocolatequeue-2168406.stratus.slc.ebay.com <<EOSSH
hostname
cd /home/chocolate/chocolate-sparknrt/bin/prod
pwd
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME
export hdfs=hdfs://elvisha
echo $hdfs
./monitoring.sh DISPLAY $hdfs/apps/tracking-events-workdir http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200