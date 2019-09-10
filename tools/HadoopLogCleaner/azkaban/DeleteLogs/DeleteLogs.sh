#!/bin/bash
whoami
ssh -i /usr/azkaban/id_rsa_spark stack@slcchocolatepits-1242736.stratus.slc.ebay.com <<EOSSH
hostname
cd /datashare/mkttracking/tools/HadoopLogCleaner/bin/
pwd
export HADOOP_USER_NAME=hdfs
export HADOOP_CLIENT_OPTS="-XX:-UseGCOverheadLimit -Xmx6g"
echo $HADOOP_USER_NAME
./delete_logs.sh