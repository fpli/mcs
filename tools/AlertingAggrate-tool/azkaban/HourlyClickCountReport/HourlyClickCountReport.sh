#!/bin/bash
whoami
ssh -i /usr/azkaban/id_rsa_spark stack@slcchocolatepits-1242736.stratus.slc.ebay.com <<EOSSH
hostname
cd /datashare/mkttracking/tools/AlertingAggrate-tool/bin/
pwd
export HADOOP_USER_NAME=hdfs
echo $HADOOP_USER_NAME
./hourly_click_count_report.sh