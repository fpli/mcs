#!/bin/bash
whoami
ssh -i /usr/azkaban/id_rsa_spark _choco_admin@slcchocolatepits-1242736.stratus.slc.ebay.com <<EOSSH
hostname
cd /datashare/mkttracking/tools/HdfsDataClean/bin/
pwd
export HADOOP_USER_NAME=hdfs
echo $HADOOP_USER_NAME
./dataClean.sh