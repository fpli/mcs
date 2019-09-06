#!/bin/bash
whoami
ssh -i /usr/azkaban/id_rsa_spark stack@slcchocolatepits-1242736.stratus.slc.ebay.com <<EOSSH
hostname
cd /datashare/mkttracking/tools/HadoopLogCleaner/bin/
pwd
export HADOOP_USER_NAME=hdfs
echo $HADOOP_USER_NAME
./get_files_list.sh