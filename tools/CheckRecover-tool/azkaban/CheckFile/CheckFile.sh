#!/bin/bash
whoami
ssh -i /usr/azkaban/id_rsa_lxiong1 lxiong1@slcchocolatepits-1242736.stratus.slc.ebay.com <<EOSSH
hostname
cd /datashare/mkttracking/tools/CheckRecover-tool/bin/
pwd
export HADOOP_USER_NAME=hdfs
echo $HADOOP_USER_NAME
./checkFile.sh