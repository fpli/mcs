#!/bin/bash
whoami
ssh -i /usr/azkaban/id_rsa_appdl xiangli4@slcchocolatepits-1242736.stratus.slc.ebay.com <<EOSSH
hostname
cd /home/hdfs/CheckRecover/
pwd
export HADOOP_USER_NAME=hdfs
echo $HADOOP_USER_NAME
./checkFile.sh