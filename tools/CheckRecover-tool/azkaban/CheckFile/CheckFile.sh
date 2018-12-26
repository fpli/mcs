#!/bin/bash
whoami
ssh -i /usr/azkaban/id_rsa_spark yimeng@lvschocolatepits-1585074.stratus.lvs.ebay.com <<EOSSH
hostname
cd /home/chocolate/tools/CheckRecover-tool/bin/
pwd
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME
./checkFile.sh