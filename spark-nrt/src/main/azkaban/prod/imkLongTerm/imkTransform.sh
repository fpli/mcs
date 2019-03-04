#!/bin/bash
CHANNEL=$1
WORK_DIR=$2
OUTPUT_DIR=$3

whoami
ssh -T -i /usr/azkaban/id_rsa_spark yimeng@lvschocolatepits-1585074.stratus.lvs.ebay.com <<EOSSH
hostname
cd /home/chocolate/chocolate-sparknrt/bin/prod
pwd
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME

./imkTransform.sh ${CHANNEL} ${WORK_DIR} ${OUTPUT_DIR}