#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark yimeng@slcchocolatepits-1154246.stratus.slc.ebay.com <<EOSSH
hostname
cd /home/chocolate/chocolate-sparknrt-crab/bin/prod
pwd
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME
./imkCrabTransform.sh hdfs://elvisha/apps/tracking-events-workdir hdfs://slickha/apps/tracking-events/imkTransform
