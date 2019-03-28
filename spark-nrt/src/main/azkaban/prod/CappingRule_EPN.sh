#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark stack@lvschocolatepits-1585074.stratus.lvs.ebay.com <<EOSSH
hostname
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME
export hdfs=hdfs://elvisha
echo $hdfs
/datashare/mkttracking/jobs/tracking/sparknrt/bin/prod/cappingRule.sh EPN $hdfs/apps/tracking-events-workdir $hdfs/apps/tracking-events $hdfs/apps/tracking-events-archiveDir 150 http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200