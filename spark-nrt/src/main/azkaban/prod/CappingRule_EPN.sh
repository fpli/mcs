#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark yimeng@lvschocolatepits-1585074.stratus.lvs.ebay.com <<EOSSH
hostname
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME
/datashare/mkttracking/jobs/tracking/sparknrt/bin/prod/cappingRule.sh EPN /apps/tracking-events-workdir /apps/tracking-events /apps/tracking-events-archiveDir 150 http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200 false