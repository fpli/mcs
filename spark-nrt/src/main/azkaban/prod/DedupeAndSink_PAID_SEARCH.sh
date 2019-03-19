#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark yimeng@lvschocolatepits-1585074.stratus.lvs.ebay.com <<EOSSH
hostname
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME
/datashare/mkttracking/jobs/tracking/sparknrt/bin/prod/dedupeAndSink.sh PAID_SEARCH marketingtech.ap.tracking-events.filtered-paid-search /apps/tracking-events-workdir /apps/tracking-events http://chocolateclusteres-app-private-11.stratus.lvs.ebay.com:9200 1 true