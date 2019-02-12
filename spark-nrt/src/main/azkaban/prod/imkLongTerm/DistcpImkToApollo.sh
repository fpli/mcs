#!/usr/bin/env bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark yimeng@slcchocolatepits-1242746.stratus.slc.ebay.com <<EOSSH
hostname
cd /apache/distcp
pwd
export HADOOP_USER_NAME=hdfs
echo $HADOOP_USER_NAME
./RoverImkToApollo.sh /apps/tracking-events/imk/output/imkOutput hdfs://apollo-phx-nn-ha/apps/b_marketing_tracking/chocolate/rover-tfs-imk /apps/tracking-events/imk/archive/imkArchive
./RoverImkToApollo.sh /apps/tracking-events/imk/output/dtlOutput hdfs://apollo-phx-nn-ha/apps/b_marketing_tracking/chocolate/rover-tfs-imk-dtl /apps/tracking-events/imk/archive/dtlArchive
./RoverImkToApollo.sh /apps/tracking-events/imk/output/mgOutput hdfs://apollo-phx-nn-ha/apps/b_marketing_tracking/chocolate/rover-tfs-imk-mg /apps/tracking-events/imk/archive/mgArchive
