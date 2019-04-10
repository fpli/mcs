#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark yimeng@slcchocolatepits-1154246.stratus.slc.ebay.com <<EOSSH
hostname
cd /home/chocolate/chocolate-sparknrt-crab/bin/prod
pwd
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME
./scpImkToReno.sh /apps/tracking-events/crabTransform/imkOutput /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event /mnt/imkToReno/crab/imkTemp
./scpImkToReno.sh /apps/tracking-events/imkTransform/imkOutput /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event /mnt/imkToReno/imk/imkTemp
./scpImkToReno.sh /apps/tracking-events/crabTransform/dtlOutput /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_dtl /mnt/imkToReno/crab/dtlTemp
./scpImkToReno.sh /apps/tracking-events/imkTransform/dtlOutput /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event_dtl /mnt/imkToReno/imk/dtlTemp
