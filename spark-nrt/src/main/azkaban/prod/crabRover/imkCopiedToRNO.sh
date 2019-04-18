#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark stack@slcchocolatepits-1154246.stratus.slc.ebay.com <<EOSSH
hostname
cd /datashare/mkttracking/jobs/chocolate-sparknrt/bin/prod
pwd
export HADOOP_USER_NAME=chocolate
echo $HADOOP_USER_NAME

./imkCopiedToRNO.sh /apps/tracking-events/crabTransform/imkOutput /apps/tracking-events/imkTransform/imkOutput /apps/tracking-events/crabTransform/dtlOutput /apps/tracking-events/imkTransform/dtlOutput
