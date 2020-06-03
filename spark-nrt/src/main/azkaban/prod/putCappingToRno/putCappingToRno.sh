#!/bin/bash

# not use SubmitJobshell, this scp job is one high IO job, not want do this job in spark submit hosts
CHANNEL=$1
ssh -T -i /usr/azkaban/id_rsa_spark _choco_admin@slcchocolatepits-1242731.stratus.slc.ebay.com <<EOSSH
cd /datashare/mkttracking/jobs/chocolate-sparknrt/bin/prod
./putDataToRnoByMeta.sh hdfs://elvisha/apps/tracking-events-workdir ${CHANNEL} capping meta.rno /apps/b_marketing_tracking/chocolate/tracking-events-workdir /apps/b_marketing_tracking/chocolate/tracking-events
EOSSH

if [ $? -eq 0 ]; then
    echo "job success"
else
    echo "job failed, retry another machine"
    ssh -T -i /usr/azkaban/id_rsa_spark _choco_admin@slcchocolatepits-1242732.stratus.slc.ebay.com <<EOSSH
    cd /datashare/mkttracking/jobs/chocolate-sparknrt/bin/prod
    ./putDataToRnoByMeta.sh hdfs://elvisha/apps/tracking-events-workdir ${CHANNEL} capping meta.rno /apps/b_marketing_tracking/chocolate/tracking-events-workdir /apps/b_marketing_tracking/chocolate/tracking-events
EOSSH
fi


