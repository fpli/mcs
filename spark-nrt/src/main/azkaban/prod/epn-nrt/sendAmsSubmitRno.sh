#!/bin/bash

set -x

COMMAND=$1

whoami

ssh -T -i /usr/azkaban/id_rsa_spark _choco_admin@slcchocolatepits-1154246.stratus.slc.ebay.com <<EOSSH
cd /datashare/mkttracking/jobs/tracking/epnnrt/bin/prod
${COMMAND}
EOSSH

if [ $? -eq 0 ]; then
    echo "job success"
else
    echo "job failed, retry another machine"
    ssh -T -i /usr/azkaban/id_rsa_spark _choco_admin@slcchocolatepits-1242730.stratus.slc.ebay.com <<EOSSH
    cd /datashare/mkttracking/jobs/tracking/epnnrt/bin/prod
    ${COMMAND}
EOSSH
fi
