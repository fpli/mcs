#!/bin/bash

set -x

COMMAND=$1

whoami

ssh -T -i /usr/azkaban/id_rsa_spark _choco_admin@lvschocolatepits-1583707.stratus.lvs.ebay.com <<EOSSH
cd /datashare/mkttracking/jobs/tracking/epnnrt/bin/prod
${COMMAND}
EOSSH

if [ $? -eq 0 ]; then
    echo "job success"
else
    echo "job failed, retry another machine"
    ssh -T -i /usr/azkaban/id_rsa_spark _choco_admin@lvschocolatepits-1583708.stratus.lvs.ebay.com <<EOSSH
    cd /datashare/mkttracking/jobs/tracking/epnnrt/bin/prod
    ${COMMAND}
EOSSH
fi
