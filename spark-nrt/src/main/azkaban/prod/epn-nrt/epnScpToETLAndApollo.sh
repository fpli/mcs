#!/bin/bash
whoami

ssh -T -i /usr/azkaban/id_rsa_spark stack@lvschocolatepits-1583720.stratus.lvs.ebay.com <<EOSSH
cd /datashare/mkttracking/jobs/tracking/epnnrt/bin/prod
./sendEPNData.sh
EOSSH

if [ $? -eq 0 ]; then
    echo "job success"
else
    echo "job failed, retry another machine"
    ssh -T -i /usr/azkaban/id_rsa_spark stack@lvschocolatepits-1583707.stratus.lvs.ebay.com <<EOSSH
    cd /datashare/mkttracking/jobs/tracking/epnnrt/bin/prod
    ./sendEPNData.sh
EOSSH
fi