#!/bin/bash

# not use SubmitJobshell, this scp job is one high IO job, not want do this job in spark submit hosts

ssh -T -i /usr/azkaban/id_rsa_spark stack@lvschocolatepits-1583720.stratus.lvs.ebay.com <<EOSSH
cd /datashare/mkttracking/jobs/tracking/chocolate-sparknrt-imk/bin/prod
./scpDataToETLByMeta.sh /apps/tracking-events-workdir PAID_SEARCH imkDump meta.etl /datashare/mkttracking/rsa/panda_id_rsa etl_panda_push@lvsdpeetl012.lvs.ebay.com:/dw/etl/home/prod/land/dw_dfe/rvr_test YES NO
EOSSH

if [ $? -eq 0 ]; then
    echo "job success"
else
    echo "job failed, retry another machine"
    ssh -T -i /usr/azkaban/id_rsa_spark stack@lvschocolatepits-1583707.stratus.lvs.ebay.com <<EOSSH
    cd /datashare/mkttracking/jobs/tracking/chocolate-sparknrt-imk/bin/prod
    ./scpDataToETLByMeta.sh /apps/tracking-events-workdir PAID_SEARCH imkDump meta.etl /datashare/mkttracking/rsa/panda_id_rsa etl_panda_push@lvsdpeetl012.lvs.ebay.com:/dw/etl/home/prod/land/dw_dfe/rvr_test YES NO
EOSSH
fi


