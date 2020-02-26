#!/bin/bash

# not use SubmitJobshell, this scp job is one high IO job, not want do this job in spark submit hosts

ssh -T -i /usr/azkaban/id_rsa_spark stack@slcchocolatepits-1154246.stratus.slc.ebay.com <<EOSSH
cd /datashare/mkttracking/jobs/tracking/chocolate-sparknrt-imk/bin/prod
./scpDataToETLByMeta.sh hdfs://elvisha/apps/tracking-events-workdir DISPLAY imkDump meta.etl /datashare/mkttracking/rsa/panda_id_rsa etl_panda_push@lvsdpeetl012.lvs.ebay.com:/dw/etl/home/prod/land/dw_dfe/rvr_test YES NO
EOSSH

if [ $? -eq 0 ]; then
    echo "job success"
else
    echo "job failed, retry another machine"
    ssh -T -i /usr/azkaban/id_rsa_spark stack@slcchocolatepits-1242730.stratus.slc.ebay.com <<EOSSH
    cd /datashare/mkttracking/jobs/tracking/chocolate-sparknrt-imk/bin/prod
    ./scpDataToETLByMeta.sh hdfs://elvisha/apps/tracking-events-workdir DISPLAY imkDump meta.etl /datashare/mkttracking/rsa/panda_id_rsa etl_panda_push@lvsdpeetl012.lvs.ebay.com:/dw/etl/home/prod/land/dw_dfe/rvr_test YES NO
EOSSH
fi


