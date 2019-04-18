#!/bin/bash
whoami
ssh -T -i /usr/azkaban/id_rsa_spark stack@slcchocolatepits-1154246.stratus.slc.ebay.com <<EOSSH
hostname
cd /datashare/mkttracking/jobs/chocolate-sparknrt/bin/prod
pwd

./imkGenerateDailyDoneFile.sh /apps/b_marketing_tracking/imk_tracking/daily_done_files /apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event imk_rvr_trckng_event