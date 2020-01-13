#!/bin/bash
whoami
ssh -i /usr/azkaban/id_rsa_spark stack@slcchocolatepits-1242736.stratus.slc.ebay.com <<EOSSH
hostname
cd /datashare/mkttracking/tools/AlertingAggrate-tool-imk/bin
pwd
./hourly_click_count_report_imk_rno.sh viewfs://apollo-rno/apps/b_marketing_tracking/imk_tracking/imk_rvr_trckng_event/*/ false