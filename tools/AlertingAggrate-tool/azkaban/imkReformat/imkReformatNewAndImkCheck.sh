#!/bin/bash
whoami
ssh -i /usr/azkaban/id_rsa_spark _choco_admin@slcchocolatepits-1242736.stratus.slc.ebay.com <<EOSSH
hostname

whoami
cd /datashare/mkttracking/jobs/imkReformate
pwd

./imkReformatDataCheck.sh 20200101 20200531 imk