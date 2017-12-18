#!/bin/sh

cd /Users/yimeng/Documents/workspace-ij/chocolate-forked/marketing-tracking
rm -r chocolate-1.0-SNAPSHOT-bin
rm chocolate-1.0-SNAPSHOT-bin.gz
sh make-distribution.sh

ssh -i ~/.ssh/id_rsa_stack stack@chocolate-qa-slc-7-5904.slc01.dev.ebayc3.com
cd /usr/yimeng
if [ -d capper-tmp ];then
	sudo rm -r capper-tmp/
	sudo mkdir capper-tmp
	sudo chmod 777 capper-tmp
fi
exit

cd /Users/yimeng/Documents/workspace-ij/chocolate-forked/marketing-tracking
if [ -d chocolate-1.0-SNAPSHOT-bin ];then
	tar cvf chocolate-1.0-SNAPSHOT-bin.gz chocolate-1.0-SNAPSHOT-bin/
	
	sftp -i  ~/.ssh/id_rsa_stack stack@chocolate-qa-slc-7-5904.slc01.dev.ebayc3.com:
	put chocolate-1.0-SNAPSHOT-bin.gz /usr/yimeng/capper-tmp
fi

ssh -i ~/.ssh/id_rsa_stack stack@chocolate-qa-slc-7-5904.slc01.dev.ebayc3.com
cd /usr/yimeng/capper-tmp
tar xvf chocolate-1.0-SNAPSHOT-bin.gz
sudo cp /usr/yimeng/capper/chocolate-1.0-SNAPSHOT-bin/chocolate-cappingrule/bin/chocolate-env.sh /usr/yimeng/capper-tmp/chocolate-1.0-SNAPSHOT-bin/chocolate-cappingrule/bin/chocolate-env.sh
cd chocolate-1.0-SNAPSHOT-bin/chocolate-cappingrule/bin/
sudo chmod 777 reportGeneratorJob-qa.sh
sudo su spark 
./reportGeneratorJob-qa.sh qa_transactional snid_capping_result "2017-11-16 23:00:00" "2017-11-16 23:59:59"
