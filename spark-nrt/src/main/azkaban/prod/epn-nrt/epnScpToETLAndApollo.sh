#!/bin/bash
whoami

ssh -T -i /usr/azkaban/id_rsa_spark stack@lvschocolatepits-1583720.stratus.lvs.ebay.com <<EOSSH
java -Xmx2048m -classpath /datashare/mkttracking/jobs/tracking/epnnrt/jar/cho-epn-scp-*.jar com.ebay.traffic.chocolate.EPNScp prod
EOSSH

if [ $? -eq 0 ]; then
    echo "job success"
else
    echo "job failed, retry another machine"
    ssh -T -i /usr/azkaban/id_rsa_spark stack@lvschocolatepits-1583707.stratus.lvs.ebay.com <<EOSSH
    java -Xmx2048m -classpath /datashare/mkttracking/jobs/tracking/epnnrt/jar/cho-epn-scp-*.jar com.ebay.traffic.chocolate.EPNScp prod
EOSSH
fi


