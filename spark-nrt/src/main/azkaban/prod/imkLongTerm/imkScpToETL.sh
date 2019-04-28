#!/bin/bash
whoami

ssh -T -i /usr/azkaban/id_rsa_spark stack@lvschocolatepits-1583701.stratus.lvs.ebay.com <<EOSSH
java -Xmx2048m -classpath cho-panda-etl-scp-*.jar com.ebay.traffic.chocolate.ScpJob prod
EOSSH

if [ $? -eq 0 ]; then
    echo "job success"
else
    echo "job failed, retry another machine"
    ssh -T -i /usr/azkaban/id_rsa_spark stack@lvschocolatepits-1583700.stratus.lvs.ebay.com <<EOSSH
    java -Xmx2048m -classpath cho-panda-etl-scp-*.jar com.ebay.traffic.chocolate.ScpJob prod
EOSSH
fi


