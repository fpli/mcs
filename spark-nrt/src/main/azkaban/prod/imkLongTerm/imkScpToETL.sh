#!/bin/bash
whoami

ssh -T -i /usr/azkaban/id_rsa_spark stack@lvsnrt2batch-1471741.stratus.lvs.ebay.com <<EOSSH
/usr/share/jdk1.8.0_201/bin/java -Xmx2048m -classpath cho-panda-etl-scp-*.jar com.ebay.traffic.chocolate.ScpJob prod
EOSSH

if [ $? -eq 0 ]; then
    echo "job success"
else
    echo "job failed, retry another machine"
    ssh -T -i /usr/azkaban/id_rsa_spark stack@lvsnrt2batch-1471937.stratus.lvs.ebay.com <<EOSSH
    /usr/share/jdk1.8.0_201/bin/java -Xmx2048m -classpath cho-panda-etl-scp-*.jar com.ebay.traffic.chocolate.ScpJob prod
EOSSH
fi


