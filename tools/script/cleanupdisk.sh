#!/bin/bash

OUT_FIL0=/tmp/cleanupdisk.$$.out0
OUT_FIL1=/tmp/cleanupdisk.$$.out1
OUT_FIL2=/tmp/cleanupdisk.$$.out2
OUT_FIL3=/tmp/cleanupdisk.$$.out3

trap "/bin/rm $OUT_FIL0 $OUT_FIL1 $OUT_FIL2 $OUT_FIL3" 1 2 3 9 15 0

exec 2> OUT_FIL2

clear

df -k | head -1 > $OUT_FIL0
#Filesystem            kbytes    used   avail capacity  Mounted on

df -h | sed -n '2,$p' | cat > $OUT_FIL1

/bin/echo -n 'RUNNING: .'
/bin/echo -n ' .'

exec 2> $OUT_FIL3

cat /dev/null > /usr/share/aiops/metrics_collector/kafkaproducer.log
find /ebay/cronus -name out-\*.log -exec truncate -s 0 {} \;
find /ebay/uc4/agent-out/ -size +2G -exec truncate -s 0 {} \;
find /var/log/cronus/ -name .sys.*.log  -exec truncate -s 0 {} \;
find /ebay/cronus/software/cronusapp_home/ -name viewitem_stacktrace.log -exec truncate -s 0 {} \;
find /ebay/cronus/software/cronusapp_home -name \*.log -exec truncate -s 0 {} \;
find /var/log/cronus/ -name .sys.*.log.*  -exec truncate -s 0 {} \;
find /var/lib/docker/containers ./ -name *.log -exec truncate -s 0 {} \;
find /home/_te_user/targetingEngine/log -name start_te_service.*.log -exec truncate -s 0 {} \;
docker images --no-trunc | grep '<none>' | awk '{ print $3 }'|xargs -r docker rmi
/ebay/zookeeper/bin/zkCleanup.sh -n 10
find /var/log/cronus -name .sys*.log -exec truncate -s 0 {} \;
find /usr/local/ebay/log -name nr.log -exec truncate -s 0 {} \;
rm -f /var/log/cronus/.sys*.log.*
cat /dev/null > /ebay/cronus/software/druid/logstash/logstash-1.4.2/bin/nohup.out
cat /dev/null >  /ebay/local/var/logs/accessa.log
cat /dev/null >  /var/log/elasticsearch/invinsight.log
find /opt/tools/vendor/tomcat/*/logs/ -name catalina.out -exec truncate -s 0 {} \;
find /var/log/cronus/ -name .s*\*.out -exec truncate -s 0 {} \;
find /var/log/ -name unused.log -exec truncate -s 0 {} \;
find /var/log/ -name daemon.log -exec truncate -s 0 {} \;
find /var/log/ -name munin-graph.log -exec truncate -s 0 {} \;
find /home/_te_user/targetingEngine/log/ -name  -exec truncate -s 0 {} \;
find /ebay/evps/zoo-log/ -name zoo-start.log \*.log -exec truncate -s 0 {} \;
rm -f /var/log/munin/*.*.*
rm -f  /var/log/cronus/.s*.1
find /var/log/splunk-nfs/ -name \*.log -exec truncate -s 0 {} \;
find /ebay/cronus/software/service_nodes/.ENV*/rheos_schema_registry/1.0.0_2017033000050.unx/cronus/scripts/logs/ -name rheossr.log  -exec truncate -s 0 {} \;
find /ebay/cronus/software/service_nodes/.ENV*/installed-packages/sojenrich/*/sojenrich*/scripts/ -name aero.log.\*  -exec truncate -s 0 {} \;
find /ebay/cronus/software/service_nodes/.ENV*//installed-packages/Tomcat/ -name pmapi.log.\* -mtime +30 -delete
find /ebay/cronus/software/service_nodes/.ENV*/installed-packages/Tomcat/*/cronus/scripts/Tomcat/logs -name aero-ebay.log*.tmp -exec truncate -s 0 {} \;
rm -f /ebay/cronus/software/service_nodes/.ENV*/installed-packages/Tomcat/*/cronus/scripts/Tomcat/logs/*.gz 
find /var/log/elasticsearch -mtime +1 -delete
find /usr/local/share/kafka/kafka_2.10-0.10.2.0/logs -mtime +1 -delete
find /ebay/cronus/software/service_nodes -name \*.log -exec truncate -s 0 {} \;
rm -f /var/log/hadoop/hadoop/hadoop-hadoop-datanode.log.*
rm -f /var/log/audit/*.*.*
rm -f /var/log/postgresql/*.log.*
cat /dev/null > /opt/tools/vendor/tomcat/apache-tomcat-7.0.52/logs/catalina.out
rm -f /opt/tools/vendor/tomcat/apache-tomcat-7.0.52/logs/catalina.2*
cat /dev/null > /var/log/logstash.log
cat /dev/null > /var/log/audit/audit.log
cat /dev/null > /var/log/messages
cat /dev/null > /var/log/maillog
cat /dev/null > /var/log/cron
find /var/log/comet-straas -name straas-agent.log -exec truncate -s 0 {} \;
find /ebay/ -name nohup.out -exec truncate -s 0 {} \;
find /ebay/builds/aggregator/current -name core.\* -delete
cat /dev/null >  /ebay/RateLimiter/monitor_scripts/error.log
cat /dev/null >  /ebay/RateLimiter/monitor_scripts/queue_error.log
rm -f /var/log/hadoop/hadoop/hadoop-hadoop-datanode.log.*
rm -f /var/log/jenkins/jenkins.log-*.gz
cat /dev/null > /var/log/jenkins/jenkins.log
find /ebay/local/var/logs -name accessa.log.\* -exec truncate -s 0 {} \;
find /ebay/apache-tomcat-8.0.27/webapps/QueryServiceApp/WEB-INF/classes/cachedir -mtime +1 -delete
find /home/appmon/apache-tomcat/logs -name catalina.out -exec truncate -s 0 {} \;
find /ebay/cronus/software/service_nodes/.E*/installed-packages/mbegrpht/0.1.20160115131622.unx/cronus/scripts/log/statsd-0 -name out.log -exec truncate -s 0 {} \;
find /usr/local -name \*.hprof -exec rm -f {} \;
/bin/echo -n ' .'
rm -f heapdump.*
rm -f javacore.*
find /usr/local/ -name catalina.out -exec truncate -s 0 {} \;
find /ebay/ -name nohup.out -exec truncate -s 0 {} \;
find /ebay/builds/aggregator/current -name core.\* -delete
find /ebay/ -name catalina.out -exec truncate -s 0 {} \;
find /opt/zookeeper/data/version-2 -mtime +7 -delete
find /home/appmon/apache-tomcat/logs -name catalina.out -exec truncate -s 0 {} \;
rm -f /kafka/kafka_2.10-0.8.2.2/logs/server.log.*
cat /dev/null > /kafka/kafka_2.10-0.8.2.2/logs/server.log
rm -f /home/appmon/apache-tomcat/logs/catalina.*.log
find /ebay/cronus/software/service_nodes/.*/installed-packages/ -name zookeper.out -exec truncate -s 0 {} \;
find /var/lib/docker -name *.log -exec du -sh {} \;|grep G|awk {'print "cat /dev/null > " $2'}|sh
find /var/lib -name \*.log -exec truncate -s 0 {} \;
/bin/echo -n ' .'
find /var/lib/docker -name *.log -exec du -sh {} \;|grep M|awk {'print "cat /dev/null > " $2'}|sh
find /ebay/cronus/software/service_nodes/ -name zookeeper.out -exec truncate -s 0 {} \;
find /home/cronusapp/sip_dashboard/dashboard-1.0.048/logs -name application.log -exec truncate -s 0 {} \;
find /opt/tomcat/logs/ -name \*.log -mtime +7 -exec rm -f {} \;
find /usr/local/tomcat/logs/ -mtime +1 -exec rm -f {} \;
find /ebay/cronus/software/cronusapp_home/cassini/logs -mtime +30 -exec rm -f {} \;
find /var/log/apache2/ -mtime +30 -exec rm -f {} \;
find /opt/tomcat/ -name catalina.out|awk {'print "cat /dev/null > " $1'}|sh
find /ebay/cronus/software/service_nodes  -name \*log.out -exec truncate -s 0 {} \;
find /var/log/ -name avs.log|awk {'print "cat /dev/null > " $1'}|sh
find /mnt/prontologs/pronto -mtime +2 -name \*.log -delete
ls -al /ebay/uc4/agent-out|grep TXT|awk {'print "cat /dev/null >  /ebay/uc4/agent-out/" $8'}|sh
find /ebay/core -name core\* -exec rm -f {} \;
cat /dev/null >  /home/cronusapp/apache2/apache-sbe-rootserver/log/apache-sbe-root-server-error.log
/bin/echo -n ' .'
cat /dev/null > /ebay/conveyor/logs/conveyor.log
find /var/log/logstash -name logstash-collector.log |awk {'print "cat /dev/null > " $1'}|sh
find /var/log/logstash -name logstash-agent.log |awk {'print "cat /dev/null > " $1'}|sh
rm -f /var/log/kern.log.*
cat /dev/null > /var/log/kern.log
rm -f /opt/tomcat/logs/catalina.out.*
rm -f /opt/tomcat/logs/*.[0-9]
cat /dev/null > /var/log/upstart/kafka-broker.log
find /ebay/uc4/agent-out/ -mtime +30 -exec rm -f {} \;
cat /dev/null > /home/cronusapp/sip_dashboard/dashboard-1.0.044/app.log
cat /dev/null > /home/cronusapp/sip_dashboard/dashboard-1.0.044/logs/application.log
rm -f /ebay/search/logs/*.gz
cat /dev/null > /ebay/search/logs/access_log
/bin/echo -n ' .'
cat /dev/null > /ebay/search/logs/error_log
find /home/appmon/apache-tomcat/logs/ -mtime +3 -exec rm -f {} \;
rm -f /home/appmon/apache-tomcat/logs/*catalina.2016-*.log
cat /dev/null > /home/appmon/apache-tomcat/logs/catalina.out
rm -f /var/log/syslog.*
rm -f /var/log/adm/pacct.2*
rm -f /var/adm/log/pacct.2*
rm -f /var/adm/pacct.2*
cat /dev/null > /var/adm/wtmpx
cat /dev/null > /var/adm/pacct
find /usr/local/search/software/elasticsearch-2.2.0/logs -mtime +7 -exec rm -f {} \;
cat dev/null> /usr/local/search/software/elasticsearch-2.2.0/logs/elasticsearch.log
find /ebay/cronus/software/service_nodes/ -name err-0.log|awk {'print "cat /dev/null > " $1'}|sh
/bin/echo -n ' .'
find /ebay/cronus/software/service_nodes/ -name err-1.log|awk {'print "cat /dev/null > " $1'}|sh
find /ebay/cronus/software/service_nodes/ -name err-2.log|awk {'print "cat /dev/null > " $1'}|sh
find /ebay/cronus/software/service_nodes/ -name out-0.log|awk {'print "cat /dev/null > " $1'}|sh
find /ebay/cronus/software/service_nodes/ -name out-1.log|awk {'print "cat /dev/null > " $1'}|sh
find /ebay/cronus/software/service_nodes/ -name out-2.log|awk {'print "cat /dev/null > " $1'}|sh
find /ebay/cronus/software/service_nodes/ -name derby.log|awk {'print "cat /dev/null > " $1'}|sh
find /ebay/cronus/software/service_nodes/ -name derby.log|awk {'print "cat /dev/null > " $1'}|sh
find /opt/logstash/ -name logstash_output.data|awk {'print "cat /dev/null > " $1'}|sh
find /usr/local/software/ -name elasticsearch.log.2\*  -exec rm -f {} \;
find /usr/local/software/ -name elasticsearch.log|awk {'print "cat /dev/null > " $1'}|sh
rm -f /var/log/maillog-2*
rm -f /var/log/messages-2*
find /ebay/uc4/agent-out -mtime +4 -exec rm -f {} \;
/bin/echo -n ' .'
find /ebay/cronus/software/service_nodes/ -name aero-ebay\*.log|awk {'print "cat /dev/null > " $1'}|sh
find /ebay/cronus/software/service_nodes/ -name server-all.log|awk {'print "cat /dev/null > " $1'}|sh
find /ebay/cronus/software/service_nodes/ -name db.log|awk {'print "cat /dev/null > " $1'}|sh
find /ebay/cronus/software/service_nodes/ -name catalina.out|awk {'print "cat /dev/null > " $1'}|sh
find /ebay/cronus/software/service_nodes/ -name wrapper.log|awk {'print "cat /dev/null > " $1'}|sh
find /ebay/zookeeper/data/version-2  -mtime +30 -exec rm -f {} \;
find /ebay -name *.log.*|awk {'print "rm -f " $1'}|sh
find /ebay -name java*|grep prof|awk {'print "rm -f " $1'}|sh
find /ebay -name \*2015\*.log -exec rm -f {} \;
find /ebay -name \*2016\*.log -exec rm -f {} \;
find /ebay -name core.*.dmp -exec rm -f {} \;
find /ebay -name core.0\* -exec rm -f {} \;
find /ebay -name core.1\* -exec rm -f {} \;
/bin/echo -n ' .'
find /ebay -name core.2\* -exec rm -f {} \;
find /ebay -name core.3\* -exec rm -f {} \;
find /ebay -name core.4\* -exec rm -f {} \;
find /ebay -name core.5\* -exec rm -f {} \;
find /ebay -name core.6\* -exec rm -f {} \;
find /ebay -name core.7\* -exec rm -f {} \;
find /ebay -name core.8\* -exec rm -f {} \;
find /ebay -name core.9\* -exec rm -f {} \;
find /ebay -name err-0.log|awk {'print "cat /dev/null > " $1'}|sh
find /ebay -name out-0.log|awk {'print "cat /dev/null > " $1'}|sh
find /ebay/ -name stdout.log|awk {'print "cat /dev/null > " $1'}|sh
find /home/ -name catalina.out|awk {'print "cat /dev/null > " $1'}|sh
find /ebay/cronus/software/cronusapp_home/services/loggy -mtime +7 -name \*.gz -exec rm -f {} \;
/bin/echo -n ' .'
find /ebay/uc4/agent-out -name \*.TXT -mtime +1 -exec rm -f {} \;
find /ebay/zookeeper/datalog/version-2 -name log.\* -mtime +60 -exec rm -f {} \;
/ebay/cronus/software/cronusapp_home/cassini/cassini_logpusher_titan/cronus/scripts/service-startup
find /usr/local/share/kafka/kafka*/logs -mtime +0 -delete
find /usr/local/share/kafka/kafka*/logs -name  kafkaServer.out -exec truncate -s 0 {} \;
find /usr/local/share/zookeeperdata/version-2 -mtime +7 -delete
find /usr/local/share/kafka/kafka_2.10-0.10.2.0/logs -name zookeeper.out -exec truncate -s 0 {} \;
rm -f /usr/share/elasticsearch/*.hprof


/bin/echo -n ' DONE'
/bin/echo
/bin/echo "Checking the deleted 'open' files  --  From 'lsof' output ..."

pids=`lsof /|grep deleted|awk {'print $2'}|sort|uniq`
for i in $pids
do
        cd /proc/$i/fd
        ls -al|grep deleted|awk {'print "cat /dev/null > " $9'}|sh > /dev/null
done

find /proc/*/fd -ls 2> /dev/null | awk '/deleted/ {print $11}' |awk {'print "cat /dev/null >" $1'}|sh

sleep 1

#clear

#cat $OUT_FIL3
echo; echo; echo


echo " *** BEFORE ***"
cat $OUT_FIL0 $OUT_FIL1
echo; echo; echo


echo " *** AFTER  ***"
df -h | sed -n '2,$p' | cat > $OUT_FIL2
cat $OUT_FIL0 $OUT_FIL2
echo; echo; echo

echo " *** CHANGE ***"
diff $OUT_FIL1 $OUT_FIL2 | egrep '<|>|--' | sed 's/   */    /;s/</Before/;s/>/After-/'
echo;echo

exit
exit
exit
