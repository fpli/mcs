#!/usr/bin/env bash
#export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_USER_NAME=hdfs
DIFF=0
DATE=`date --date=$DIFF" days ago" +%Y-%m-%d`
echo $DATE
kinit -kt /datashare/mkttracking/common/b_marketing_tracking_APD.keytab b_marketing_tracking@APD.EBAY.COM
dt="date="$DATE
echo $dt

local=/apps/tracking-events/EPN/capping/$dt
apollo=/apps/b_marketing_tracking/chocolate/tracking-events/EPN/capping/$dt
path=hdfs://elvisha/apps/tracking-events/EPN/capping/$dt
apolloPath=hdfs://apollo-phx-nn-ha/apps/b_marketing_tracking/chocolate/tracking-events/EPN/capping/$dt

hdfs dfs -ls $local | grep -v "^$" | awk '{print $NF}' | xargs -i basename {} | grep parquet> /tmp/main.txt
/apache/hadoop/bin/hdfs dfs -ls $apollo  | grep -v "^$" | awk '{print $NF}' | xargs -i basename {} | grep parquet> /tmp/sub.txt

echo >/tmp/all.list
cat /tmp/main.txt | while read filename; do
        cnt=`cat /tmp/sub.txt | grep ${filename} | wc -l`
        if [[ ${cnt} -eq 0 ]]; then
                echo "${path}/${filename}" >> /tmp/all.list
        fi
done

all_files_number=`cat /tmp/all.list | grep -v "^$" | wc -l`
if [[ ${all_files_number} -gt 0 ]]; then
    all_files=`cat /tmp/all.list | tr "\n" " "`
fi

if [ -n "$all_files" ]; then
  hadoop jar chocolate-distcp-1.0-SNAPSHOT.jar -files core-site-target.xml,hdfs-site-target.xml,b_marketing_tracking_APD.keytab -copyFromInsecureToSecure -targetprinc b_marketing_tracking@APD.EBAY.COM -targetkeytab b_marketing_tracking_APD.keytab -skipcrccheck -update ${all_files} ${apolloPath}
else
  echo "No file need to copy"
fi