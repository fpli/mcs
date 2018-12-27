#!/usr/bin/env bash
#export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_USER_NAME=hdfs
DIFF=0
DATE=`date --date=$DIFF" days ago" +%Y-%m-%d`
echo $DATE
kinit -kt b_marketing_tracking_APD.keytab b_marketing_tracking@APD.EBAY.COM
dt="date="$DATE
echo $dt

path=hdfs://elvisha/apps/tracking-events/DISPLAY/capping/$dt
apolloPath=hdfs://apollo-phx-nn-ha/apps/b_marketing_tracking/chocolate/tracking-events/DISPLAY/capping/$dt
displayLocalPath=/apps/tracking-events/DISPLAY/capping/$dt
displayApolloPath=/apps/b_marketing_tracking/chocolate/tracking-events/DISPLAY/capping/$dt

/apache/hadoop/bin/hdfs dfs -mkdir -p ${apolloPath}

hdfs dfs -ls $displayLocalPath | grep -v "^$" | awk '{print $NF}' | xargs -i basename {} | grep parquet > /tmp/dap_local.txt
/apache/hadoop/bin/hdfs dfs -ls $displayApolloPath  | grep -v "^$" | awk '{print $NF}' | xargs -i basename {} | grep parquet > /tmp/dap_apollo.txt

echo >/tmp/dap_diff.list
cat /tmp/dap_local.txt | while read filename; do
        cnt=`cat /tmp/dap_apollo.txt | grep ${filename} | wc -l`
        if [[ ${cnt} -eq 0 ]]; then
                echo "${path}/${filename}" >> /tmp/dap_diff.list
        fi
done
all_files_number=`cat /tmp/dap_diff.list | grep -v "^$" | wc -l`
if [[ ${all_files_number} -gt 0 ]]; then
    all_files=`cat /tmp/dap_diff.list | tr "\n" " "`
fi

if [ -n "$all_files" ]; then
  hadoop jar chocolate-distcp-1.0-SNAPSHOT.jar -files core-site-target.xml,hdfs-site-target.xml,b_marketing_tracking_APD.keytab -copyFromInsecureToSecure -targetprinc b_marketing_tracking@APD.EBAY.COM -targetkeytab b_marketing_tracking_APD.keytab -skipcrccheck -update ${all_files} ${apolloPath}
else
  echo "No file need to copy"
fi