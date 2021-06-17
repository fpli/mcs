#!/usr/bin/env bash
set -e

export HADOOP_USER_NAME=hdfs

inputDir=$1
outputDir=$2
projectName=$3
envDir=$4

echo $inputDir
echo $outputDir
echo $projectName
echo $envDir

ts=`date +%s`
echo $ts

tokenJson="token_"$projectName"_"$ts".json"
tokenTxt="token_"$projectName"_"$ts".txt"
jobJson="job_"$projectName"_"$ts".json"
jobTxt="job_"$projectName"_"$ts".txt"
resultJson="result_"$projectName"_"$ts".json"
resultTxt="result_"$projectName"_"$ts".txt"

echo $tokenJson
echo $tokenTxt
echo $jobJson
echo $jobTxt
echo $resultJson
echo $resultTxt

curl --request POST --url https://os-identity.vip.ebayc3.com:5443/v2.0/tokens --header 'content-type: application/json' --data '{"auth":{"passwordCredentials":{"username":"2a49f8c879d34281b043cfd817c067bf","password":"k5m6PWxEbATa4Thv9o0ilxEekZrqTjZYhmTqQu8qOXEJHpfYVAktSqCrfqcyhJGN"}}}' >> $envDir/conf/$tokenJson
python $envDir/lib/TokenJsonUtil.py $envDir/conf/$tokenJson $envDir/conf/$tokenTxt
token_id=`cat $envDir/conf/$tokenTxt`
echo $token_id

curl -k -X POST "https://optimus.vip.ebay.com/api/job" -H "accept: */*" -H "AUTH_TYPE: KEYSTONE" -H "HMC_USER_TOKEN: " -H "KEYSTONE_TOKEN: $token_id" -H "Content-Type: application/json" -d "{\"src\":\"$inputDir\",\"dst\":\"$outputDir\",\"copyMode\":\"2\",\"jobType\":\"DIST_CP\",\"doAs\":\"b_marketing_tracking\",\"queue\":\"hdlq-commrce-mkt-high-mem\"}" >> $envDir/conf/$jobJson
echo "curl -k -X POST \"https://optimus.vip.ebay.com/api/job\" -H \"accept: */*\" -H \"AUTH_TYPE: KEYSTONE\" -H \"HMC_USER_TOKEN: \" -H \"KEYSTONE_TOKEN: $token_id\" -H \"Content-Type: application/json\" -d \"{\"src\":\"$inputDir\",\"dst\":\"$outputDir\",\"copyMode\":\"2\",\"jobType\":\"DIST_CP\",\"doAs\":\"b_marketing_tracking\",\"queue\":\"hdlq-commrce-mkt-high-mem\"}\""
cat $envDir/conf/$jobJson
python $envDir/lib/JobJsonUtil.py $envDir/conf/$jobJson $envDir/conf/$jobTxt
job_id=`cat $envDir/conf/$jobTxt`
echo $job_id

rm $envDir/conf/$tokenJson
rm $envDir/conf/$tokenTxt
rm $envDir/conf/$jobJson
rm $envDir/conf/$jobTxt

cnt=0
while true; do
  cnt=$(($cnt+1))
  echo "[$(date +%FT%T)][Job: ${job_id}]Attempt: ${cnt}"
  curl -k -H "Content-Type: application/json" -H "AUTH_TYPE: KEYSTONE" -H "HMC_USER_TOKEN: " -H "KEYSTONE_TOKEN: ${token_id}" -X GET https://optimus.vip.ebay.com/api/job/${job_id} >> $envDir/conf/$resultJson
  python $envDir/lib/ResultJsonUtil.py $envDir/conf/$resultJson $envDir/conf/$resultTxt
  status_cd=`cat $envDir/conf/$resultTxt`
  echo "[$(date +%FT%T)][Job: ${job_id}]Service returned status(0: READY, 1: RUNNING, 2: SUCCESS, 3: FAILURE): ${status_cd}"

  if [[ ${status_cd} == 2 ]]; then
    echo "[$(date +%FT%T)][Job: ${job_id}]**Success**"
    echo "=====================================================DistcpToHercules is completed!!======================================================"
    rm $envDir/conf/$resultJson
    rm $envDir/conf/$resultTxt
    exit 0
  elif [[ ${status_cd} == 3 ]]; then
    echo "[$(date +%FT%T)][Job: ${job_id}]**Failure**"
    echo "=====================================================DistcpToHercules ERROR!!======================================================"
    echo "You can check the job details in: https://bdp.vip.ebay.com/optimus/jobDetail/hdfs/LVS/${job_id}"
    rm $envDir/conf/$resultJson
    rm $envDir/conf/$resultTxt
    exit 1
  fi

  rm $envDir/conf/$resultJson
  rm $envDir/conf/$resultTxt
  sleep 5s
done