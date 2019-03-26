DT=$(date +%Y-%m-%d)
HDP_CLICK=/apps/epn-nrt/click/date=${DT}/                   #chocolate hdfs files
LOCAL_PATH=/datashare/mkttracking/data/epn-nrt/date=${DT}   #Local file path which contains the epnnrt click result files
HDP_IMP=/apps/epn-nrt/impression/date=${DT}                 #Local file path which contains the epnnrt impression result files
NRT_PATH=/home/stack/epn-nrt/${DT}                                #the file path on epnnrt vm: lvsnrt2batch-1761265


log_dt=${HOSTNAME}_$(date +%Y%m%d%H%M%S)
log_file="/datashare/mkttracking/logs/chocolate/epn-nrt/done_${log_dt}.log"

echo "HDP_CLICK="${HDP_CLICK} | tee -a ${log_file}
echo "LOCAL_PATH="${LOCAL_PATH} | tee -a ${log_file}
echo "HDP_IMP="${HDP_IMP} | tee -a ${log_file}
echo "NRT_PATH="${NRT_PATH} | tee -a ${log_file}

if [[ ! -d "${LOCAL_PATH}/" ]]; then
    echo "chocolate-ePN ${DT}'s NRT data is NOT generated~!!!" | mail -s "No NRT result!!!!" DL-eBay-Chocolate-GC@ebay.com
    exit 1;
fi

hdfs dfs -ls -C ${HDP_CLICK} > ${LOCAL_PATH}/all_files.txt
ls ${LOCAL_PATH}/*.processed > ${LOCAL_PATH}/all_processed.txt

HDP_CLICK_FORMAT=`echo "${HDP_CLICK}" | sed 's:\/:\\\/:g'`
sed -i -E "s/$HDP_CLICK_FORMAT//g" all_files.txt
LOCAL_PATH_FORMAT=`echo "${LOCAL_PATH}/" | sed 's:\/:\\\/:g'`
sed -i -E "s/.processed//g" all_processed.txt
sed -i -E "s/$LOCAL_PATH_FORMAT//g" all_processed.txt

sort all_files.txt > sorted_all_files.txt
sort all_processed.txt > sorted_all_processed.txt

diff_line=`diff sorted_all_files.txt sorted_all_processed.txt |wc -l`
if [[ ${diff_line} -ge 1 ]]; then
    echo "chocolate-ePN ${DT}'s NRT completed." | mail -s "NRT completed" DL-eBay-Chocolate-GC@ebay.com
    exit 0
else
    echo "chocolate-ePN ${DT}'s NRT delayed!!!!" | mail -s "NRT delayed!!!!" DL-eBay-Chocolate-GC@ebay.com
    exit 1
fi
