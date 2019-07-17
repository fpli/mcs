#!/bin/bash

# Common script to send data from c3 to reno or hercules.
# Meta file is needed.
# Date partition, so DEST_DIR must contains date column's name, eg: /sys/edw/imk/im_tracking/epn/ams_click/snapshot/click_dt=


WORK_DIR=$1
CHANNEL=$2
USAGE=$3
META_SUFFIX=$4
DEST_DIR=$5
DEST_DC=$6
TOUCH_PROCESS_FILE=$7

function process_one_meta(){
    meta_file=$1
    output_file=output_file.txt
    meta_file_name=$(basename "$meta_file")
    rm -f ${meta_file_name}

    ## get one meta file from chocolate hdfs and parse it
    hdfs dfs -get ${meta_file}
    python ../readMetaFile.py ${meta_file_name} ${output_file}
    rcode=$?
    if [ ${rcode} -ne 0 ]
    then
        echo -e "Failed to parse meta file: ${meta_file_name}!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[NRT ERROR] Error in parsing meta file!!!" -v DL-eBay-Chocolate-GC@ebay.com
        exit ${rcode}
    fi
    files_size=`cat ${output_file} | wc -l`
    echo "Start upload files size:"${files_size}

    data_files=`cat ${output_file} | tr "\n" " "`
    while read -r date_file; do
        date=`echo ${date_file} | cut -d" " -f1`
        data_file=`echo ${date_file} | cut -d" " -f2`
        data_file_name=$(basename "$data_file")
        rm -f data_file_name
        hdfs dfs -get ${data_file}
        dest_full_dir=${DEST_DIR}${date}


        ####################################### Generate epn nrt processed file #######################################
        if [ $2 = "YES" ]
        then
            touch "/datashare/mkttracking/data/epn-nrt/process/date=${date}.processed"
        fi


        if [ "${DEST_DC}" == "reno" ]
        then
            ########################################## Send data to Apollo Reno ##########################################

            echo "====================== Start sending ${data_file} to ${dest_full_dir} ======================"

            ## Create date dir in reno if it doesn't exist
            /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -test -e ${dest_full_dir}
            if [ $? -ne 0 ]; then
                echo "Create reno folder for ${date}"
                /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -mkdir ${dest_full_dir}
            fi

            retry=1
            rcode_rno=1
            until [[ ${retry} -gt 3 ]]
            do
                /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -put ${data_file_name} ${dest_full_dir}
                rcode_rno=$?
                if [ ${rcode_rno} -eq 0 ]
                then
                    echo "Successfully send to Reno: "${data_file_name}
                    break
                else
                    /datashare/mkttracking/tools/apollo_rno/hadoop_apollo_rno/bin/hadoop fs -test -e ${dest_full_dir}'/'${data_file_name}
                    if [ $? -eq 0 ]; then
                        echo "${data_file_name} already exists!!"
                        rcode_rno=0
                        break
                    else
                        echo "Faild to send to Reno, retrying ${retry}"
                        retry=`expr ${retry} + 1`
                    fi
                fi
            done
            rm -f ${data_file_name}
            if [ ${rcode_rno} -ne 0 ]
            then
                echo -e "Failed to send file: ${data_file_name} to Apollo Reno!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[NRT ERROR] Error in sending data to Apollo Reno!!!" -v DL-eBay-Chocolate-GC@ebay.com
                exit ${rcode_rno}
            fi

        elif [ "${DEST_DC}" == "hercules" ]
        then
            ############################################ Send data to Hercules ############################################

            echo "=================== Start sending ${data_file_name} to ${dest_full_dir} ==================="

            ## Create date dir in hercules if it doesn't exist
            /datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -test -e ${dest_full_dir}
            if [ $? -ne 0 ]; then
                echo "Create hercules folder for ${date}"
                /datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -mkdir ${dest_full_dir}
            fi

            retry=1
            rcode_hercules=1
            until [[ ${retry} -gt 3 ]]
            do
                /datashare/mkttracking/tools/hercules_lvs/hadoop-hercules/bin/hadoop fs -put ${data_file_name} ${dest_full_dir}
                rcode_hercules=$?
                if [ ${rcode_hercules} -eq 0 ]
                then
                    echo "Successfully send to Hercules: "${data_file_name}
                    break
                else
                    echo "Faild to send to Reno, retrying ${retry}"
                    retry=`expr ${retry} + 1`
                fi
            done
            if [ ${rcode_hercules} -ne 0 ]
            then
                echo -e "Failed to send file: ${data_file_name} to Hercules!!!" | mailx -S smtp=mx.vip.lvs.ebay.com:25 -s "[NRT ERROR] Error in sending data to Hercules!!!" -v DL-eBay-Chocolate-GC@ebay.com
                exit ${rcode_hercules}
            fi

        else
            echo "Wrong dest dc!"
            exit 1
        fi

    done < "$output_file"
    rm -f ${output_file}
    echo "Finish send files size:"${files_size}
}

export HADOOP_USER_NAME=chocolate
/datashare/mkttracking/tools/keytab-tool/kinit/kinit_byhost.sh


meta_dir=${WORK_DIR}'/meta/'${CHANNEL}'/output/'${USAGE}
tmp_dir='tmp_scp_to_'${DEST_DC}'_'${CHANNEL}'_'${USAGE}
mkdir -p ${tmp_dir}
cd ${tmp_dir}

all_meta_files=all_meta_files.txt
hdfs dfs -ls ${meta_dir} | grep -v "^$" | awk '{print $NF}' | grep ${META_SUFFIX} > ${all_meta_files}

files_size=`cat ${all_meta_files} | wc -l`
echo "start process meta files size:"${files_size}

all_files=`cat ${all_meta_files} | tr "\n" " "`
for one_meta in ${all_files}
do
    process_one_meta ${one_meta} ${TOUCH_PROCESS_FILE}
    rcode=$?
    if [ ${rcode} -ne 0 ]
    then
        echo "Fail to process one meta file: ${one_meta}"
        exit ${rcode}
    fi
    hdfs dfs -rm ${one_meta}
done
cd ..
rm -r -f ${tmp_dir}