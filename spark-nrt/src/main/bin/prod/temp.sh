
tmp_file=11.txt
hdfs dfs -ls /apps/tracking-events-workdir/meta/EPN/output/epnnrt_click/ | grep -v "^$" | awk '{print $NF}' | grep "meta" | grep -v "epnnrt_click_output" > ${tmp_file}


all_files=`cat ${tmp_file} | tr "\n" " "`

for one_file in ${all_files}
do
    file_name=$(basename "$one_file")
    new_file_name=epnnrt_click_output_${file_name}
    hdfs dfs -mv ${one_file} /apps/tracking-events-workdir/meta/EPN/output/epnnrt_click/${new_file_name}
done

rm -f ${tmp_file}



#tmp_file=22.txt
#hdfs dfs -ls /apps/tracking-events-workdir/meta/EPN/output/epnnrt_imp/ | grep -v "^$" | awk '{print $NF}' | grep "meta" | grep -v "epnnrt_imp_output" > ${tmp_file}
#
#
#all_files=`cat ${tmp_file} | tr "\n" " "`
#
#for one_file in ${all_files}
#do
#    file_name=$(basename "$one_file")
#    new_file_name=epnnrt_imp_output_${file_name}
#    hdfs dfs -mv ${one_file} /apps/tracking-events-workdir/meta/EPN/output/epnnrt_imp/${new_file_name}
#    echo "one"
#done
#
#rm -f ${tmp_file}