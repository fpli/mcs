# CheckRecover-tool
you can add your job to the file check system to monitor the file count in real-time.

## Documentation
add your job to tasks.xml
* name: your job name;
* period: the schedule period (unit：minute), such as 5; if your period is one day or many day, the value is 0;
* timeDiff: the time that your job consume data (unit: hour); 
* inputURI: the ha of the inputDir, if your input file comes from local file system, the value is "";
* dataCountURI: the ha of the dataCountDir;
* inputDir: the directory of your input, if the input file comes from local file system, such as "/home/demo/test";
* dataCountDir: the directory of your dataCount;

## example
    <task name="cappingRule_EPN"
          period="5"
          timeDiff="0"
          inputURI="hdfs://elvisha"
          dataCountURI="hdfs://slickha"
          inputDir="hdfs://elvisha/apps/tracking-events/EPN/dedupe/date="
          dataCountDir="hdfs://slickha/apps/tracking-CheckRecover/countData/cappingRule_EPN">
    </task>
