# ls
hadoop-2.7.4  hive
# cd ..
# ls
bin  boot  dev  employee  entrypoint.sh  etc  hadoop-data  home  lib  lib64  media  mnt  opt  proc  root  run  sbin  srv  sys  tmp  usr  var
# cd employee/
# ls
employee.csv  employee_table.hql
# hive -f employee_table.hql
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/hive/lib/log4j-slf4j-impl-2.6.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop-2.7.4/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]

Logging initialized using configuration in file:/opt/hive/conf/hive-log4j2.properties Async: true
OK
Time taken: 1.363 seconds
OK
Time taken: 0.037 seconds
OK
Time taken: 0.298 seconds
# hadoop fs -put employee.csv hdfs://namenode:8020/user/hive/warehouse/testdb.db/employee


# hive
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/hive/lib/log4j-slf4j-impl-2.6.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop-2.7.4/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]

Logging initialized using configuration in file:/opt/hive/conf/hive-log4j2.properties Async: true
Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
hive> show databases;
OK
default
testdb
Time taken: 0.794 seconds, Fetched: 2 row(s)
hive> use testdb;
OK
Time taken: 0.063 seconds
hive> select * from employee;
OK
1       Rudolf Bardin   30      cashier 100     New York        40000   5
2       Rob Trask       22      driver  100     New York        50000   4
3       Madie Nakamura  20      janitor 100     New York        30000   4
4       Alesha Huntley  40      cashier 101     Los Angeles     40000   10
5       Iva Moose       50      cashier 102     Phoenix 50000   20
Time taken: 1.468 seconds, Fetched: 5 row(s)

hive> CREATE TABLE experiments (
    >     id INT,
    >     experiment_name STRING,
    >     subject STRING,
    >     `date` STRING,
    >     result STRING
    > )
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY '\t'
    > STORED AS TEXTFILE;
OK
Time taken: 0.43 seconds

hive>
    > INSERT INTO experiments VALUES
    >     (1, 'Exp1', 'SubjectA', '2024-11-06', 'Success'),
    >     (2, 'Exp2', 'SubjectB', '2024-11-06', 'Failure'),
    >     (3, 'Exp3', 'SubjectC', '2024-11-06', 'Pending');
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20241106114643_6993adbf-6170-4dd3-80a2-85f74337dcfc
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Job running in-process (local Hadoop)
2024-11-06 11:46:46,607 Stage-1 map = 0%,  reduce = 0%
2024-11-06 11:46:47,644 Stage-1 map = 100%,  reduce = 0%
Ended Job = job_local1425173938_0001
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to directory hdfs://namenode:8020/user/hive/warehouse/testdb.db/experiments/.hive-staging_hive_2024-11-06_11-46-43_920_8972024703544826608-1/-ext-10000
Loading data to table testdb.experiments
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 342 HDFS Write: 285 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
Time taken: 5.024 seconds

hive> SELECT * FROM experiments;
OK
1       Exp1    SubjectA        2024-11-06      Success
2       Exp2    SubjectB        2024-11-06      Failure
3       Exp3    SubjectC        2024-11-06      Pending
Time taken: 0.422 seconds, Fetched: 3 row(s)
hive>

-------------
docker run -it --name hadoop -p 9870:9870 -p 8088:8088 bde2020/hadoop-base:latest /bin/bash
hdfs namenode -format
hdfs namenode & hdfs datanode & yarn resourcemanager & yarn nodemanager &
http://localhost:9870/dfshealth.html#tab-overview
http://localhost:8088/cluster
