SET mapred.job.name='dashboard_table_mapping';
SET hive.exec.compress.output=true;
set hive.cbo.enable=true;
SET mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapred.output.compression.type=BLOCK;
SET mapreduce.map.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.exec.compress.output.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.dynamic.partition=true;
SET mapreduce.reduce.shuffle.input.buffer.percent =0.6;
SET hive.exec.max.created.files=655350;
SET hive.exec.max.dynamic.partitions=10000000;
SET hive.exec.max.dynamic.partitions.pernode=10000000;
set hive.stats.autogather=false;
set hive.merge.mapfiles = true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task = 512000000;
set hive.support.concurrency=false;

set MODIFIED = substr(current_timestamp,1,19);

--drop table if exists new_dashboard.dashboard_table_mapping;
create table if not exists new_dashboard.dashboard_table_mapping(
	table_name      string,
	available       string,
	modified        string,
        stat_date      string
)partitioned by(part string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS textfile;

insert into table new_dashboard.dashboard_table_mapping partition(part ='${stat_date}')
select
    table_name,
    '1' as available,
    ${hiveconf:MODIFIED} as modified,
     '${stat_date}' as stat_date
from new_dashboard.dashboard_table
where status = '1';


