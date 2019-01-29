SET mapred.job.name='export-catogery-table-notify';
--set hive.execution.engine=mr;
set hive.tez.container.size=6144;
set hive.cbo.enable=true;
SET hive.exec.compress.output=true;
SET mapred.max.split.size=512000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
SET mapred.output.compression.type=BLOCK;
SET mapreduce.map.output.compress=true;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
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

-- 设置任务提交时间
set submitTime=from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');

-- 将行业分析的三个表的记录插入通知表中
CREATE TABLE IF NOT EXISTS dw_rfm.`category_data_analysis_notify`(
	`table_name` string,
    `available` string,
    `modified` string,
	`stat_date` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.`category_data_analysis_notify`
select a.table_name,a.available,a.modified,a.stat_date
from(
	select 'cix_online_shop_has_permission_category' as table_name,1 as available,${hiveconf:submitTime} as modified,'${stat_date}' as stat_date
	union all
	select 'cix_online_category_customer_assets' as table_name,1 as available,${hiveconf:submitTime} as modified,'${stat_date}' as stat_date
	union all
	select 'cix_online_category_purchase_interval' as table_name,1 as available,${hiveconf:submitTime} as modified,'${stat_date}' as stat_date
) a;

insert overwrite table dw_rfm.category_data_analysis_notify 
    select a.table_name,a.available,a.modified,a.stat_date 
	from dw_rfm.category_data_analysis_notify a;