SET mapred.job.name='merge-analyze-result-for-export-rfm';
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

-- 创建活跃客户的RFM的RF指标表
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_active_customer_rf`(
	`tenant` string,
    `plat_code` string,
    `uni_shop_id` string,
	`recency` int,
	`frequency` int,
	`customer_num` bigint,
	`customer_rate` double,
	`avg_payment` double,
    `avg_guest_pay` double,
	`avg_guest_item` double, 
	`avg_item_pay` double,
	`type` int,
	`stat_date` string,
	`modified` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.`cix_online_active_customer_rf`
select tenant,plat_code,uni_shop_id,
	recency,
	frequency,
	customer_num,
	customer_rate,
	avg_payment,
	avg_guest_pay,
	avg_guest_item,
	avg_item_pay,
	type,
	stat_date,
	${hiveconf:submitTime} as modified
from dw_rfm.b_active_customer_rf_temp where part='${stat_date}';


-- 创建活跃客户的RFM的RF指标表
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_active_customer_rm`(
	`tenant` string,
    `plat_code` string,
    `uni_shop_id` string,
	`recency` int,
	`monetary` int,
	`customer_num` bigint,
	`customer_rate` double,
	`avg_payment` double,
    `avg_guest_pay` double,
	`avg_guest_item` double, 
	`avg_item_pay` double,
	`interval_type` int,
	`type` int,
	`stat_date` string,
	`modified` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS TEXTFILE;


insert overwrite table dw_rfm.`cix_online_active_customer_rm`
select tenant,plat_code,uni_shop_id,
	recency,
	monetary,
	customer_num,
	customer_rate,
	avg_payment,
	avg_guest_pay,
	avg_guest_item,
	avg_item_pay,
	interval_type,
	type,
	stat_date,
	${hiveconf:submitTime} as modified
from dw_rfm.b_active_customer_rm_temp where part='${stat_date}';


-- 删除RFM分析结果临时表
drop table if exists dw_rfm.b_active_customer_rm_temp;
drop table if exists dw_rfm.b_active_customer_rf_temp;


-- 将两个表的记录插入通知表
CREATE TABLE IF NOT EXISTS dw_rfm.`cix_online_customer_analysis_notify`(
	`table_name` string,
    `available` string,
    `modified` string,
	`stat_date` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS TEXTFILE;

insert overwrite table dw_rfm.`cix_online_customer_analysis_notify`
select a.table_name,a.available,a.modified,a.stat_date
from(
	select 'cix_online_active_customer_rm' as table_name,1 as available,${hiveconf:submitTime} as modified,'${stat_date}' as stat_date
	union all
	select 'cix_online_active_customer_rf' as table_name,1 as available,${hiveconf:submitTime} as modified,'${stat_date}' as stat_date
	union all
	select 'cix_online_customer_purchase_interval' as table_name,1 as available,${hiveconf:submitTime} as modified,'${stat_date}' as stat_date
) a;

insert overwrite table dw_rfm.cix_online_customer_analysis_notify
	select a.table_name,a.available,a.modified,a.stat_date 
	from dw_rfm.cix_online_customer_analysis_notify a;

-- TODO 接下来使用sqoop导出上面几个表的数据到mysql表中





