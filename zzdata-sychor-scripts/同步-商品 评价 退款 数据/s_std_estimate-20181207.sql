SET mapred.job.name='s_std_estimate-评价数据同步';
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

-- 创建评价每天同步数据表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_std_estimate_increm`(
	`data_from` int,
	`partner` string,
    `plat_code` string,
	`order_id` string,
	`order_item_id` string,
    `product_id` string, 
    `sku_id` string,
	`estimate_content` string,
	`estimate_time` string,
    `estimate_result` string, 
	`estimate_replay` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/business_source/estimate';

-- 批量重设分区
msck repair table dw_source.`s_std_estimate_increm`;

-- 创建评价历史表
CREATE TABLE IF NOT EXISTS dw_business.`s_std_estimate`(
	`data_from` int,
	`partner` string,
    `plat_code` string,
	`order_id` string,
	`order_item_id` string,
    `product_id` string, 
    `sku_id` string,
	`estimate_content` string,
	`estimate_time` string,
    `estimate_result` string, 
	`estimate_replay` string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 合并并去重插入结果
insert overwrite table dw_business.`s_std_estimate`
select t.data_from,t.partner,t.plat_code,t.order_id,t.order_item_id,t.product_id,
	   t.sku_id,t.estimate_content,t.estimate_time,t.estimate_result,t.estimate_replay
 from (
	select re.*,row_number() over(distribute by re.plat_code,re.order_id,re.order_item_id,re.product_id,re.sku_id sort by re.estimate_time desc) as num
	from (
		select * from dw_business.`s_std_estimate` t1
		union all 
		select t2.data_from,t2.partner,t2.plat_code,t2.order_id,t2.order_item_id,t2.product_id,
			t2.sku_id,t2.estimate_content,t2.estimate_time,t2.estimate_result,t2.estimate_replay
			from dw_source.`s_std_estimate_increm` t2 where t2.dt='${stat_date}'
	) re
) t
where t.num=1;


