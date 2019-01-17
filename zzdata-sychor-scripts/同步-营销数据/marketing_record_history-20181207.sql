SET mapred.job.name='b_marketing_history-营销数据同步';
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

-- 创建每日营销记录增量表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_marketing_history`(
	`uni_id` string,
    `plat_code` string,
    `plat_account` string,
	`parnter` string,
	`uni_shop_id` string,
	`tenant` string,
	`shop_id` string,
    `marketing_time` string,
	`marketing_type` string, 
	`marketing_scene` string, 
	`activity_name` string, 
	`communication_mode` string,
	`communication_content` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/business_source/marketing';

-- 批量重设分区
msck repair table dw_source.`s_marketing_history`;

-- 创建营销数据记录表,以营销类型+日期进行分区
CREATE TABLE IF NOT EXISTS dw_business.`b_marketing_history`(
	`uni_id` string,
    `plat_code` string,
    `plat_account` string,
	`parnter` string,
	`uni_shop_id` string,
	`tenant` string,
	`shop_id` string,
    `marketing_time` string,
	`marketing_type` string, 
	`marketing_scene` string, 
	`activity_name` string, 
	`communication_mode` string,
	`communication_content` string
)
partitioned by(`type` string,`day` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");


-- 增量插入营销数据历史记录表
insert into dw_business.`b_marketing_history` partition(type,day)
select t.uni_id,t.plat_code,t.plat_account,t.parnter,t.uni_shop_id,t.tenant,t.shop_id,t.marketing_time,t.marketing_type,t.marketing_scene,
	t.activity_name,t.communication_mode,t.communication_content,t.marketing_type as type,t.dt as day
from(
	select *,row_number() over (partition by uni_id,plat_code,uni_shop_id,tenant,marketing_time,marketing_type ) as num 
	from dw_source.`s_marketing_history` where dt='${stat_date}'
) t 
where t.num=1
distribute by t.marketing_type;





