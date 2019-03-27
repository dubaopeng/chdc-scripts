SET mapred.job.name='b_interaction_history-互动数据同步';
set hive.tez.auto.reducer.parallelism=true;
set hive.tez.container.size=16384;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=4915;
set tez.runtime.unordered.output.buffer.size-mb=1640;
set tez.runtime.io.sort.mb=6553;
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

-- 创建每日互动记录增量表
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_interaction_history`(
	`uni_id` string,
    `plat_code` string,
    `plat_account` string,
	`parnter` string,
	`uni_shop_id` string,
	`shop_id` string,	
    `interaction_time` string,
	`interaction_type` string, 
	`interaction_name` string,
	`interaction_channel` string,
	`interaction_detail` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/business_source/interaction';

-- 批量重设分区
msck repair table dw_source.`s_interaction_history`;

-- 创建互动历史记录表,已互动日期进行分区
CREATE TABLE IF NOT EXISTS dw_business.`b_interaction_history`(
	`uni_id` string,
    `plat_code` string,
    `plat_account` string,
	`parnter` string,
	`uni_shop_id` string,
	`shop_id` string,	
    `interaction_time` string,
	`interaction_type` string, 
	`interaction_name` string, 
	`interaction_detail` string
)
partitioned by(`day` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");


-- 增量插入互动历史记录表
insert into dw_business.`b_interaction_history` partition(day)
select t.uni_id,t.plat_code,t.plat_account,t.parnter,t.uni_shop_id,t.shop_id,t.interaction_time,
	t.interaction_type,t.interaction_name,t.interaction_detail,t.dt as day
from(
    select *,row_number() over (partition by uni_id,plat_code,plat_account,shop_id,interaction_time,interaction_type) as num 
    from dw_source.`s_interaction_history` where dt='${stat_date}'
) t 
where t.num=1;





