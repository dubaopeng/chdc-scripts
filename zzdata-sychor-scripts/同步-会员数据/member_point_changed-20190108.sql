SET mapred.job.name='b_member_point_change-会员积分变更信息';
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

-- 创建每日会员积分增量数据
CREATE EXTERNAL TABLE IF NOT EXISTS dw_source.`s_member_point_change`(
	`card_plan_id` string,
    `member_id` string,
    `change_value` bigint,
	`change_time` string,
	`action_type` string,
	`source` string,
	`id` string,
	`tenant` string
)
partitioned by(`dt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/hive/source_data/std_source/member/pointchange';

-- 批量重设分区
msck repair TABLE dw_source.`s_member_point_change`;

-- 创建会员积分信息基表
CREATE TABLE IF NOT EXISTS dw_business.`b_member_point_change`(
	`card_plan_id` string,
    `member_id` string,
    `change_value` bigint,
	`change_time` string,
	`action_type` string,
	`source` string,
	`id` string,
	`tenant` string
)
partitioned by(`part` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' lines terminated by '\n'
STORED AS ORC tblproperties ("orc.compress" = "SNAPPY");

-- 合并去重取最新数据
insert overwrite table dw_business.b_member_point_change partition(part='${stat_date}')
select t.card_plan_id,t.member_id,t.change_value,t.change_time,t.action_type,t.source,t.id,t.tenant
from (
	select card_plan_id,member_id,change_value,change_time,action_type,source,id,tenant,
		row_number() over (partition by id) as num 
    from dw_source.s_member_point_change where dt='${stat_date}'
) t
where t.num=1;





